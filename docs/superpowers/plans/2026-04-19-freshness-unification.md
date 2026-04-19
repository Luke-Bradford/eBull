# Freshness unification — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the two disagreeing freshness systems (`ops_monitor` + `sync_orchestrator.freshness`) with a single `LayerState` source of truth that is cadence-aware, cascade-aware, and self-healing, so the operator sees only what they must act on.

**Architecture:** Introduce typed `LayerState` (8 states) + `FailureCategory` taxonomy. Extend the existing `DataLayer` registry with `Cadence`, `grace_multiplier`, `RetryPolicy`, `secret_refs`, `content_predicate`, and `plain_language_sla`. Add a pure `compute_layer_state(ctx)` function + DB-facing `compute_layer_states_from_db(conn)`. Expose results at a new `GET /sync/layers/v2` and extend `POST /sync` with a `scope: "behind"|"full"|"layer:<name>"` body field. Retire `ops_monitor`'s watermark checks by moving spike detection into per-layer `content_predicate` hooks. Seven chunks, each one PR, each independently revertible.

**Tech Stack:** Python 3.11, FastAPI, psycopg3, pydantic v2, APScheduler, Vite+React+TypeScript, Tailwind, pytest, vitest.

**Spec:** [`docs/superpowers/specs/2026-04-19-freshness-unification-design.md`](../specs/2026-04-19-freshness-unification-design.md)

**Issue:** [#328](https://github.com/Luke-Bradford/eBull/issues/328)

**Branch convention:** the spec commit already lives on `feature/328-freshness-unification-spec`. Each chunk merges from its own branch: `feature/328-chunk-<n>-<slug>`.

---

## Reference — current code contracts (verified 2026-04-19)

- **`DataLayer` dataclass** in `app/services/sync_orchestrator/registry.py`:
  `name, display_name, tier, is_fresh: Callable[[conn], tuple[bool, str]], refresh: LayerRefresh, dependencies: tuple[str, ...], is_blocking: bool = True, cadence: str = "daily"`.
- **`LayerRefresh` protocol** in `app/services/sync_orchestrator/types.py:72`:
  `__call__(*, sync_run_id: int, progress: ProgressCallback, upstream_outcomes: Mapping[str, LayerOutcome]) -> Sequence[tuple[str, RefreshResult]]`. Keyword-only, returns a list of `(layer_name, RefreshResult)` pairs.
- **`RefreshResult`** has `outcome: LayerOutcome, row_count: int | None, error_msg: str | None, error_category: str | None = None` (`types.py:55-65`).
- **`record_job_finish(conn, run_id, *, status, row_count, error_msg, now)`** in `app/services/ops_monitor.py:291` — there is **no** `record_job_failure`. Status is `Literal["success", "failure"]`.
- **`record_job_skip(conn, job_name, reason, *, now)`** at `ops_monitor.py:322` — writes a single row with `status='skipped'`, `error_msg=reason`.
- **`POST /sync`** in `app/api/sync.py:93` — Pydantic `SyncRequest` body (not query params). Calls `submit_sync(_scope_from(body), trigger="manual")` and returns `{"sync_run_id": ..., "plan": ...}`.
- **`GET /sync/layers`** at `app/api/sync.py:166` — v1 payload. Each layer row emits: `name, display_name, tier, is_fresh, freshness_detail, last_success_at, last_duration_seconds, last_error_category, consecutive_failures, dependencies, is_blocking`.
- **`/health`** lives in `app/main.py:179` (not in any router).
- **`job_runs.status`** check: `('running','success','failure','skipped')` per `sql/020_job_runs_skipped_status.sql`. No `error_category` column yet (chunk 3 adds it).
- **`sync_layer_progress.status`** check: `('pending','running','complete','failed','skipped','partial')` per `sql/033_sync_orchestrator.sql:42`. **Different string set from `job_runs`**. Plan code that reads `sync_layer_progress` compares against `"failed"`; code that writes `job_runs` uses `"failure"`.
- **Test DB**: `from tests.fixtures.ebull_test_db import test_database_url, ebull_test_conn`. There is no `settings.test_database_url`.
- **Scheduler wrapping**: `app/workers/scheduler.py::_tracked_job` already calls `record_job_start` / `record_job_finish`. Extend it, do not wrap it again.
- **Existing failure-history helper**: `app/services/sync_orchestrator/layer_failure_history.py::all_layer_histories(conn, names)` returns `(streaks, categories)` from `sync_layer_progress` using the same ordering the new state machine needs.

---

## Chunk 1 — Types module

Pure additions. No behaviour change. Branch: `feature/328-chunk-1-types`.

### Task 1.1: LayerState enum

**Files:**
- Create: `app/services/sync_orchestrator/layer_types.py`
- Test: `tests/services/sync_orchestrator/test_layer_types.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/services/sync_orchestrator/test_layer_types.py
from app.services.sync_orchestrator.layer_types import LayerState


def test_layer_state_has_eight_members() -> None:
    assert {s.value for s in LayerState} == {
        "healthy",
        "running",
        "retrying",
        "degraded",
        "action_needed",
        "secret_missing",
        "cascade_waiting",
        "disabled",
    }


def test_layer_state_is_str_enum() -> None:
    assert LayerState.HEALTHY == "healthy"
    assert LayerState("healthy") is LayerState.HEALTHY
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/services/sync_orchestrator/test_layer_types.py -v
```
Expected: `ModuleNotFoundError: app.services.sync_orchestrator.layer_types`.

- [ ] **Step 3: Write minimal implementation**

```python
# app/services/sync_orchestrator/layer_types.py
"""Typed vocabulary for the freshness state machine (spec sub-project A).

Consumed by the registry (chunk 2), adapters (chunk 3), state
computation (chunk 4), and the v2 API (chunk 5). Must not import from
any orchestrator runtime module — this sits at the bottom of the
orchestrator import graph.
"""

from __future__ import annotations

from enum import StrEnum


class LayerState(StrEnum):
    HEALTHY = "healthy"
    RUNNING = "running"
    RETRYING = "retrying"
    DEGRADED = "degraded"
    ACTION_NEEDED = "action_needed"
    SECRET_MISSING = "secret_missing"
    CASCADE_WAITING = "cascade_waiting"
    DISABLED = "disabled"
```

- [ ] **Step 4: Run test to verify it passes**

```
uv run pytest tests/services/sync_orchestrator/test_layer_types.py -v
```
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add app/services/sync_orchestrator/layer_types.py tests/services/sync_orchestrator/test_layer_types.py
git commit -m "feat(#328): introduce LayerState enum"
```

### Task 1.2: FailureCategory + REMEDIES

**Files:**
- Modify: `app/services/sync_orchestrator/layer_types.py`
- Modify: `tests/services/sync_orchestrator/test_layer_types.py`

- [ ] **Step 1: Append failing test**

```python
from app.services.sync_orchestrator.layer_types import (
    FailureCategory,
    REMEDIES,
    Remedy,
)


def test_failure_category_members() -> None:
    assert {c.value for c in FailureCategory} == {
        "auth_expired",
        "rate_limited",
        "source_down",
        "schema_drift",
        "db_constraint",
        "data_gap",
        "upstream_waiting",
        "internal_error",
    }


def test_every_category_has_a_remedy() -> None:
    for category in FailureCategory:
        assert category in REMEDIES
        remedy = REMEDIES[category]
        assert isinstance(remedy, Remedy)
        assert remedy.message
        if not remedy.self_heal:
            assert remedy.operator_fix is not None


def test_non_self_heal_categories_match_spec() -> None:
    non_self_heal = {
        FailureCategory.AUTH_EXPIRED,
        FailureCategory.SCHEMA_DRIFT,
        FailureCategory.DB_CONSTRAINT,
    }
    for category in FailureCategory:
        assert REMEDIES[category].self_heal == (category not in non_self_heal)
```

- [ ] **Step 2: Run to confirm fail** — `ImportError: cannot import name 'FailureCategory'`.

- [ ] **Step 3: Append to layer_types.py**

```python
from dataclasses import dataclass


class FailureCategory(StrEnum):
    AUTH_EXPIRED = "auth_expired"
    RATE_LIMITED = "rate_limited"
    SOURCE_DOWN = "source_down"
    SCHEMA_DRIFT = "schema_drift"
    DB_CONSTRAINT = "db_constraint"
    DATA_GAP = "data_gap"
    UPSTREAM_WAITING = "upstream_waiting"
    INTERNAL_ERROR = "internal_error"


@dataclass(frozen=True)
class Remedy:
    message: str
    operator_fix: str | None
    self_heal: bool


REMEDIES: dict[FailureCategory, Remedy] = {
    FailureCategory.AUTH_EXPIRED: Remedy(
        message="Credential rejected by provider",
        operator_fix="Update the API key in Settings → Providers",
        self_heal=False,
    ),
    FailureCategory.RATE_LIMITED: Remedy(
        message="Rate limit hit — retrying with backoff",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.SOURCE_DOWN: Remedy(
        message="Data source unreachable — retrying with backoff",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.SCHEMA_DRIFT: Remedy(
        message="Provider payload shape changed — needs code update",
        operator_fix="File a bug; the adapter needs a parser update",
        self_heal=False,
    ),
    FailureCategory.DB_CONSTRAINT: Remedy(
        message="Database constraint violated — likely data-model bug",
        operator_fix="Open orchestrator details and inspect the offending row",
        self_heal=False,
    ),
    FailureCategory.DATA_GAP: Remedy(
        message="Source returned no data — will retry next cycle",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.UPSTREAM_WAITING: Remedy(
        message="Waiting on upstream layer",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.INTERNAL_ERROR: Remedy(
        message="Unclassified error — retrying with backoff",
        operator_fix=None,
        self_heal=True,
    ),
}
```

- [ ] **Step 4: Run tests — 5 passed.**

- [ ] **Step 5: Commit.**

```bash
git add app/services/sync_orchestrator/layer_types.py tests/services/sync_orchestrator/test_layer_types.py
git commit -m "feat(#328): add FailureCategory + REMEDIES table"
```

### Task 1.3: Cadence + RetryPolicy + SecretRef + LayerRefreshFailed + ContentPredicate

**Files:** same two.

- [ ] **Step 1: Append failing tests**

```python
from datetime import timedelta
from typing import Callable, Protocol

import psycopg
import pytest

from app.services.sync_orchestrator.layer_types import (
    Cadence,
    ContentPredicate,
    DEFAULT_RETRY_POLICY,
    LayerRefreshFailed,
    RetryPolicy,
    SecretRef,
)


def test_cadence_grace_window_uses_multiplier() -> None:
    c = Cadence(interval=timedelta(hours=24))
    assert c.grace_window(grace_multiplier=1.25) == timedelta(hours=30)


def test_cadence_rejects_non_positive_interval() -> None:
    with pytest.raises(ValueError, match="interval must be positive"):
        Cadence(interval=timedelta(0))


def test_default_retry_policy() -> None:
    assert DEFAULT_RETRY_POLICY.max_attempts == 3
    assert DEFAULT_RETRY_POLICY.backoff_seconds == (60, 600, 3600)


def test_retry_policy_backoff_matches_max_attempts() -> None:
    with pytest.raises(ValueError, match="backoff_seconds"):
        RetryPolicy(max_attempts=3, backoff_seconds=(60, 600))


def test_secret_ref_fields() -> None:
    ref = SecretRef(env_var="ANTHROPIC_API_KEY", display_name="Anthropic API key")
    assert ref.env_var == "ANTHROPIC_API_KEY"
    assert ref.display_name == "Anthropic API key"


def test_layer_refresh_failed_carries_category() -> None:
    from app.services.sync_orchestrator.layer_types import FailureCategory
    err = LayerRefreshFailed(category=FailureCategory.SOURCE_DOWN, detail="finnhub 503")
    assert err.category is FailureCategory.SOURCE_DOWN
    assert err.detail == "finnhub 503"
    assert "source_down" in str(err)
    assert "finnhub" in str(err)


def test_content_predicate_is_a_callable_protocol() -> None:
    # A plain function matches the protocol — structural typing.
    def my_pred(conn: psycopg.Connection) -> tuple[bool, str]:
        return True, "ok"
    _: ContentPredicate = my_pred
    assert my_pred is not None  # silence unused-var
```

- [ ] **Step 2: Run to confirm fail.**

- [ ] **Step 3: Append to layer_types.py**

```python
from datetime import timedelta
from typing import Any, Protocol

import psycopg


@dataclass(frozen=True)
class Cadence:
    interval: timedelta

    def __post_init__(self) -> None:
        if self.interval <= timedelta(0):
            raise ValueError("interval must be positive")

    def grace_window(self, grace_multiplier: float) -> timedelta:
        return self.interval * grace_multiplier


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: tuple[int, ...] = (60, 600, 3600)

    def __post_init__(self) -> None:
        if len(self.backoff_seconds) != self.max_attempts:
            raise ValueError(
                "backoff_seconds must have exactly max_attempts entries "
                f"(got {len(self.backoff_seconds)} for max_attempts={self.max_attempts})"
            )


DEFAULT_RETRY_POLICY = RetryPolicy()


@dataclass(frozen=True)
class SecretRef:
    env_var: str
    display_name: str


class ContentPredicate(Protocol):
    """Structural signature for a per-layer content check.

    Returns (ok, detail). `ok=True` means the layer's data is
    considered content-current (e.g. every Tier 1 ticker has a candle
    for today). `detail` is an operator-visible sentence surfaced when
    the predicate fails. Pure SELECT; must not write.
    """

    def __call__(self, conn: psycopg.Connection[Any]) -> tuple[bool, str]: ...


class LayerRefreshFailed(Exception):
    """Adapter-level failure carrying a categorisation.

    Adapters raise this so the executor can persist the category
    alongside the error message.
    """

    def __init__(self, category: "FailureCategory", detail: str) -> None:
        super().__init__(f"{category.value}: {detail}")
        self.category = category
        self.detail = detail
```

- [ ] **Step 4: Run tests — 11 passed (2 + 3 + 7).**

- [ ] **Step 5: Commit**

```bash
git add app/services/sync_orchestrator/layer_types.py tests/services/sync_orchestrator/test_layer_types.py
git commit -m "feat(#328): add Cadence, RetryPolicy, SecretRef, ContentPredicate, LayerRefreshFailed"
```

### Task 1.4: Pre-push gate, push chunk 1, merge

- [ ] **Run** `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest`.
- [ ] **Push**, **PR** `feat(#328): chunk 1 — freshness types module`, poll review + CI, resolve, merge. Do not start chunk 2 until merged.

---

## Chunk 2 — Registry extension with content_predicate

Adds new fields to `DataLayer` including `content_predicate`. Splits today's `is_fresh` predicates into a pure audit check (handled by the new state machine in chunk 4) and an optional content predicate (per-instrument coverage). Branch: `feature/328-chunk-2-registry`.

### Task 2.1: Content predicates — split out of `freshness.py`

**Files:**
- Create: `app/services/sync_orchestrator/content_predicates.py`
- Test: `tests/services/sync_orchestrator/test_content_predicates.py`

- [ ] **Step 1: Failing test**

```python
# tests/services/sync_orchestrator/test_content_predicates.py
from datetime import date
from unittest.mock import MagicMock

from app.services.sync_orchestrator.content_predicates import (
    candles_content_ok,
    fundamentals_content_ok,
)


def test_candles_content_ok_returns_tuple() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (0,)  # no missing rows
    ok, detail = candles_content_ok(conn)
    assert ok is True


def test_candles_content_missing_reports_count() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (17,)
    ok, detail = candles_content_ok(conn)
    assert ok is False
    assert "17" in detail


def test_fundamentals_content_ok_no_missing() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (0,)
    ok, _ = fundamentals_content_ok(conn)
    assert ok is True
```

- [ ] **Step 2: Run to confirm fail.**

- [ ] **Step 3: Write content_predicates.py by lifting the body of the content checks currently embedded in `freshness.py::candles_is_fresh` and `freshness.py::fundamentals_is_fresh`.**

```python
# app/services/sync_orchestrator/content_predicates.py
"""Per-layer content predicates (spec §4).

These live independently of the audit-age check so the state machine
can distinguish "audit is fresh but data is missing rows" (DEGRADED
via content) from "audit is stale" (DEGRADED via age). The legacy
`is_fresh` predicates in `freshness.py` combined both signals; once
chunk 7 retires that module these are the surviving content checks.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import psycopg


def candles_content_ok(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    """Every Tier 1/2 instrument must have a candle for the most recent
    trading day."""
    from app.services.market_data import _most_recent_trading_day

    trading_day = _most_recent_trading_day(date.today())
    row = conn.execute(
        """
        SELECT COUNT(*) AS missing
        FROM instruments i
        JOIN coverage c USING (instrument_id)
        WHERE c.coverage_tier IN (1, 2)
          AND COALESCE(
              (SELECT MAX(price_date) FROM price_daily p
               WHERE p.instrument_id = i.instrument_id),
              DATE '1900-01-01'
          ) < %s
        """,
        (trading_day,),
    ).fetchone()
    missing = row[0] if row else 0
    if missing > 0:
        return (
            False,
            f"{missing} T1/T2 instruments missing candle for {trading_day.isoformat()}",
        )
    return True, "all T1/T2 instruments current"


def fundamentals_content_ok(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    """Every tradable instrument must have a fundamentals snapshot row in
    the current quarter."""
    today = date.today()
    quarter = (today.month - 1) // 3
    quarter_start = date(today.year, quarter * 3 + 1, 1)
    row = conn.execute(
        """
        SELECT COUNT(*) AS missing
        FROM instruments i
        WHERE i.is_tradable = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM fundamentals_snapshot fs
              WHERE fs.instrument_id = i.instrument_id
                AND fs.as_of_date >= %s
          )
        """,
        (quarter_start,),
    ).fetchone()
    missing = row[0] if row else 0
    if missing > 0:
        return (
            False,
            f"{missing} tradable instruments lack fundamentals snapshot "
            f"for quarter starting {quarter_start.isoformat()}",
        )
    return True, "all tradable instruments have snapshot"
```

- [ ] **Step 4: Tests green.**

- [ ] **Step 5: Commit**

```bash
git add app/services/sync_orchestrator/content_predicates.py tests/services/sync_orchestrator/test_content_predicates.py
git commit -m "feat(#328): extract candles/fundamentals content predicates"
```

### Task 2.2: Extend `DataLayer` + update every literal

**Files:**
- Modify: `app/services/sync_orchestrator/registry.py`
- Test: `tests/services/sync_orchestrator/test_registry_shape.py`

- [ ] **Step 1: Failing test**

```python
# tests/services/sync_orchestrator/test_registry_shape.py
from datetime import timedelta

from app.services.sync_orchestrator.layer_types import (
    Cadence,
    DEFAULT_RETRY_POLICY,
    RetryPolicy,
)
from app.services.sync_orchestrator.registry import LAYERS


EXPECTED_CADENCES: dict[str, timedelta] = {
    "universe": timedelta(days=7),
    "cik_mapping": timedelta(hours=24),
    "candles": timedelta(hours=24),
    "financial_facts": timedelta(hours=24),
    "financial_normalization": timedelta(hours=24),
    "fundamentals": timedelta(days=90),
    "news": timedelta(hours=4),
    "thesis": timedelta(hours=24),
    "scoring": timedelta(hours=24),
    "recommendations": timedelta(hours=24),
    "portfolio_sync": timedelta(minutes=5),
    "fx_rates": timedelta(minutes=5),
    "cost_models": timedelta(hours=24),
    "weekly_reports": timedelta(days=7),
    "monthly_reports": timedelta(days=31),
}


def test_every_layer_has_typed_cadence() -> None:
    for name, layer in LAYERS.items():
        assert isinstance(layer.cadence, Cadence)


def test_cadence_intervals_match_expected() -> None:
    for name, expected in EXPECTED_CADENCES.items():
        assert LAYERS[name].cadence.interval == expected


def test_minute_cadence_layers_have_tighter_retry_policy() -> None:
    for name in ("fx_rates", "portfolio_sync"):
        policy = LAYERS[name].retry_policy
        assert policy.max_attempts == 5
        assert policy.backoff_seconds == (30, 60, 120, 300, 600)


def test_daily_layers_use_default_retry_policy() -> None:
    for name in ("cik_mapping", "candles", "financial_facts"):
        assert LAYERS[name].retry_policy == DEFAULT_RETRY_POLICY


def test_every_layer_has_non_empty_plain_language_sla() -> None:
    for name, layer in LAYERS.items():
        assert layer.plain_language_sla


def test_grace_multiplier_default() -> None:
    for name, layer in LAYERS.items():
        assert layer.grace_multiplier == 1.25


def test_llm_layers_declare_anthropic_secret() -> None:
    news = {s.env_var for s in LAYERS["news"].secret_refs}
    thesis = {s.env_var for s in LAYERS["thesis"].secret_refs}
    assert "ANTHROPIC_API_KEY" in news
    assert "ANTHROPIC_API_KEY" in thesis


def test_market_data_layers_declare_no_env_secrets() -> None:
    assert LAYERS["candles"].secret_refs == ()
    assert LAYERS["cik_mapping"].secret_refs == ()


def test_candles_has_content_predicate() -> None:
    from app.services.sync_orchestrator.content_predicates import candles_content_ok
    assert LAYERS["candles"].content_predicate is candles_content_ok


def test_fundamentals_has_content_predicate() -> None:
    from app.services.sync_orchestrator.content_predicates import fundamentals_content_ok
    assert LAYERS["fundamentals"].content_predicate is fundamentals_content_ok


def test_other_layers_have_no_content_predicate() -> None:
    # Only candles and fundamentals carry content predicates today.
    for name in ("universe", "news", "scoring", "portfolio_sync"):
        assert LAYERS[name].content_predicate is None
```

- [ ] **Step 2: Run — fails with `AttributeError` or `TypeError` on the old dataclass.**

- [ ] **Step 3: Rewrite the `DataLayer` dataclass**

In `app/services/sync_orchestrator/registry.py`, replace:

```python
@dataclass(frozen=True)
class DataLayer:
    name: str
    display_name: str
    tier: int
    cadence: Cadence
    is_fresh: Callable[[psycopg.Connection[Any]], tuple[bool, str]]
    refresh: LayerRefresh
    dependencies: tuple[str, ...] = ()
    is_blocking: bool = True
    grace_multiplier: float = 1.25
    retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY
    secret_refs: tuple[SecretRef, ...] = ()
    content_predicate: ContentPredicate | None = None
    plain_language_sla: str = ""
```

Add imports at the top:

```python
from datetime import timedelta

from app.services.sync_orchestrator.content_predicates import (
    candles_content_ok,
    fundamentals_content_ok,
)
from app.services.sync_orchestrator.layer_types import (
    Cadence,
    ContentPredicate,
    DEFAULT_RETRY_POLICY,
    RetryPolicy,
    SecretRef,
)
```

- [ ] **Step 4: Rewrite every layer literal in `LAYERS` to use the new fields**

Pattern for every entry — example:

```python
"universe": DataLayer(
    name="universe",
    display_name="Tradable Universe",
    tier=0,
    cadence=Cadence(interval=timedelta(days=7)),
    is_fresh=universe_is_fresh,
    refresh=refresh_universe,
    dependencies=(),
    plain_language_sla="Refreshed weekly — eToro instrument list.",
),
"cik_mapping": DataLayer(
    name="cik_mapping",
    display_name="SEC CIK Mapping",
    tier=0,
    cadence=Cadence(interval=timedelta(hours=24)),
    is_fresh=cik_mapping_is_fresh,
    refresh=refresh_cik_mapping,
    dependencies=("universe",),
    plain_language_sla="Refreshed nightly from SEC company_tickers.json.",
),
"candles": DataLayer(
    name="candles",
    display_name="Daily Price Candles",
    tier=1,
    cadence=Cadence(interval=timedelta(hours=24)),
    is_fresh=candles_is_fresh,
    refresh=refresh_candles,
    dependencies=("universe",),
    content_predicate=candles_content_ok,
    plain_language_sla="Refreshed every trading day after market close.",
),
"financial_facts": DataLayer(
    name="financial_facts",
    display_name="SEC EDGAR XBRL Facts",
    tier=1,
    cadence=Cadence(interval=timedelta(hours=24)),
    is_fresh=financial_facts_is_fresh,
    refresh=refresh_financial_facts_and_normalization,
    dependencies=("cik_mapping",),
    plain_language_sla="Refreshed nightly from SEC XBRL filings.",
),
"financial_normalization": DataLayer(
    name="financial_normalization",
    display_name="Financial Period Normalization",
    tier=2,
    cadence=Cadence(interval=timedelta(hours=24)),
    is_fresh=financial_normalization_is_fresh,
    refresh=refresh_financial_facts_and_normalization,
    dependencies=("financial_facts",),
    plain_language_sla="Derived nightly from SEC XBRL facts.",
),
"fundamentals": DataLayer(
    name="fundamentals",
    display_name="Fundamentals Snapshot",
    tier=1,
    cadence=Cadence(interval=timedelta(days=90)),
    is_fresh=fundamentals_is_fresh,
    refresh=refresh_fundamentals,
    dependencies=("universe",),
    content_predicate=fundamentals_content_ok,
    plain_language_sla="Refreshed quarterly alongside earnings.",
),
"news": DataLayer(
    name="news",
    display_name="News & Sentiment",
    tier=1,
    cadence=Cadence(interval=timedelta(hours=4)),
    is_fresh=news_is_fresh,
    refresh=refresh_news,
    dependencies=("universe",),
    is_blocking=False,
    secret_refs=(SecretRef(env_var="ANTHROPIC_API_KEY", display_name="Anthropic API key"),),
    plain_language_sla="Refreshed every 4h — news + sentiment scoring.",
),
"thesis": DataLayer(
    name="thesis",
    display_name="Investment Thesis",
    tier=2,
    cadence=Cadence(interval=timedelta(hours=24)),
    is_fresh=thesis_is_fresh,
    refresh=refresh_thesis,
    dependencies=("fundamentals", "financial_normalization", "news"),
    secret_refs=(SecretRef(env_var="ANTHROPIC_API_KEY", display_name="Anthropic API key"),),
    plain_language_sla="Refreshed nightly for stale Tier 1 tickers.",
),
"scoring": DataLayer(
    name="scoring",
    display_name="Ranking Scores",
    tier=3,
    cadence=Cadence(interval=timedelta(hours=24)),
    is_fresh=scoring_is_fresh,
    refresh=refresh_scoring_and_recommendations,
    dependencies=("thesis", "candles"),
    plain_language_sla="Refreshed every morning pre-market.",
),
"recommendations": DataLayer(
    name="recommendations",
    display_name="Trade Recommendations",
    tier=3,
    cadence=Cadence(interval=timedelta(hours=24)),
    is_fresh=recommendations_is_fresh,
    refresh=refresh_scoring_and_recommendations,
    dependencies=("scoring",),
    plain_language_sla="Refreshed every morning after scoring.",
),
"portfolio_sync": DataLayer(
    name="portfolio_sync",
    display_name="Portfolio Sync",
    tier=0,
    cadence=Cadence(interval=timedelta(minutes=5)),
    is_fresh=portfolio_sync_is_fresh,
    refresh=refresh_portfolio_sync,
    dependencies=(),
    is_blocking=False,
    retry_policy=RetryPolicy(max_attempts=5, backoff_seconds=(30, 60, 120, 300, 600)),
    plain_language_sla="Synced every 5 minutes against eToro.",
),
"fx_rates": DataLayer(
    name="fx_rates",
    display_name="FX Rates",
    tier=0,
    cadence=Cadence(interval=timedelta(minutes=5)),
    is_fresh=fx_rates_is_fresh,
    refresh=refresh_fx_rates,
    dependencies=(),
    is_blocking=False,
    retry_policy=RetryPolicy(max_attempts=5, backoff_seconds=(30, 60, 120, 300, 600)),
    plain_language_sla="Refreshed every 5 minutes for live valuation.",
),
"cost_models": DataLayer(
    name="cost_models",
    display_name="Transaction Cost Models",
    tier=2,
    cadence=Cadence(interval=timedelta(hours=24)),
    is_fresh=cost_models_is_fresh,
    refresh=refresh_cost_models,
    dependencies=("universe",),
    plain_language_sla="Re-seeded nightly.",
),
"weekly_reports": DataLayer(
    name="weekly_reports",
    display_name="Weekly Performance Report",
    tier=3,
    cadence=Cadence(interval=timedelta(days=7)),
    is_fresh=weekly_reports_is_fresh,
    refresh=refresh_weekly_reports,
    dependencies=(),
    is_blocking=False,
    plain_language_sla="Published every Monday morning.",
),
"monthly_reports": DataLayer(
    name="monthly_reports",
    display_name="Monthly Performance Report",
    tier=3,
    cadence=Cadence(interval=timedelta(days=31)),
    is_fresh=monthly_reports_is_fresh,
    refresh=refresh_monthly_reports,
    dependencies=(),
    is_blocking=False,
    plain_language_sla="Published on the 1st of every month.",
),
```

- [ ] **Step 5: Add a string-cadence shim for any surviving caller**

Search for string `cadence`:

```
grep -rn "layer.cadence\b\|\.cadence\b" app tests frontend/src --include="*.py" --include="*.ts" --include="*.tsx"
```

For each caller that used the old `str`, either (a) read `layer.cadence.interval` directly, or (b) import the new helper `cadence_display_string` added in layer_types.py:

```python
# Append to app/services/sync_orchestrator/layer_types.py
def cadence_display_string(cadence: Cadence) -> str:
    total = int(cadence.interval.total_seconds())
    if total % 86400 == 0:
        d = total // 86400
        return f"{d}d" if d > 1 else "daily"
    if total % 3600 == 0:
        return f"{total // 3600}h"
    if total % 60 == 0:
        return f"{total // 60}m"
    return f"{total}s"
```

The v1 `/sync/layers` response does **not** expose `cadence`, so there is no JSON-field change here. Internal callers (dashboards, tests) may display the new typed value however they choose.

- [ ] **Step 6: Run tests — green.**

```
uv run pytest tests/services/sync_orchestrator/test_registry_shape.py -v
uv run pytest
```

- [ ] **Step 7: Commit**

```bash
git add app/services/sync_orchestrator/registry.py app/services/sync_orchestrator/layer_types.py tests/services/sync_orchestrator/test_registry_shape.py
git commit -m "feat(#328): typed Cadence + retry_policy + secret_refs + content_predicate on DataLayer"
```

### Task 2.3: Pre-push + push chunk 2

- [ ] Full gate. Push. PR `feat(#328): chunk 2 — extend DataLayer registry`. Poll. Resolve. Merge.

---

## Chunk 3 — Error categorisation in adapters + scheduler + `job_runs.error_category`

Adds the `error_category` column to `job_runs`, teaches adapters to raise `LayerRefreshFailed`, and extends `_tracked_job` so scheduler failures persist a category. Branch: `feature/328-chunk-3-error-category`.

### Task 3.1: Migration — add `error_category` to `job_runs`

**Files:**
- Create: `sql/039_job_runs_error_category.sql`

- [ ] **Step 1: Write the migration**

Verify next available migration number: `ls sql/ | tail -5` (repo ends at 038 as of 2026-04-19). Use 039.

```sql
-- 039_job_runs_error_category.sql
-- Adds error_category column to job_runs so the legacy job runner
-- can persist the same taxonomy as sync_layer_progress.

ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS error_category TEXT;

CREATE INDEX IF NOT EXISTS idx_job_runs_error_category
    ON job_runs(error_category)
    WHERE error_category IS NOT NULL;
```

- [ ] **Step 2: Apply locally**

```
uv run python -c "from app.db.migrations import run_migrations; run_migrations()"
```

Apply idempotently — rerun to confirm.

- [ ] **Step 3: Commit**

```bash
git add sql/034_job_runs_error_category.sql
git commit -m "feat(#328): add error_category column to job_runs"
```

### Task 3.2: `record_job_finish` accepts `error_category`

**Files:**
- Modify: `app/services/ops_monitor.py`
- Test: `tests/services/test_ops_monitor_error_category.py`

- [ ] **Step 1: Write failing test**

```python
# tests/services/test_ops_monitor_error_category.py
import psycopg
import pytest

from app.services.ops_monitor import record_job_finish, record_job_start
from app.services.sync_orchestrator.layer_types import FailureCategory
from tests.fixtures.ebull_test_db import test_database_url


@pytest.mark.integration
def test_record_job_finish_persists_error_category() -> None:
    with psycopg.connect(test_database_url()) as conn:
        run_id = record_job_start(conn, "test_job_cat")
        record_job_finish(
            conn,
            run_id,
            status="failure",
            error_msg="simulated",
            error_category=FailureCategory.DB_CONSTRAINT,
        )
        row = conn.execute(
            "SELECT status, error_category FROM job_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "failure"
        assert row[1] == "db_constraint"


@pytest.mark.integration
def test_record_job_finish_without_category_keeps_null() -> None:
    with psycopg.connect(test_database_url()) as conn:
        run_id = record_job_start(conn, "test_job_nocat")
        record_job_finish(conn, run_id, status="failure", error_msg="oops")
        row = conn.execute(
            "SELECT error_category FROM job_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row is not None
        assert row[0] is None
```

- [ ] **Step 2: Run to confirm fail — `TypeError: record_job_finish() got an unexpected keyword argument 'error_category'`.**

- [ ] **Step 3: Extend `record_job_finish`**

In `app/services/ops_monitor.py`, change the signature:

```python
from app.services.sync_orchestrator.layer_types import FailureCategory


def record_job_finish(
    conn: psycopg.Connection[Any],
    run_id: int,
    *,
    status: Literal["success", "failure"],
    row_count: int | None = None,
    error_msg: str | None = None,
    error_category: FailureCategory | None = None,
    now: datetime | None = None,
) -> None:
    now = now or _utcnow()
    conn.execute(
        """
        UPDATE job_runs
        SET finished_at    = %(finished)s,
            status         = %(status)s,
            row_count      = %(row_count)s,
            error_msg      = %(error_msg)s,
            error_category = %(error_category)s
        WHERE run_id = %(run_id)s
        """,
        {
            "finished": now,
            "status": status,
            "row_count": row_count,
            "error_msg": error_msg,
            "error_category": error_category.value if error_category else None,
            "run_id": run_id,
        },
    )
    conn.commit()
```

- [ ] **Step 4: Run tests — 2 green.**

- [ ] **Step 5: Commit**

```bash
git add app/services/ops_monitor.py tests/services/test_ops_monitor_error_category.py
git commit -m "feat(#328): record_job_finish accepts FailureCategory"
```

### Task 3.3: Adapter classifier + `@_categorise` decorator

Adapters currently match `LayerRefresh` protocol: keyword-only args, returns `Sequence[tuple[str, RefreshResult]]`. Do not change that contract. The decorator wraps the existing body and translates exceptions.

**Files:**
- Modify: `app/services/sync_orchestrator/adapters.py`
- Test: `tests/services/sync_orchestrator/test_adapter_categorization.py`

- [ ] **Step 1: Failing test**

```python
# tests/services/sync_orchestrator/test_adapter_categorization.py
from collections.abc import Mapping
from unittest.mock import patch

import httpx
import psycopg.errors
import pytest

from app.services.sync_orchestrator.adapters import (
    refresh_candles,
    refresh_cik_mapping,
)
from app.services.sync_orchestrator.layer_types import (
    FailureCategory,
    LayerRefreshFailed,
)
from app.services.sync_orchestrator.types import LayerOutcome


def _kwargs():
    return dict(
        sync_run_id=1,
        progress=lambda items_done, items_total=None: None,
        upstream_outcomes={},
    )


@pytest.mark.parametrize(
    ("status", "expected_category"),
    [
        (401, FailureCategory.AUTH_EXPIRED),
        (403, FailureCategory.AUTH_EXPIRED),
        (429, FailureCategory.RATE_LIMITED),
        (500, FailureCategory.SOURCE_DOWN),
        (503, FailureCategory.SOURCE_DOWN),
    ],
)
def test_httpx_errors_map_to_categories(
    status: int, expected_category: FailureCategory
) -> None:
    response = httpx.Response(status_code=status, text="error")
    error = httpx.HTTPStatusError(
        "err", request=httpx.Request("GET", "https://example"), response=response
    )
    with patch(
        "app.services.sync_orchestrator.adapters._refresh_candles_impl",
        side_effect=error,
    ):
        with pytest.raises(LayerRefreshFailed) as exc:
            refresh_candles(**_kwargs())
        assert exc.value.category is expected_category


def test_unique_violation_maps_to_db_constraint() -> None:
    err = psycopg.errors.UniqueViolation("duplicate key")
    with patch(
        "app.services.sync_orchestrator.adapters._refresh_cik_mapping_impl",
        side_effect=err,
    ):
        with pytest.raises(LayerRefreshFailed) as exc:
            refresh_cik_mapping(**_kwargs())
        assert exc.value.category is FailureCategory.DB_CONSTRAINT


def test_unknown_exception_maps_to_internal_error() -> None:
    with patch(
        "app.services.sync_orchestrator.adapters._refresh_candles_impl",
        side_effect=RuntimeError("surprise"),
    ):
        with pytest.raises(LayerRefreshFailed) as exc:
            refresh_candles(**_kwargs())
        assert exc.value.category is FailureCategory.INTERNAL_ERROR
```

- [ ] **Step 2: Run to confirm fail** — the impl functions `_refresh_candles_impl` / `_refresh_cik_mapping_impl` don't exist yet.

- [ ] **Step 3: Introduce the classifier + split adapters into `public @_categorise` + `_impl`**

Add to the top of `adapters.py`:

```python
from functools import wraps

import httpx
import psycopg.errors

from app.services.sync_orchestrator.layer_types import (
    FailureCategory,
    LayerRefreshFailed,
)


def _classify(exc: BaseException) -> FailureCategory:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status in (401, 403):
            return FailureCategory.AUTH_EXPIRED
        if status == 429:
            return FailureCategory.RATE_LIMITED
        if 500 <= status < 600:
            return FailureCategory.SOURCE_DOWN
        return FailureCategory.INTERNAL_ERROR
    if isinstance(exc, (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout)):
        return FailureCategory.SOURCE_DOWN
    if isinstance(exc, psycopg.errors.UniqueViolation):
        return FailureCategory.DB_CONSTRAINT
    if isinstance(exc, psycopg.errors.IntegrityError):
        return FailureCategory.DB_CONSTRAINT
    return FailureCategory.INTERNAL_ERROR


def _categorise(fn):
    """Wrap an adapter so any unraised exception becomes a
    LayerRefreshFailed with a classified category. LayerRefreshFailed
    passes through unchanged so manually categorised raises are
    preserved.
    """

    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except LayerRefreshFailed:
            raise
        except Exception as exc:
            raise LayerRefreshFailed(
                category=_classify(exc), detail=str(exc)
            ) from exc

    return wrapper
```

For every `refresh_<name>` function currently defined in adapters.py, split into two:

```python
@_categorise
def refresh_candles(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    return _refresh_candles_impl(
        sync_run_id=sync_run_id,
        progress=progress,
        upstream_outcomes=upstream_outcomes,
    )


def _refresh_candles_impl(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    # existing body moved here verbatim
    ...
```

Repeat for `refresh_universe`, `refresh_cik_mapping`, `refresh_fundamentals`, `refresh_news`, `refresh_thesis`, `refresh_portfolio_sync`, `refresh_cost_models`, `refresh_fx_rates`, `refresh_weekly_reports`, `refresh_monthly_reports`, `refresh_financial_facts_and_normalization`, `refresh_scoring_and_recommendations`. Keep decorated functions as the public entry points wired into `registry.py` — nothing there changes.

- [ ] **Step 4: Update executor to read category off `LayerRefreshFailed`**

In `app/services/sync_orchestrator/executor.py`, find the adapter-call site:

```python
except LayerRefreshFailed as exc:
    # Spec §5: category flows into sync_layer_progress.error_category
    result = RefreshResult(
        outcome=LayerOutcome.FAILED,
        row_count=None,
        error_msg=exc.detail,
        error_category=exc.category.value,
    )
    # persist result into sync_layer_progress via existing write path
```

The generic `except Exception` path stays — anything that bypasses `@_categorise` still produces a typed row (`error_category='internal_error'`).

- [ ] **Step 5: Run tests + full pytest**

```
uv run pytest tests/services/sync_orchestrator/test_adapter_categorization.py -v
uv run pytest tests/services/sync_orchestrator -v
```

- [ ] **Step 6: Commit**

```bash
git add app/services/sync_orchestrator/adapters.py app/services/sync_orchestrator/executor.py tests/services/sync_orchestrator/test_adapter_categorization.py
git commit -m "feat(#328): classify adapter exceptions into FailureCategory"
```

### Task 3.4: Scheduler — extend `_tracked_job` to persist `error_category`

`_tracked_job` in `app/workers/scheduler.py` is a **contextmanager** (not a decorator) that opens the connection, writes `record_job_start`, yields, and writes `record_job_finish` on exit. Extend the exit branch to classify the raised exception. Do not replace it with a decorator. Read the current implementation with `sed -n '/def _tracked_job/,/^def /p' app/workers/scheduler.py` before editing to preserve the existing shape.

**Files:**
- Modify: `app/workers/scheduler.py`
- Test: `tests/workers/test_scheduler_error_category.py`

- [ ] **Step 1: Failing test**

```python
# tests/workers/test_scheduler_error_category.py
from unittest.mock import patch

import psycopg
import psycopg.errors
import pytest

from app.workers.scheduler import daily_cik_refresh
from tests.fixtures.ebull_test_db import test_database_url


@pytest.mark.integration
def test_daily_cik_refresh_uniqueviolation_persists_db_constraint() -> None:
    # Patch the underlying service called by daily_cik_refresh to force
    # a UniqueViolation. The _tracked_job wrapper should catch, classify,
    # and persist error_category='db_constraint'.
    with patch(
        "app.workers.scheduler._refresh_cik_mapping_service",
        side_effect=psycopg.errors.UniqueViolation("dup"),
    ):
        with pytest.raises(psycopg.errors.UniqueViolation):
            daily_cik_refresh()

    with psycopg.connect(test_database_url()) as conn:
        row = conn.execute(
            """
            SELECT status, error_category
            FROM job_runs
            WHERE job_name = 'daily_cik_refresh'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    assert row[0] == "failure"
    assert row[1] == "db_constraint"
```

- [ ] **Step 2: Run to confirm fail** — `error_category` stays NULL because `_tracked_job` doesn't classify yet.

- [ ] **Step 3: Extend `_tracked_job`**

Current `_tracked_job` is a contextmanager: enter → `record_job_start`; exit → `record_job_finish(status="success" | "failure")`. Keep that structure. Add classification on the failure branch. Minimal diff pattern:

```python
from app.services.sync_orchestrator.adapters import _classify
from app.services.sync_orchestrator.layer_types import LayerRefreshFailed


@contextmanager
def _tracked_job(job_name: str) -> Iterator[int]:
    with psycopg.connect(settings.database_url) as conn:
        run_id = record_job_start(conn, job_name)
    try:
        yield run_id
    except LayerRefreshFailed as exc:
        with psycopg.connect(settings.database_url) as conn:
            record_job_finish(
                conn, run_id,
                status="failure",
                error_msg=exc.detail,
                error_category=exc.category,
            )
        raise
    except Exception as exc:
        with psycopg.connect(settings.database_url) as conn:
            record_job_finish(
                conn, run_id,
                status="failure",
                error_msg=str(exc),
                error_category=_classify(exc),
            )
        raise
    else:
        with psycopg.connect(settings.database_url) as conn:
            record_job_finish(conn, run_id, status="success")
```

Preserve any additional arguments the real `_tracked_job` already takes (e.g. `row_count` for success). Do not add a second wrapper layer. Do not change any call site that already uses `with _tracked_job(job_name) as run_id:`.

Also confirm via `grep -n "INSERT INTO job_runs\|UPDATE job_runs" app/workers/scheduler.py` that no other place in the file writes to `job_runs` directly — everything must flow through `_tracked_job`.

- [ ] **Step 4: Run tests**

```
uv run pytest tests/workers/test_scheduler_error_category.py -v
uv run pytest
```

- [ ] **Step 5: Commit**

```bash
git add app/workers/scheduler.py tests/workers/test_scheduler_error_category.py
git commit -m "feat(#328): scheduler _tracked_job classifies failures into FailureCategory"
```

### Task 3.5: Pre-push + push chunk 3

- [ ] Full gate. Push. PR `feat(#328): chunk 3 — classify failures end-to-end`. Poll. Resolve. Merge.

---

## Chunk 4 — `compute_layer_state` pure function + DB glue + layer-enable config

Implements the decision flow. Adds a tiny config module for per-layer enable/disable. Branch: `feature/328-chunk-4-state-fn`.

### Task 4.1: `layer_is_enabled` config service

The state machine's `DISABLED` branch needs a boolean per layer. Today the codebase has no layer-level enable config — add a minimal table + helper.

**Files:**
- Create: `sql/040_layer_enabled.sql`
- Create: `app/services/layer_enabled.py`
- Test: `tests/services/test_layer_enabled.py`

- [ ] **Step 1: Migration**

```sql
-- 040_layer_enabled.sql
CREATE TABLE IF NOT EXISTS layer_enabled (
    layer_name  TEXT PRIMARY KEY,
    is_enabled  BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Apply locally:

```
uv run python -c "from app.db.migrations import run_migrations; run_migrations()"
```

- [ ] **Step 2: Failing test**

```python
# tests/services/test_layer_enabled.py
import psycopg
import pytest

from app.services.layer_enabled import (
    is_layer_enabled,
    set_layer_enabled,
)
from tests.fixtures.ebull_test_db import test_database_url


@pytest.mark.integration
def test_default_missing_row_is_enabled() -> None:
    with psycopg.connect(test_database_url()) as conn:
        conn.execute("DELETE FROM layer_enabled WHERE layer_name = 'candles'")
        assert is_layer_enabled(conn, "candles") is True


@pytest.mark.integration
def test_set_and_read_back() -> None:
    with psycopg.connect(test_database_url()) as conn:
        set_layer_enabled(conn, "candles", enabled=False)
        assert is_layer_enabled(conn, "candles") is False
        set_layer_enabled(conn, "candles", enabled=True)
        assert is_layer_enabled(conn, "candles") is True
```

- [ ] **Step 3: Implement**

```python
# app/services/layer_enabled.py
"""Per-layer enable/disable flag (spec §3.2 rule 1).

Default: enabled. Absent row counts as enabled so adding a new layer
to the registry never surprises an operator with a disabled-by-default
row.
"""

from __future__ import annotations

from typing import Any

import psycopg


def is_layer_enabled(conn: psycopg.Connection[Any], layer_name: str) -> bool:
    row = conn.execute(
        "SELECT is_enabled FROM layer_enabled WHERE layer_name = %s",
        (layer_name,),
    ).fetchone()
    if row is None:
        return True
    return bool(row[0])


def set_layer_enabled(
    conn: psycopg.Connection[Any],
    layer_name: str,
    *,
    enabled: bool,
) -> None:
    conn.execute(
        """
        INSERT INTO layer_enabled (layer_name, is_enabled, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (layer_name) DO UPDATE
          SET is_enabled = EXCLUDED.is_enabled,
              updated_at = now()
        """,
        (layer_name, enabled),
    )
    conn.commit()


def read_all_enabled(conn: psycopg.Connection[Any], names: list[str]) -> dict[str, bool]:
    """Batched read for the state machine — one query for every layer."""
    rows = conn.execute(
        "SELECT layer_name, is_enabled FROM layer_enabled WHERE layer_name = ANY(%s)",
        (names,),
    ).fetchall()
    out = {str(r[0]): bool(r[1]) for r in rows}
    for name in names:
        out.setdefault(name, True)
    return out
```

- [ ] **Step 4: Tests green.**

- [ ] **Step 5: Commit**

```bash
git add sql/035_layer_enabled.sql app/services/layer_enabled.py tests/services/test_layer_enabled.py
git commit -m "feat(#328): layer_enabled table + is_layer_enabled/set_layer_enabled"
```

### Task 4.2: `compute_layer_state` pure function

**Files:**
- Create: `app/services/sync_orchestrator/layer_state.py`
- Test: `tests/services/sync_orchestrator/test_layer_state.py`

- [ ] **Step 1: Failing test (scaffolding + disabled + healthy)**

```python
# tests/services/sync_orchestrator/test_layer_state.py
from app.services.sync_orchestrator.layer_state import (
    LayerContext,
    compute_layer_state,
)
from app.services.sync_orchestrator.layer_types import LayerState


def _ctx(**overrides):
    defaults = dict(
        is_enabled=True,
        is_running=False,
        latest_status="complete",     # sync_layer_progress vocabulary
        latest_category=None,
        attempts=0,
        upstream_states={},
        secret_present=True,
        content_ok=True,
        age_seconds=60,
        cadence_seconds=86400,
        grace_multiplier=1.25,
        max_attempts=3,
    )
    defaults.update(overrides)
    return LayerContext(**defaults)


def test_disabled_overrides_everything() -> None:
    assert compute_layer_state(_ctx(is_enabled=False, latest_status="failed")) is LayerState.DISABLED


def test_healthy_when_all_clean() -> None:
    assert compute_layer_state(_ctx()) is LayerState.HEALTHY
```

- [ ] **Step 2: Run to confirm fail** — `ImportError`.

- [ ] **Step 3: Write the module**

```python
# app/services/sync_orchestrator/layer_state.py
"""Compute LayerState for every registered layer (spec §3.2).

`compute_layer_state(ctx) -> LayerState` is pure: input is a
LayerContext, output is a LayerState. `compute_layer_states_from_db`
builds the context from a live connection + the registry and applies
fixed-point cascade propagation.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import psycopg

from app.services.sync_orchestrator.layer_types import (
    FailureCategory,
    LayerState,
    REMEDIES,
)


@dataclass(frozen=True)
class LayerContext:
    is_enabled: bool
    is_running: bool
    # `latest_status` uses the sync_layer_progress vocabulary:
    # 'complete' | 'failed' | 'skipped' | 'partial' | 'pending' | 'running'.
    # Legacy job_runs ('success'/'failure') is normalised by the caller
    # when reading from that side.
    latest_status: str
    latest_category: str | None
    attempts: int
    upstream_states: dict[str, LayerState]
    secret_present: bool
    content_ok: bool
    age_seconds: float
    cadence_seconds: float
    grace_multiplier: float
    max_attempts: int


def compute_layer_state(ctx: LayerContext) -> LayerState:
    # Rule 1: config override.
    if not ctx.is_enabled:
        return LayerState.DISABLED
    # Rule 2: in-flight.
    if ctx.is_running:
        return LayerState.RUNNING
    # Rule 3: missing secrets beat stale failure rows.
    if not ctx.secret_present:
        return LayerState.SECRET_MISSING
    # Rules 4-6: local-failure branches.
    if ctx.latest_status == "failed":
        category_values = {c.value for c in FailureCategory}
        category = (
            FailureCategory(ctx.latest_category)
            if ctx.latest_category in category_values
            else FailureCategory.INTERNAL_ERROR
        )
        remedy = REMEDIES[category]
        if not remedy.self_heal:
            return LayerState.ACTION_NEEDED
        if ctx.attempts >= ctx.max_attempts:
            return LayerState.ACTION_NEEDED
        return LayerState.RETRYING
    # Rule 7: cascade. Only terminal upstream states cascade.
    if any(
        s in {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING}
        for s in ctx.upstream_states.values()
    ):
        return LayerState.CASCADE_WAITING
    # Rule 8: content predicate.
    if not ctx.content_ok:
        return LayerState.DEGRADED
    # Rule 9: age vs grace window.
    if ctx.age_seconds > ctx.cadence_seconds * ctx.grace_multiplier:
        return LayerState.DEGRADED
    return LayerState.HEALTHY
```

- [ ] **Step 4: 2 tests green.**

- [ ] **Step 5: Commit**

```bash
git add app/services/sync_orchestrator/layer_state.py tests/services/sync_orchestrator/test_layer_state.py
git commit -m "feat(#328): compute_layer_state (disabled + healthy branches)"
```

### Task 4.3: Pin every decision-flow rule

Tasks 4.3.a through 4.3.c add tests that pin rules already covered by 4.2's implementation. These are regression guards, not red-first TDD — the implementation anticipated them. Each sub-task runs the new tests and confirms green, then commits the pin.

- [ ] **Task 4.3.a — RUNNING / SECRET_MISSING / ACTION_NEEDED / RETRYING**

Append to `tests/services/sync_orchestrator/test_layer_state.py`:

```python
def test_running_wins_over_failure() -> None:
    assert compute_layer_state(_ctx(is_running=True, latest_status="failed", attempts=99)) is LayerState.RUNNING


def test_secret_missing_beats_prior_failure() -> None:
    # Rule 3 before rule 4.
    assert compute_layer_state(
        _ctx(secret_present=False, latest_status="failed", latest_category="source_down")
    ) is LayerState.SECRET_MISSING


def test_auth_expired_escalates_on_first_failure() -> None:
    assert compute_layer_state(
        _ctx(latest_status="failed", latest_category="auth_expired", attempts=1)
    ) is LayerState.ACTION_NEEDED


def test_schema_drift_escalates_on_first_failure() -> None:
    assert compute_layer_state(
        _ctx(latest_status="failed", latest_category="schema_drift", attempts=1)
    ) is LayerState.ACTION_NEEDED


def test_db_constraint_escalates_on_first_failure() -> None:
    assert compute_layer_state(
        _ctx(latest_status="failed", latest_category="db_constraint", attempts=1)
    ) is LayerState.ACTION_NEEDED


def test_rate_limited_under_budget_retries() -> None:
    assert compute_layer_state(
        _ctx(latest_status="failed", latest_category="rate_limited", attempts=1)
    ) is LayerState.RETRYING


def test_rate_limited_exhausted_escalates() -> None:
    assert compute_layer_state(
        _ctx(latest_status="failed", latest_category="rate_limited", attempts=3)
    ) is LayerState.ACTION_NEEDED


def test_unknown_category_treated_as_internal_error() -> None:
    assert compute_layer_state(
        _ctx(latest_status="failed", latest_category="totally-made-up", attempts=1)
    ) is LayerState.RETRYING
```

Run + commit:

```
uv run pytest tests/services/sync_orchestrator/test_layer_state.py -v
```

All green. Commit:

```bash
git add tests/services/sync_orchestrator/test_layer_state.py
git commit -m "test(#328): pin ACTION_NEEDED / RETRYING / SECRET_MISSING rules"
```

- [ ] **Task 4.3.b — CASCADE_WAITING + DEGRADED + precedence**

```python
def test_cascade_waiting_on_action_needed_upstream() -> None:
    assert compute_layer_state(
        _ctx(upstream_states={"cik_mapping": LayerState.ACTION_NEEDED})
    ) is LayerState.CASCADE_WAITING


def test_cascade_waiting_on_secret_missing_upstream() -> None:
    assert compute_layer_state(
        _ctx(upstream_states={"news": LayerState.SECRET_MISSING})
    ) is LayerState.CASCADE_WAITING


def test_upstream_degraded_does_not_cascade() -> None:
    assert compute_layer_state(
        _ctx(upstream_states={"financial_facts": LayerState.DEGRADED})
    ) is LayerState.HEALTHY


def test_upstream_retrying_does_not_cascade() -> None:
    assert compute_layer_state(
        _ctx(upstream_states={"financial_facts": LayerState.RETRYING})
    ) is LayerState.HEALTHY


def test_content_predicate_failure_marks_degraded() -> None:
    assert compute_layer_state(_ctx(content_ok=False)) is LayerState.DEGRADED


def test_age_past_grace_marks_degraded() -> None:
    assert compute_layer_state(
        _ctx(age_seconds=80, cadence_seconds=60, grace_multiplier=1.25)
    ) is LayerState.DEGRADED


def test_age_inside_grace_is_healthy() -> None:
    assert compute_layer_state(
        _ctx(age_seconds=70, cadence_seconds=60, grace_multiplier=1.25)
    ) is LayerState.HEALTHY


def test_local_failure_beats_cascade() -> None:
    # Spec §3.2 rule 4 precedes rule 7.
    assert compute_layer_state(
        _ctx(
            latest_status="failed",
            latest_category="schema_drift",
            upstream_states={"cik_mapping": LayerState.ACTION_NEEDED},
        )
    ) is LayerState.ACTION_NEEDED
```

Run + commit:

```bash
uv run pytest tests/services/sync_orchestrator/test_layer_state.py -v
git add tests/services/sync_orchestrator/test_layer_state.py
git commit -m "test(#328): pin CASCADE_WAITING / DEGRADED / precedence rules"
```

### Task 4.4: `compute_layer_states_from_db` with fixed-point cascade

**Files:**
- Modify: `app/services/sync_orchestrator/layer_state.py`
- Test: `tests/services/sync_orchestrator/test_layer_state_from_db.py`

- [ ] **Step 1: Failing test**

```python
# tests/services/sync_orchestrator/test_layer_state_from_db.py
import psycopg
import pytest

from app.services.sync_orchestrator.layer_state import (
    compute_layer_states_from_db,
)
from app.services.sync_orchestrator.layer_types import LayerState
from app.services.sync_orchestrator.registry import LAYERS
from tests.fixtures.ebull_test_db import test_database_url


@pytest.mark.integration
def test_every_registered_layer_gets_a_state() -> None:
    with psycopg.connect(test_database_url()) as conn:
        states = compute_layer_states_from_db(conn)
    assert set(states.keys()) == set(LAYERS.keys())
    for state in states.values():
        assert isinstance(state, LayerState)


@pytest.mark.integration
def test_dag_depth_assumption() -> None:
    # Fixed-point iteration runs up to depth rounds. Update the cap if
    # the registry grows deeper.
    max_depth = _longest_path(LAYERS)
    assert max_depth <= 6, f"registry depth {max_depth} exceeds cascade iteration cap"


def _longest_path(layers) -> int:
    memo: dict[str, int] = {}
    def depth(name: str) -> int:
        if name in memo: return memo[name]
        deps = layers[name].dependencies
        d = 0 if not deps else 1 + max(depth(dep) for dep in deps)
        memo[name] = d
        return d
    return max((depth(n) for n in layers), default=0)
```

- [ ] **Step 2: Extend `layer_state.py`** with the DB-facing builder + fixed-point cascade

Append:

```python
from app.services.layer_enabled import read_all_enabled
from app.services.sync_orchestrator.layer_failure_history import (
    all_layer_histories,
)
from app.services.sync_orchestrator.registry import LAYERS, DataLayer


def compute_layer_states_from_db(
    conn: psycopg.Connection[Any],
) -> dict[str, LayerState]:
    names = list(LAYERS.keys())
    enabled = read_all_enabled(conn, names)
    streaks, categories = all_layer_histories(conn, names)
    running_set = _running_layers(conn, names)
    latest_status = _latest_status_map(conn, names)
    latest_ages = _latest_age_seconds_map(conn, names)
    content_results = _content_ok_map(conn)

    def build(upstream: dict[str, LayerState], name: str) -> LayerContext:
        layer = LAYERS[name]
        status = latest_status.get(name, "__never_run__")
        # Never-run layer: age ≫ grace → rule 9 fires DEGRADED. Normalise
        # the status sentinel to "complete" here so compute_layer_state's
        # "failed" branch doesn't mis-trigger on the sentinel string.
        if status == "__never_run__":
            age_seconds = float("inf")
            status = "complete"
        else:
            age_seconds = latest_ages.get(name, float("inf"))
        return LayerContext(
            is_enabled=enabled.get(name, True),
            is_running=name in running_set,
            latest_status=status,
            latest_category=categories.get(name),
            attempts=streaks.get(name, 0),
            upstream_states=upstream,
            secret_present=all(
                bool(os.environ.get(ref.env_var)) for ref in layer.secret_refs
            ),
            content_ok=content_results.get(name, True),
            age_seconds=age_seconds,
            cadence_seconds=layer.cadence.interval.total_seconds(),
            grace_multiplier=layer.grace_multiplier,
            max_attempts=layer.retry_policy.max_attempts,
        )

    # Round 0: compute every layer without upstream info. Yields local
    # state (RETRYING / ACTION_NEEDED / SECRET_MISSING / DEGRADED /
    # HEALTHY / DISABLED / RUNNING). CASCADE_WAITING is impossible in
    # round 0 because upstream_states is empty.
    current: dict[str, LayerState] = {
        name: compute_layer_state(build({}, name)) for name in names
    }

    # Fixed-point: re-evaluate with populated upstream_states until no
    # layer's state changes. This converges in at most max-DAG-depth
    # rounds because each round propagates one level of cascade.
    max_iterations = 16  # safety — the DAG depth is 4 today; room to grow.
    for _ in range(max_iterations):
        next_states: dict[str, LayerState] = {}
        for name in names:
            upstream = {
                dep: current[dep] for dep in LAYERS[name].dependencies
            }
            next_states[name] = compute_layer_state(build(upstream, name))
        if next_states == current:
            return next_states
        current = next_states
    return current


def _running_layers(conn: psycopg.Connection[Any], names: list[str]) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT layer_name
        FROM sync_layer_progress
        WHERE status = 'running' AND layer_name = ANY(%s)
        """,
        (names,),
    ).fetchall()
    return {str(r[0]) for r in rows}


def _latest_status_map(conn: psycopg.Connection[Any], names: list[str]) -> dict[str, str]:
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                layer_name, status,
                ROW_NUMBER() OVER (
                    PARTITION BY layer_name
                    ORDER BY started_at DESC NULLS LAST, sync_run_id DESC
                ) AS rn
            FROM sync_layer_progress
            WHERE layer_name = ANY(%s)
        )
        SELECT layer_name, status FROM ranked WHERE rn = 1
        """,
        (names,),
    ).fetchall()
    out = {str(r[0]): str(r[1]) for r in rows}
    # Never-run layer: we return a sentinel that the context-builder
    # maps to a DEGRADED state (age=infinity). Do NOT default to
    # "complete" — that would mark a layer HEALTHY despite having no
    # runs on record.
    for name in names:
        out.setdefault(name, "__never_run__")
    return out


def _latest_age_seconds_map(
    conn: psycopg.Connection[Any], names: list[str]
) -> dict[str, float]:
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                layer_name,
                COALESCE(finished_at, started_at) AS anchor,
                ROW_NUMBER() OVER (
                    PARTITION BY layer_name
                    ORDER BY started_at DESC NULLS LAST, sync_run_id DESC
                ) AS rn
            FROM sync_layer_progress
            WHERE layer_name = ANY(%s) AND status IN ('complete', 'partial', 'skipped')
        )
        SELECT layer_name, EXTRACT(EPOCH FROM (now() - anchor)) AS age
        FROM ranked WHERE rn = 1
        """,
        (names,),
    ).fetchall()
    return {str(r[0]): float(r[1]) for r in rows}


def _content_ok_map(conn: psycopg.Connection[Any]) -> dict[str, bool]:
    """Invoke each layer's `content_predicate` if one is declared.
    Layers without a predicate default to True (content checks are
    opt-in per spec §4)."""
    out: dict[str, bool] = {}
    for name, layer in LAYERS.items():
        if layer.content_predicate is None:
            out[name] = True
            continue
        try:
            ok, _detail = layer.content_predicate(conn)
        except Exception:
            # A broken predicate is not a freshness signal — log and
            # treat as content-ok to avoid masking real failures in the
            # state machine with transient predicate errors. Chunk 7
            # moves content-predicate errors into their own log path.
            ok = True
        out[name] = ok
    return out
```

- [ ] **Step 3: Tests green.**

- [ ] **Step 4: Commit**

```bash
git add app/services/sync_orchestrator/layer_state.py tests/services/sync_orchestrator/test_layer_state_from_db.py
git commit -m "feat(#328): compute_layer_states_from_db with fixed-point cascade"
```

### Task 4.5: Pre-push + push chunk 4

- [ ] Full gate. Push. PR `feat(#328): chunk 4 — layer state computation`. Poll. Resolve. Merge.

---

## Chunk 5 — `/sync/layers/v2` endpoint

New endpoint. v1 untouched. Branch: `feature/328-chunk-5-v2-endpoint`.

### Task 5.1: Pydantic models + endpoint

**Files:**
- Modify: `app/api/sync.py`
- Test: `tests/api/test_sync_layers_v2_schema.py`

- [ ] **Step 1: Failing test**

```python
# tests/api/test_sync_layers_v2_schema.py
from fastapi.testclient import TestClient

from app.main import app


def test_v2_endpoint_returns_expected_top_level_keys() -> None:
    with TestClient(app) as client:
        resp = client.get("/sync/layers/v2")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == {
        "generated_at",
        "system_state",
        "system_summary",
        "action_needed",
        "degraded",
        "secret_missing",
        "healthy",
        "disabled",
        "cascade_groups",
    }


def test_v2_system_state_value() -> None:
    with TestClient(app) as client:
        resp = client.get("/sync/layers/v2")
    assert resp.json()["system_state"] in {"ok", "catching_up", "needs_attention"}


def test_v2_healthy_entries_have_layer_and_last_updated() -> None:
    with TestClient(app) as client:
        resp = client.get("/sync/layers/v2")
    for entry in resp.json()["healthy"]:
        assert set(entry.keys()) >= {"layer", "display_name", "last_updated"}
```

- [ ] **Step 2: Run — 404.**

- [ ] **Step 3: Add models + endpoint to `app/api/sync.py`**

Append to the file (the router + `get_conn` dependency already exist):

```python
import os
from datetime import datetime, UTC

from pydantic import BaseModel

from app.services.sync_orchestrator.cascade import collapse_cascades
from app.services.sync_orchestrator.layer_failure_history import (
    all_layer_histories,
)
from app.services.sync_orchestrator.layer_state import (
    compute_layer_states_from_db,
)
from app.services.sync_orchestrator.layer_types import (
    FailureCategory,
    LayerState,
    REMEDIES,
)
from app.services.sync_orchestrator.registry import LAYERS


class ActionNeededItem(BaseModel):
    root_layer: str
    display_name: str
    category: str
    operator_message: str
    operator_fix: str | None
    self_heal: bool
    consecutive_failures: int
    affected_downstream: list[str]


class SecretMissingItem(BaseModel):
    layer: str
    display_name: str
    missing_secret: str
    operator_fix: str


class LayerSummary(BaseModel):
    layer: str
    display_name: str
    last_updated: datetime | None


class CascadeGroupModel(BaseModel):
    root: str
    affected: list[str]


class SyncLayersV2Response(BaseModel):
    generated_at: datetime
    system_state: str
    system_summary: str
    action_needed: list[ActionNeededItem]
    degraded: list[LayerSummary]
    secret_missing: list[SecretMissingItem]
    healthy: list[LayerSummary]
    disabled: list[LayerSummary]
    cascade_groups: list[CascadeGroupModel]


@router.get("/layers/v2", response_model=SyncLayersV2Response)
def get_sync_layers_v2(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> SyncLayersV2Response:
    states = compute_layer_states_from_db(conn)
    names = list(states.keys())
    streaks, categories = all_layer_histories(conn, names)
    last_updates = _layer_last_updated_map(conn, names)

    if any(s in {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING} for s in states.values()):
        system_state = "needs_attention"
    elif any(
        s in {LayerState.DEGRADED, LayerState.RUNNING, LayerState.RETRYING, LayerState.CASCADE_WAITING}
        for s in states.values()
    ):
        system_state = "catching_up"
    else:
        system_state = "ok"

    action_needed: list[ActionNeededItem] = []
    secret_missing: list[SecretMissingItem] = []
    degraded: list[LayerSummary] = []
    healthy: list[LayerSummary] = []
    disabled: list[LayerSummary] = []

    deps_map = {name: layer.dependencies for name, layer in LAYERS.items()}
    groups = collapse_cascades(deps_map, states)
    groups_by_root = {g.root: g for g in groups}

    category_values = {c.value for c in FailureCategory}

    for name, state in states.items():
        layer = LAYERS[name]
        summary = LayerSummary(
            layer=name,
            display_name=layer.display_name,
            last_updated=last_updates.get(name),
        )
        if state is LayerState.ACTION_NEEDED:
            raw_cat = categories.get(name) or "internal_error"
            category = FailureCategory(raw_cat) if raw_cat in category_values else FailureCategory.INTERNAL_ERROR
            remedy = REMEDIES[category]
            affected = groups_by_root[name].affected if name in groups_by_root else []
            action_needed.append(ActionNeededItem(
                root_layer=name,
                display_name=layer.display_name,
                category=category.value,
                operator_message=remedy.message,
                operator_fix=remedy.operator_fix,
                self_heal=remedy.self_heal,
                consecutive_failures=streaks.get(name, 0),
                affected_downstream=affected,
            ))
        elif state is LayerState.SECRET_MISSING:
            missing = next(
                (ref for ref in layer.secret_refs if not os.environ.get(ref.env_var)),
                None,
            )
            if missing is not None:
                secret_missing.append(SecretMissingItem(
                    layer=name,
                    display_name=layer.display_name,
                    missing_secret=missing.env_var,
                    operator_fix=f"Set {missing.env_var} in Settings → Providers",
                ))
        elif state is LayerState.DEGRADED:
            degraded.append(summary)
        elif state is LayerState.HEALTHY:
            healthy.append(summary)
        elif state is LayerState.DISABLED:
            disabled.append(summary)
        # RUNNING, RETRYING, CASCADE_WAITING feed into system_summary
        # (via counts) and the cascade_groups array; not bucketed to a
        # top-level list.

    return SyncLayersV2Response(
        generated_at=datetime.now(UTC),
        system_state=system_state,
        system_summary=_system_summary(action_needed, secret_missing, degraded),
        action_needed=action_needed,
        degraded=degraded,
        secret_missing=secret_missing,
        healthy=healthy,
        disabled=disabled,
        cascade_groups=[CascadeGroupModel(root=g.root, affected=g.affected) for g in groups],
    )


def _system_summary(
    action_needed: list[ActionNeededItem],
    secret_missing: list[SecretMissingItem],
    degraded: list[LayerSummary],
) -> str:
    if action_needed:
        first = action_needed[0].display_name
        count = len(action_needed)
        return f"{count} layer(s) need attention ({first})" if count > 1 else f"{first} needs attention"
    if secret_missing:
        return f"{len(secret_missing)} layer(s) missing credentials"
    if degraded:
        return f"{len(degraded)} layer(s) catching up"
    return "All layers healthy"


def _layer_last_updated_map(
    conn: psycopg.Connection[object], names: list[str]
) -> dict[str, datetime]:
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                layer_name, finished_at,
                ROW_NUMBER() OVER (
                    PARTITION BY layer_name
                    ORDER BY started_at DESC NULLS LAST, sync_run_id DESC
                ) AS rn
            FROM sync_layer_progress
            WHERE layer_name = ANY(%s) AND status IN ('complete', 'partial')
        )
        SELECT layer_name, finished_at FROM ranked WHERE rn = 1
        """,
        (names,),
    ).fetchall()
    return {str(r[0]): r[1] for r in rows if r[1] is not None}
```

**Note:** `collapse_cascades` is implemented in chunk 6 task 6.1, which is ordered after this one. Chunk 5's cascade block is a forward reference — until chunk 6 lands, implement a trivial inline version here:

```python
# Placeholder until chunk 6: compute a shallow list of groups inline.
# Delete when chunk 6 merges and replace with the import above.
from dataclasses import dataclass


@dataclass(frozen=True)
class _InlineGroup:
    root: str
    affected: list[str]


def _inline_collapse(
    deps_map: dict[str, tuple[str, ...]],
    states: dict[str, LayerState],
) -> list[_InlineGroup]:
    terminal = {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING}
    roots = [n for n, s in states.items() if s in terminal]
    out = []
    for root in roots:
        affected: list[str] = []
        frontier = {root}
        visited = {root}
        while frontier:
            next_frontier: set[str] = set()
            for n, deps in deps_map.items():
                if n in visited:
                    continue
                if any(dep in frontier for dep in deps):
                    visited.add(n)
                    if states.get(n) is LayerState.CASCADE_WAITING:
                        affected.append(n)
                    next_frontier.add(n)
            frontier = next_frontier
        out.append(_InlineGroup(root=root, affected=affected))
    return out
```

Use `_inline_collapse` instead of `collapse_cascades` in chunk 5; chunk 6 task 6.1 replaces the inline block with the import.

- [ ] **Step 4: Tests green.**

- [ ] **Step 5: Commit**

```bash
git add app/api/sync.py tests/api/test_sync_layers_v2_schema.py
git commit -m "feat(#328): add GET /sync/layers/v2 endpoint"
```

### Task 5.2: v1 byte-identity guard

**Files:**
- Create: `tests/api/test_sync_layers_v1_unchanged.py`

- [ ] **Step 1: Pin the current v1 shape**

```python
# tests/api/test_sync_layers_v1_unchanged.py
"""Guard: /sync/layers (v1) must not gain or lose fields in this PR.

If a later refactor deliberately retires v1, delete this test in the
same PR that removes the endpoint."""

from fastapi.testclient import TestClient

from app.main import app


EXPECTED_LAYER_KEYS = {
    "name",
    "display_name",
    "tier",
    "is_fresh",
    "freshness_detail",
    "last_success_at",
    "last_duration_seconds",
    "last_error_category",
    "consecutive_failures",
    "dependencies",
    "is_blocking",
}


def test_v1_top_level_shape() -> None:
    with TestClient(app) as client:
        resp = client.get("/sync/layers")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == {"layers"}


def test_v1_layer_keys_unchanged() -> None:
    with TestClient(app) as client:
        resp = client.get("/sync/layers")
    for layer in resp.json()["layers"]:
        assert set(layer.keys()) == EXPECTED_LAYER_KEYS
```

- [ ] **Step 2: Tests green.**

- [ ] **Step 3: Commit**

```bash
git add tests/api/test_sync_layers_v1_unchanged.py
git commit -m "test(#328): pin /sync/layers v1 shape against accidental drift"
```

### Task 5.3: Pre-push + push chunk 5

- [ ] Full gate. Push. PR `feat(#328): chunk 5 — /sync/layers/v2 endpoint`. Poll. Resolve. Merge.

---

## Chunk 6 — Cascade pure function + scoped resync

Extracts `collapse_cascades` (replaces the inline shim from chunk 5), extends `SyncRequest` with `scope`, and wires the Admin button to `scope="behind"`. Branch: `feature/328-chunk-6-scope`.

### Task 6.1: `collapse_cascades` pure function

**Files:**
- Create: `app/services/sync_orchestrator/cascade.py`
- Modify: `app/api/sync.py` (replace `_inline_collapse` import with real one)
- Test: `tests/services/sync_orchestrator/test_cascade.py`

- [ ] **Step 1: Failing tests**

```python
# tests/services/sync_orchestrator/test_cascade.py
from app.services.sync_orchestrator.cascade import (
    ProblemGroup,
    collapse_cascades,
)
from app.services.sync_orchestrator.layer_types import LayerState


def _graph() -> dict[str, tuple[str, ...]]:
    return {
        "universe": (),
        "cik_mapping": ("universe",),
        "candles": ("universe",),
        "financial_facts": ("cik_mapping",),
        "financial_normalization": ("financial_facts",),
        "fundamentals": ("universe",),
        "news": ("universe",),
        "thesis": ("fundamentals", "financial_normalization", "news"),
        "scoring": ("thesis", "candles"),
        "recommendations": ("scoring",),
    }


def test_single_root_collapses_transitive_downstream() -> None:
    states = {name: LayerState.CASCADE_WAITING for name in _graph()}
    states["universe"] = LayerState.ACTION_NEEDED
    groups = collapse_cascades(_graph(), states)
    assert len(groups) == 1
    assert groups[0].root == "universe"
    assert set(groups[0].affected) == {
        "cik_mapping", "candles", "financial_facts",
        "financial_normalization", "fundamentals", "news",
        "thesis", "scoring", "recommendations",
    }


def test_multiple_roots_produce_multiple_groups() -> None:
    states = {name: LayerState.HEALTHY for name in _graph()}
    states["cik_mapping"] = LayerState.ACTION_NEEDED
    states["news"] = LayerState.SECRET_MISSING
    for name in ("financial_facts", "financial_normalization", "thesis", "scoring", "recommendations"):
        states[name] = LayerState.CASCADE_WAITING
    groups = collapse_cascades(_graph(), states)
    assert {g.root for g in groups} == {"cik_mapping", "news"}


def test_healthy_descendant_not_in_affected() -> None:
    states = {name: LayerState.HEALTHY for name in _graph()}
    states["cik_mapping"] = LayerState.ACTION_NEEDED
    # financial_facts stays healthy — not affected.
    groups = collapse_cascades(_graph(), states)
    assert "financial_facts" not in groups[0].affected


def test_degraded_root_produces_no_group() -> None:
    states = {name: LayerState.HEALTHY for name in _graph()}
    states["cik_mapping"] = LayerState.DEGRADED
    assert collapse_cascades(_graph(), states) == []
```

- [ ] **Step 2: Implement**

```python
# app/services/sync_orchestrator/cascade.py
"""Group CASCADE_WAITING layers under their terminal-blocked root (spec §6)."""

from __future__ import annotations

from dataclasses import dataclass

from app.services.sync_orchestrator.layer_types import LayerState


@dataclass(frozen=True)
class ProblemGroup:
    root: str
    affected: list[str]


def collapse_cascades(
    dependencies: dict[str, tuple[str, ...]],
    states: dict[str, LayerState],
) -> list[ProblemGroup]:
    terminal = {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING}
    roots = [n for n, s in states.items() if s in terminal]

    groups: list[ProblemGroup] = []
    for root in roots:
        affected: list[str] = []
        frontier = {root}
        visited = {root}
        while frontier:
            next_frontier: set[str] = set()
            for name, deps in dependencies.items():
                if name in visited:
                    continue
                if any(dep in frontier for dep in deps):
                    visited.add(name)
                    if states.get(name) is LayerState.CASCADE_WAITING:
                        affected.append(name)
                    next_frontier.add(name)
            frontier = next_frontier
        groups.append(ProblemGroup(root=root, affected=affected))
    return groups
```

- [ ] **Step 3: Replace `_inline_collapse` in `app/api/sync.py` with the real import**

```python
from app.services.sync_orchestrator.cascade import (
    ProblemGroup,
    collapse_cascades,
)
```

Delete the inline `_InlineGroup` dataclass + `_inline_collapse` function. Update the endpoint body to use `collapse_cascades`.

- [ ] **Step 4: Tests green.**

- [ ] **Step 5: Commit**

```bash
git add app/services/sync_orchestrator/cascade.py app/api/sync.py tests/services/sync_orchestrator/test_cascade.py
git commit -m "refactor(#328): extract collapse_cascades pure function"
```

### Task 6.2: `scope` on `POST /sync` + planner filter

**Files:**
- Modify: `app/api/sync.py`
- Modify: `app/services/sync_orchestrator/planner.py`
- Modify: `app/services/sync_orchestrator/__init__.py` (if it re-exports planner helpers)
- Test: `tests/api/test_sync_scope.py`

- [ ] **Step 1: Failing tests**

```python
# tests/api/test_sync_scope.py
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app


def test_empty_body_defaults_to_behind() -> None:
    with patch("app.api.sync.submit_sync") as submit, TestClient(app) as client:
        submit.return_value = (1, [])
        resp = client.post("/sync", json={})
        assert resp.status_code == 202
        scope_arg = submit.call_args.args[0]
        assert scope_arg.kind == "behind"


def test_explicit_full_scope() -> None:
    with patch("app.api.sync.submit_sync") as submit, TestClient(app) as client:
        submit.return_value = (1, [])
        resp = client.post("/sync", json={"scope": "full"})
        assert resp.status_code == 202
        assert submit.call_args.args[0].kind == "full"


def test_layer_colon_scope() -> None:
    with patch("app.api.sync.submit_sync") as submit, TestClient(app) as client:
        submit.return_value = (1, [])
        resp = client.post("/sync", json={"scope": "layer:candles"})
        assert resp.status_code == 202
        scope_arg = submit.call_args.args[0]
        assert scope_arg.kind == "layer"
        assert scope_arg.layer_name == "candles"


def test_unknown_scope_is_400() -> None:
    with TestClient(app) as client:
        resp = client.post("/sync", json={"scope": "wat"})
    assert resp.status_code == 400


def test_layer_unknown_name_is_400() -> None:
    with TestClient(app) as client:
        resp = client.post("/sync", json={"scope": "layer:not_a_layer"})
    assert resp.status_code == 400
```

- [ ] **Step 2: Extend `SyncRequest` + `_scope_from`**

In `app/api/sync.py` — find the existing `SyncRequest` Pydantic model and `_scope_from` helper. Extend:

```python
from dataclasses import dataclass
from typing import Literal

from app.services.sync_orchestrator.registry import LAYERS


class SyncRequest(BaseModel):
    # existing fields stay
    scope: str = "behind"


@dataclass(frozen=True)
class ScopeSpec:
    kind: Literal["behind", "full", "layer"]
    layer_name: str | None = None


def _scope_from(body: SyncRequest) -> ScopeSpec:
    raw = body.scope or "behind"
    if raw in {"behind", "full"}:
        return ScopeSpec(kind=raw)
    if raw.startswith("layer:"):
        layer_name = raw.split(":", 1)[1]
        if layer_name not in LAYERS:
            raise HTTPException(400, f"unknown layer: {layer_name}")
        return ScopeSpec(kind="layer", layer_name=layer_name)
    raise HTTPException(400, f"unknown scope: {raw}")
```

Note: if `_scope_from` previously returned something else (e.g. a different dataclass), update `submit_sync` to accept the new `ScopeSpec`. Check the existing signature via `grep -n "def submit_sync" app/services/sync_orchestrator`. If the existing `scope` parameter shape is preserved in the DB (`sync_runs.scope` is `CHECK (scope IN ('full', 'layer', 'high_frequency', 'job'))` per `sql/033_sync_orchestrator.sql:15`), the persisted value for the `behind` case maps to a new permitted value — add that to the CHECK in a follow-up migration within chunk 6:

```sql
-- sql/041_sync_runs_scope_behind.sql
ALTER TABLE sync_runs
    DROP CONSTRAINT IF EXISTS sync_runs_scope_check;
ALTER TABLE sync_runs
    ADD CONSTRAINT sync_runs_scope_check
    CHECK (scope IN ('full', 'layer', 'behind', 'high_frequency', 'job'));
```

Apply with `uv run python -c "from app.db.migrations import run_migrations; run_migrations()"`.

- [ ] **Step 3: Planner filter**

In `app/services/sync_orchestrator/planner.py`, add:

```python
from app.services.sync_orchestrator.layer_types import LayerState


def plan_behind(
    full_plan: list[str],
    states: dict[str, LayerState],
) -> list[str]:
    """Filter a full plan to only the layers that are DEGRADED or
    ACTION_NEEDED, plus any non-healthy upstreams they depend on.

    Spec §7: scope=behind targets layers that have actually fallen behind;
    RETRYING is excluded because the orchestrator already has a retry on
    the books and a manual fire would race.
    """
    needs_work: set[str] = {
        name for name, state in states.items()
        if state in {LayerState.DEGRADED, LayerState.ACTION_NEEDED}
    }
    if not needs_work:
        return []
    target = set(needs_work)
    from app.services.sync_orchestrator.registry import LAYERS
    for name in list(needs_work):
        target.update(_upstreams_not_healthy(name, states, LAYERS))
    # Preserve topological order by filtering the full plan.
    return [n for n in full_plan if n in target]


def plan_layer(
    layer_name: str,
    full_plan: list[str],
) -> list[str]:
    """Fire one layer only — no upstream auto-fire. Engineer debugging
    path per spec §7. If the layer has unhealthy upstreams the operator
    is expected to resolve them first."""
    if layer_name not in full_plan:
        return [layer_name]
    return [layer_name]


def _upstreams_not_healthy(
    name: str,
    states: dict[str, LayerState],
    layers: dict,
) -> set[str]:
    out: set[str] = set()
    frontier = set(layers[name].dependencies)
    while frontier:
        next_frontier: set[str] = set()
        for n in frontier:
            if n in out:
                continue
            if states.get(n) is not LayerState.HEALTHY:
                out.add(n)
            next_frontier.update(layers[n].dependencies)
        frontier = next_frontier
    return out
```

Wire `submit_sync` to pick `plan_behind` / `plan_full` / `plan_layer` based on `ScopeSpec.kind`. Preserve the existing `plan_full` behaviour unchanged under `scope=full`.

- [ ] **Step 4: Tests green.**

```
uv run pytest tests/api/test_sync_scope.py -v
uv run pytest tests/services/sync_orchestrator -v
```

- [ ] **Step 5: Commit**

```bash
git add app/api/sync.py app/services/sync_orchestrator/planner.py sql/041_sync_runs_scope_behind.sql tests/api/test_sync_scope.py
git commit -m "feat(#328): scope on POST /sync — behind|full|layer:X"
```

### Task 6.3: Admin Sync-now sends `scope: "behind"`

**Files:**
- Modify: `frontend/src/api/sync.ts`
- Modify: `frontend/src/lib/useSyncTrigger.ts`
- Test: `frontend/src/lib/useSyncTrigger.test.ts`

- [ ] **Step 1: Failing test**

Append to `frontend/src/lib/useSyncTrigger.test.ts`:

```ts
import { describe, expect, it, vi } from "vitest";
import { renderHook, act } from "@testing-library/react";

import { useSyncTrigger } from "./useSyncTrigger";


describe("useSyncTrigger scope default", () => {
  it("posts JSON body with scope=behind", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ sync_run_id: 1, plan: [] }), { status: 202 })
    );
    vi.stubGlobal("fetch", fetchMock);
    const { result } = renderHook(() => useSyncTrigger(() => {}));
    await act(async () => {
      await result.current.trigger();
    });
    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body as string).scope).toBe("behind");
  });
});
```

- [ ] **Step 2: Update the API call**

Repo convention: all API calls go through `apiFetch` in `@/api/client`. The existing `triggerSync` helper in `frontend/src/api/sync.ts` (line ~117) posts to `/sync`. Extend its signature instead of introducing a new function:

```ts
// frontend/src/api/sync.ts (extend existing triggerSync)
export type SyncScope = "behind" | "full" | `layer:${string}`;


export async function triggerSync(scope: SyncScope = "behind"): Promise<SyncTriggerResponse> {
  return apiFetch<SyncTriggerResponse>("/sync", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scope }),
  });
}
```

`frontend/src/lib/useSyncTrigger.ts` calls `triggerSync()` with no argument; the default `"behind"` flows through.

- [ ] **Step 3: Tests green + typecheck**

```
pnpm --dir frontend test -- useSyncTrigger
pnpm --dir frontend typecheck
```

- [ ] **Step 4: Commit**

```bash
git add frontend/src/api/sync.ts frontend/src/lib/useSyncTrigger.ts frontend/src/lib/useSyncTrigger.test.ts
git commit -m "feat(#328): admin Sync now issues scope=behind"
```

### Task 6.4: Pre-push + push chunk 6

- [ ] Full gate (backend + frontend). Push. PR `feat(#328): chunk 6 — scoped resync + cascade fn`. Poll. Resolve. Merge.

---

## Chunk 7 — `ops_monitor` retirement

Moves row-count spike detection into `sync_orchestrator/row_count_spikes.py`, deletes `LayerName` + `_STALENESS_THRESHOLDS` + `evaluate_staleness`, and updates `/health` in `app/main.py` to derive from layer state. Branch: `feature/328-chunk-7-ops-monitor-retirement`.

### Task 7.1: Move spike detection

**Files:**
- Create: `app/services/sync_orchestrator/row_count_spikes.py`
- Modify: `app/services/ops_monitor.py` (delete the moved code)
- Test: `tests/services/sync_orchestrator/test_row_count_spikes.py`

- [ ] **Step 1: Audit the code to move**

```
grep -n "row_count\|spike\|_STALENESS_THRESHOLDS\|evaluate_staleness\|LayerName" d:/Repos/eBull/app/services/ops_monitor.py
```

Inventory the symbols to move vs delete. Spike detection moves; staleness constants + `evaluate_staleness` + `LayerName` are deleted in task 7.2.

- [ ] **Step 2: Failing test**

```python
# tests/services/sync_orchestrator/test_row_count_spikes.py
from unittest.mock import MagicMock

from app.services.sync_orchestrator.row_count_spikes import (
    SpikeDirection,
    detect_spike,
)


def test_detect_spike_drop() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (1000,)
    result = detect_spike(conn, job_name="daily_candle_refresh", current_row_count=400)
    assert result.direction is SpikeDirection.DROP
    assert result.severity > 0


def test_detect_spike_within_tolerance() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (1000,)
    result = detect_spike(conn, job_name="daily_candle_refresh", current_row_count=950)
    assert result.direction is SpikeDirection.NONE
```

- [ ] **Step 3: Move functions verbatim**

Cut the spike-related code from `ops_monitor.py` and paste it into `app/services/sync_orchestrator/row_count_spikes.py`. Keep signatures identical. Update callers:

```
grep -rn "from app.services.ops_monitor import.*detect_spike\|detect_spike" app tests
```
Each import becomes `from app.services.sync_orchestrator.row_count_spikes import ...`.

- [ ] **Step 4: Tests green.**

```
uv run pytest tests/services/sync_orchestrator/test_row_count_spikes.py -v
uv run pytest
```

- [ ] **Step 5: Commit**

```bash
git add app/services/sync_orchestrator/row_count_spikes.py app/services/ops_monitor.py tests/services/sync_orchestrator/test_row_count_spikes.py
git commit -m "refactor(#328): move row-count spike detection out of ops_monitor"
```

### Task 7.2: Retire staleness + update `/health`

**Files:**
- Modify: `app/services/ops_monitor.py`
- Modify: `app/main.py` (the `/health` handler)
- Test: `tests/test_main_health.py` (new)

- [ ] **Step 1: Failing test**

```python
# tests/test_main_health.py
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app
from app.services.sync_orchestrator.layer_types import LayerState


def test_health_is_200_when_all_healthy() -> None:
    fake = {"candles": LayerState.HEALTHY, "cik_mapping": LayerState.HEALTHY}
    with patch(
        "app.services.sync_orchestrator.layer_state.compute_layer_states_from_db",
        return_value=fake,
    ), TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["system_state"] == "ok"


def test_health_is_503_when_action_needed() -> None:
    fake = {
        "candles": LayerState.HEALTHY,
        "cik_mapping": LayerState.ACTION_NEEDED,
    }
    with patch(
        "app.services.sync_orchestrator.layer_state.compute_layer_states_from_db",
        return_value=fake,
    ), TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 503
    assert resp.json()["system_state"] == "needs_attention"


def test_health_preserves_env_fields() -> None:
    # Back-compat: existing /health consumers expect `env` and `etoro_env`.
    fake = {"candles": LayerState.HEALTHY}
    with patch(
        "app.services.sync_orchestrator.layer_state.compute_layer_states_from_db",
        return_value=fake,
    ), TestClient(app) as client:
        resp = client.get("/health")
    body = resp.json()
    assert "env" in body
    assert "etoro_env" in body
```

- [ ] **Step 2: Replace `/health` in `app/main.py`**

```python
from fastapi.responses import JSONResponse

from app.dependencies import get_conn
from app.services.sync_orchestrator.layer_state import compute_layer_states_from_db
from app.services.sync_orchestrator.layer_types import LayerState


@app.get("/health")
def health(conn: psycopg.Connection[object] = Depends(get_conn)) -> JSONResponse:
    try:
        states = compute_layer_states_from_db(conn)
    except Exception as exc:
        return JSONResponse(
            {"status": "error", "error": str(exc), "env": settings.app_env, "etoro_env": settings.etoro_env},
            status_code=503,
        )
    needs_attention = any(
        s in {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING}
        for s in states.values()
    )
    return JSONResponse(
        {
            "status": "ok",
            "system_state": "needs_attention" if needs_attention else "ok",
            "env": settings.app_env,
            "etoro_env": settings.etoro_env,
        },
        status_code=503 if needs_attention else 200,
    )
```

- [ ] **Step 3: Delete legacy from `ops_monitor.py`**

Remove: `LayerName`, `ALL_LAYERS`, `_STALENESS_THRESHOLDS`, `evaluate_staleness`, `LayerStaleness`, `fetch_latest_successful_runs` if only used by it.

Keep: `record_job_start`, `record_job_finish`, `record_job_skip`, `write_kill_switch_audit`.

- [ ] **Step 4: Audit remaining callers**

```
grep -rn "evaluate_staleness\|_STALENESS_THRESHOLDS\|ALL_LAYERS\|LayerName" app tests
```

Every hit should be in a deleted-in-this-PR test file. If an `app/` file still imports these, replace with the layer-state call pattern (`compute_layer_states_from_db` + `LayerState` enum).

- [ ] **Step 5: Tests + full gate**

```
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

- [ ] **Step 6: Commit**

```bash
git add app/main.py app/services/ops_monitor.py tests/test_main_health.py
git commit -m "refactor(#328): retire ops_monitor staleness — /health derives from layer state"
```

### Task 7.3: Close issue #328

- [ ] Push. PR `refactor(#328): chunk 7 — retire ops_monitor staleness`. Poll. Resolve. Merge.
- [ ] Close the umbrella issue:

```bash
gh issue close 328 --comment "All 7 chunks merged. Sub-project A complete. v2 endpoint live, cascade collapse active, ops_monitor staleness retired."
```

---

## Self-review

**Spec coverage**

| Spec section | Covered by |
| --- | --- |
| §2 principles | Chunks 1–7 collectively. |
| §3.1 attempts derivation | Chunk 4 `LayerContext.attempts` reads `all_layer_histories.streaks`. |
| §3.2 state decision flow | Chunk 4 task 4.2 + 4.3 pin every rule. |
| §3.3 upstream-DEGRADED semantics | Pinned in task 4.3.b. |
| §4 registry extension (incl. `content_predicate`) | Chunk 2 tasks 2.1 + 2.2. |
| §5 error taxonomy | Chunk 1 task 1.2 (types + REMEDIES); chunk 3 tasks 3.3–3.4 (classifier + adapter wrap + scheduler wrap). |
| §6 cascade collapse | Chunk 6 task 6.1 (shim in chunk 5 task 5.1 bridging the gap until 6.1 lands). |
| §7 scoped resync | Chunk 6 tasks 6.2 + 6.3; default `behind` pinned by tests. |
| §8 API surface | Chunk 5 v2 endpoint; chunk 7 `/health` derivation. |
| §9 ops_monitor retirement | Chunk 7 tasks 7.1 + 7.2. |
| §10 migration plan | Chunks 1–7 match one-to-one. |
| §11 tests | Each chunk enumerates its own test files. |

**Contract consistency checks** (verified against live code 2026-04-19):

- Adapter signature (`LayerRefresh` protocol, keyword-only) preserved in chunk 3.
- `sync_layer_progress.status` uses `"failed"`; `job_runs.status` uses `"failure"`. Chunk 4 reads `sync_layer_progress`, so `latest_status == "failed"`.
- `record_job_finish(status="failure", error_category=...)` extended; no `record_job_failure`.
- `POST /sync` body extended with `scope`; route path unchanged.
- v1 `/sync/layers` shape pinned in chunk 5 task 5.2 against the actual current fields (including `last_duration_seconds`).
- `/health` lives in `app/main.py:179`; chunk 7 edits that handler.
- Test DB imports use `from tests.fixtures.ebull_test_db import test_database_url`.
- Cascade propagation iterates to fixed point (chunk 4 task 4.4), not two rounds.
- Layer enable/disable persisted in new `layer_enabled` table (chunk 4 task 4.1).
- Scheduler extends existing `_tracked_job`, does not double-wrap.
- `sync_runs.scope` CHECK extended to include `"behind"` (chunk 6 task 6.2 migration).

**Placeholder scan:** no "TBD" / "implement later". Every code block is complete. Forward references are handled by the shim in chunk 5 task 5.1 and explicitly removed in chunk 6 task 6.1.

**TDD integrity:** tasks 4.3.a and 4.3.b are regression-pins (tests green at first run); the commit message says `test(#328): pin ...` to reflect that. Every other failing-test step asserts the precise failure mode the implementation removes.

---

## Contracts to re-verify at execute time

Two internal-wiring areas move faster than a plan doc can track. Subagents implementing these tasks must grep for current shapes before coding:

1. **`RefreshResult` fields** (chunk 3 task 3.3). Plan uses `error_msg` + `error_category`. Current definition lives in `app/services/sync_orchestrator/types.py` around line 55–65. Read the dataclass and adapt the executor's `except LayerRefreshFailed` branch to match the real field set (which may include `row_count`, `items_processed`, `items_total`, `detail`, `error_category`). The behavioural requirement is unchanged: persist the category; pass the detail string through as error message; mark the outcome failed.

2. **Planner + executor wiring** (chunk 6 task 6.2). Plan introduces `ScopeSpec` and list-of-layer planners. Current code uses `SyncScope` + `ExecutionPlan` / `LayerPlan` in `app/services/sync_orchestrator/{planner,executor}.py`, and `submit_sync` consumes them. Extend those existing types (add a `behind` scope variant or a filtered-plan builder) rather than introducing a parallel `ScopeSpec` + `list[str]` planner surface. The external contract (`POST /sync` body `{"scope": "behind"|"full"|"layer:<name>"}` → 202) is the load-bearing part; the internal types should stay consistent with how `build_execution_plan` works today.

3. **`/health` connection dependency** (chunk 7 task 7.2). Plan imports `from app.dependencies import get_conn`. Actual module is `app.db` (`grep -n "def get_conn" app/db.py`). Use whichever one the rest of `app/main.py` already imports.

4. **`_health` test patching path**. Patching `app.services.sync_orchestrator.layer_state.compute_layer_states_from_db` will not take effect if `app.main` imports the function by name — the binding inside `main` is already resolved. Either: (a) patch `app.main.compute_layer_states_from_db` (targeting the already-resolved reference), or (b) import the module (`from app.services.sync_orchestrator import layer_state`) in `main.py` and call `layer_state.compute_layer_states_from_db(conn)`, then patch the module attribute. Option (b) is clearer and matches the codebase's pattern for patch-friendly imports.

Every other contract in this plan was verified against live code on 2026-04-19.

---

## Execution

Plan saved. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks.
2. **Inline Execution** — execute tasks in this session via `superpowers:executing-plans`.
