# Data orchestrator and system observability

**Date**: 2026-04-16
**Status**: Draft — pending Codex adversarial review

## Problem

eBull has 19 independently scheduled cron jobs. The operator must understand the ETL pipeline to know what to run, in what order, and why. The jobs page shows function names with brief notes — no live status, no progress, no duration, no error drill-down. Nothing feels live. Errors sit unmonitored.

This is backwards. The system should manage itself. Data should stay fresh according to its natural cadence. The operator should see a health dashboard, not a job scheduler. One "sync" button should check everything and update only what's stale, in the right order, as lightly as possible.

### Specific pain points

1. **19 jobs with implicit ordering.** No dependency awareness. Operator must mentally reconstruct the DAG.
2. **No live refresh.** Page loads once. Running jobs look the same as finished jobs.
3. **No progress indicators.** No sense of "what's happening right now" or "how far along."
4. **No duration tracking.** `started_at` and `finished_at` exist in `job_runs` but duration is never displayed.
5. **No error visibility.** Errors are truncated strings in a table. No drill-down, no failure streaks, no actionable detail.
6. **No catch-up intelligence.** After a week off, no way to know what's stale or trigger a smart refresh.
7. **Prices feel dead.** Hourly REST polling for quotes. No sub-minute updates. FX treated as a separate concern when it's just another instrument price.

### Relationship to existing specs

The [live pricing architecture spec](2026-04-13-live-pricing-architecture-design.md) (approved, 7 Codex rounds) covers:
- WebSocket connection for real-time prices
- Redis IPC for SSE fanout
- Currency conversion
- TradingView Lightweight Charts
- Market hours awareness

This spec covers the **remaining two pillars**: the data orchestrator (replacing cron jobs with a staleness-driven DAG) and the observability UI (replacing the jobs page with a system health dashboard). Together with the live pricing spec, these form the complete "self-managing, always-current" system.

---

## 1. Data layer model

Instead of 19 jobs, the system thinks in **data layers**. Each layer has a freshness contract, dependencies, and a refresh action.

### 1.1 Layer definitions

| Layer | Freshness target | Dependencies | Refresh cost | Watermark type |
|---|---|---|---|---|
| `universe` | Daily | None | 1 API call (full list) | `last_seen_at` on instruments |
| `cik_mapping` | Daily | `universe` | 1 API call (full mapping) | `last_verified_at` on external_identifiers |
| `candles` | Daily (after market close) | `universe` | Per-instrument, skip if today's candle exists | `MAX(price_date)` per instrument |
| `financial_facts` | Daily | `cik_mapping` | 1 API call per CIK | `data_ingestion_runs` audit |
| `financial_normalization` | After `financial_facts` | `financial_facts` | Computed, no API | Latest `ingestion_run_id` vs last normalized |
| `fundamentals` | Quarterly | `universe` | 100-300 API calls/day | `as_of_date >= quarter_start` |
| `news` | Every 4 hours | `universe` | Provider-dependent | 72h dedup window |
| `thesis` | Per review_frequency | `fundamentals`, `financial_normalization`, `news` | ~2 Claude calls per stale instrument | `created_at + review_frequency > now` |
| `scoring` | After upstream changes | `thesis`, `candles` | Deterministic formulas, no API | Latest `score_run_id` vs upstream timestamps |
| `recommendations` | After `scoring` | `scoring` | Portfolio review logic | Latest recommendation timestamp |
| `portfolio_sync` | Every 5 minutes | None (independent) | 1 API call (full reconciliation) | Broker state always authoritative |
| `fx_rates` | Every 5 minutes | None (independent) | 1 API call (Frankfurter) | `quoted_at` per pair |
| `cost_models` | Daily | `universe` | Computed from trade history | Latest `created_at` on cost_models |
| `reports` | Weekly/monthly | All upstream | Computed | Report schedule |

### 1.2 Dependency DAG

```
universe ─┬─→ cik_mapping ─→ financial_facts ─→ financial_normalization ─┐
          ├─→ candles ─→ (TA indicators computed inline)                 │
          ├─→ fundamentals                                               │
          ├─→ cost_models                                                │
          └─→ news ──────────────────────────────────────────────────────┤
                                                                         │
          ┌──────────────────────────────────────────────────────────────┘
          ↓
        thesis ─→ scoring ─→ recommendations

portfolio_sync (independent, high-frequency)
fx_rates (independent, high-frequency)
reports (independent, periodic)
```

### 1.3 Freshness predicates

Each layer has a `is_fresh(conn) -> (fresh: bool, detail: str)` predicate. Most already exist in the codebase:

- **`universe`**: `MAX(last_seen_at) >= today` on instruments where `is_tradable = TRUE`
- **`cik_mapping`**: `MAX(last_verified_at) >= today` on external_identifiers where `provider = 'sec'`
- **`candles`**: Count of T1/T2 instruments where `MAX(price_date) < most_recent_trading_day(today)`. Fresh if count = 0.
- **`financial_facts`**: Latest `data_ingestion_runs` row for `source = 'sec_xbrl'` has `started_at >= today`
- **`financial_normalization`**: Latest normalization run is newer than latest facts ingestion run
- **`fundamentals`**: Count of instruments where `fundamentals_snapshot.as_of_date < current_quarter_start`. Fresh if count = 0.
- **`news`**: Latest news ingestion run `started_at >= now - 4 hours`
- **`thesis`**: Count of stale instruments (per `find_stale_instruments()`). Fresh if count = 0.
- **`scoring`**: Latest `score_run` is newer than latest thesis + latest candle
- **`recommendations`**: Latest recommendation timestamp is newer than latest score run
- **`portfolio_sync`**: Latest sync `finished_at >= now - 5 minutes`
- **`fx_rates`**: `MAX(quoted_at) >= now - 5 minutes` on live_fx_rates
- **`cost_models`**: Latest cost model `created_at >= today`
- **`reports`**: On schedule (weekly: last report within 7 days, monthly: within 31 days)

The `detail` string is human-readable: "3 instruments missing today's candle", "thesis stale for 5 instruments", "last sync 47 minutes ago".

---

## 2. Orchestrator

### 2.1 Sync entry point

Single function: `run_sync(conn, scope: SyncScope) -> SyncResult`

`SyncScope` options:
- `FULL` — walk entire DAG, refresh everything stale (default for "sync" button and catch-up)
- `LAYER(name)` — refresh a specific layer and its dependencies (power-user override)
- `HIGH_FREQUENCY` — only independent high-frequency layers: portfolio_sync, fx_rates (for the periodic timer)

### 2.2 Execution algorithm

```python
def run_sync(conn, scope=SyncScope.FULL) -> SyncResult:
    plan = build_execution_plan(conn, scope)  # check freshness, resolve DAG
    
    for layer in plan.layers_to_refresh:  # topological order
        emit_progress(layer, "starting")
        try:
            result = layer.refresh(conn)
            emit_progress(layer, "complete", result)
        except Exception as e:
            emit_progress(layer, "failed", error=e)
            if layer.is_blocking:  # dependents cannot proceed
                mark_dependents_skipped(plan, layer, reason=str(e))
            # non-blocking layers: log and continue
    
    return SyncResult(plan)
```

### 2.3 Execution plan

`build_execution_plan()` produces a plan object:

```python
@dataclass
class ExecutionPlan:
    layers_to_refresh: list[LayerPlan]   # topological order, only stale layers
    layers_skipped: list[LayerSkip]      # already fresh, with detail
    estimated_duration: timedelta | None  # based on historical avg
```

```python
@dataclass
class LayerPlan:
    name: str
    reason: str           # "3 instruments missing today's candle"
    dependencies: list[str]
    is_blocking: bool     # if True, failure skips dependents
    estimated_items: int  # e.g. number of instruments to process
```

This plan is persisted to `sync_runs` table (see schema below) and displayed in the UI before and during execution.

### 2.4 Progress tracking

New table for mid-run progress:

```sql
CREATE TABLE sync_layer_progress (
    sync_run_id    BIGINT NOT NULL REFERENCES sync_runs(sync_run_id),
    layer_name     TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'complete', 'failed', 'skipped')),
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    items_total    INTEGER,        -- e.g. 200 instruments
    items_done     INTEGER,        -- e.g. 150 instruments processed
    row_count      INTEGER,        -- rows affected
    error_msg      TEXT,
    skip_reason    TEXT,           -- why skipped (fresh, or dependency failed)
    PRIMARY KEY (sync_run_id, layer_name)
);
```

Jobs update `items_done` periodically during execution (every N items or every 10 seconds, whichever comes first). This is the source for progress bars in the UI.

### 2.5 Sync runs table

```sql
CREATE TABLE sync_runs (
    sync_run_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope          TEXT NOT NULL CHECK (scope IN ('full', 'layer', 'high_frequency')),
    scope_detail   TEXT,           -- layer name if scope='layer'
    trigger        TEXT NOT NULL CHECK (trigger IN ('manual', 'scheduled', 'catch_up')),
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         TEXT NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running', 'complete', 'partial', 'failed')),
    layers_planned INTEGER NOT NULL,
    layers_done    INTEGER NOT NULL DEFAULT 0,
    layers_failed  INTEGER NOT NULL DEFAULT 0,
    layers_skipped INTEGER NOT NULL DEFAULT 0
);
```

`status = 'partial'` means some layers succeeded, some failed. Not a full failure, not fully complete.

### 2.6 Scheduling

The orchestrator replaces most cron jobs with two scheduled triggers:

| Trigger | Schedule | Scope |
|---|---|---|
| **Full sync** | Daily at 03:00 UTC | `FULL` — walks entire DAG |
| **High-frequency sync** | Every 5 minutes | `HIGH_FREQUENCY` — portfolio_sync + fx_rates only |

Plus:
- **Catch-up on boot** — same as current behavior, but uses orchestrator instead of individual job catch-up
- **Manual "Sync Now" button** — triggers `FULL` scope

The 19 individual cron schedules are removed. The existing job functions remain as the `refresh()` action inside each layer — code reuse, not rewrite.

### 2.7 Backward compatibility with job_runs

The orchestrator writes to `job_runs` as before (each layer refresh creates a `job_runs` row via the existing `record_job_start`/`record_job_finish` pattern). This preserves historical data and existing monitoring. The `sync_layer_progress` table adds the orchestrator-level view on top.

---

## 3. Observability UI

### 3.1 Mental model shift

**Before:** "Here are 19 jobs. Figure out which ones matter."
**After:** "Here's how fresh your data is. Everything green = you're current."

### 3.2 System health dashboard (replaces jobs page)

Three sections, top to bottom:

#### Section 1: System status banner

Single line at the top:
- **"All data current"** (green) — every layer fresh
- **"3 layers stale — sync recommended"** (amber) — some layers past freshness target
- **"Sync in progress — 4/12 layers complete"** (blue) — active sync running
- **"Sync failed — 2 errors"** (red) — last sync had failures

With a **"Sync Now"** button (right-aligned). Disabled with spinner while sync is running.

#### Section 2: Data layer grid

Each layer is a card/row showing:

| Field | Source |
|---|---|
| **Layer name** (human-readable) | Layer definition |
| **Status icon** | Green check / amber clock / red X / blue spinner |
| **Freshness detail** | From `is_fresh()` detail string |
| **Last updated** | `finished_at` from latest successful layer run |
| **Duration** | `finished_at - started_at` from latest run |
| **Progress bar** | `items_done / items_total` (only while running) |
| **Error** (expandable) | `error_msg` from latest failed run |

Layers grouped visually by dependency tier:
- **Tier 0 (sources):** Universe, CIK, Portfolio Sync, FX Rates
- **Tier 1 (raw data):** Candles, Financial Facts, Fundamentals, News
- **Tier 2 (computed):** Normalization, Thesis, Cost Models
- **Tier 3 (decisions):** Scoring, Recommendations, Reports

#### Section 3: Recent activity log

Chronological list of recent layer runs (newest first). Replaces the "Recent Runs" table. Shows:
- Layer name, status, started/finished, duration, row count, error (expandable)
- Filterable by: status (failed/success/skipped), layer name
- Last 100 entries by default

### 3.3 Auto-refresh

The dashboard polls every 10 seconds while a sync is running (detected via `sync_runs` with `status = 'running'`). Drops to every 60 seconds when idle. No manual refresh needed.

Implementation: React Query with dynamic `refetchInterval`:

```typescript
const { data: syncStatus } = useQuery({
  queryKey: ["sync-status"],
  queryFn: fetchSyncStatus,
  refetchInterval: (query) =>
    query.state.data?.is_running ? 10_000 : 60_000,
});
```

### 3.4 Error visibility

Errors are first-class, not hidden in truncated table cells:

- **Inline expansion:** Click error row to see full error message, stack trace (if available), and timestamp.
- **Failure streak indicator:** If a layer has failed N consecutive times, show "failed 3x in a row" badge.
- **Copy button:** One-click copy of error detail for reporting/pasting into an issue.

Error messages are sanitised server-side (no SQL fragments, no driver internals — existing prevention-log rule #70).

### 3.5 Elapsed duration for running layers

For layers currently in `running` status, the UI shows a live elapsed timer computed client-side from `started_at`:

```typescript
function ElapsedTimer({ startedAt }: { startedAt: string }) {
  const [elapsed, setElapsed] = useState("");
  useEffect(() => {
    const interval = setInterval(() => {
      const seconds = Math.floor((Date.now() - new Date(startedAt).getTime()) / 1000);
      setElapsed(formatDuration(seconds));
    }, 1000);
    return () => clearInterval(interval);
  }, [startedAt]);
  return <span className="elapsed">{elapsed}</span>;
}
```

No server-side timer — just `started_at` + client clock.

---

## 4. API changes

### 4.1 New endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `POST /sync` | POST | Trigger a sync. Body: `{"scope": "full"}` or `{"scope": "layer", "layer": "candles"}`. Returns 202 + `sync_run_id`. |
| `GET /sync/status` | GET | Current sync state: running sync (if any) + layer-by-layer progress. |
| `GET /sync/runs` | GET | Recent sync runs with layer results. Query params: `limit` (default 20). |
| `GET /sync/layers` | GET | All layers with current freshness status. No active sync required. |

### 4.2 Deprecated endpoints (kept for backward compatibility)

| Endpoint | Notes |
|---|---|
| `POST /jobs/{job_name}/run` | Still works — maps job name to layer(s) via a `JOB_TO_LAYERS` lookup, then runs those layers in order via orchestrator `LAYER` scope. Composite jobs like `morning_candidate_review` map to `["scoring", "recommendations"]`. Returns 202. |
| `GET /jobs/runs` | Still works — reads from `job_runs` table as before. |
| `GET /system/jobs` | Replaced by `GET /sync/layers` but kept until frontend migration complete. |

### 4.3 Response shapes

**`GET /sync/status`:**

```json
{
  "is_running": true,
  "current_run": {
    "sync_run_id": 42,
    "scope": "full",
    "trigger": "manual",
    "started_at": "2026-04-16T03:00:00Z",
    "layers_planned": 12,
    "layers_done": 4,
    "layers_failed": 0,
    "layers_skipped": 2
  },
  "active_layer": {
    "name": "financial_facts",
    "started_at": "2026-04-16T03:12:00Z",
    "items_total": 200,
    "items_done": 87
  }
}
```

**`GET /sync/layers`:**

```json
{
  "layers": [
    {
      "name": "universe",
      "display_name": "Tradable Universe",
      "tier": 0,
      "is_fresh": true,
      "freshness_detail": "Last synced today at 03:01 UTC",
      "last_success_at": "2026-04-16T03:01:30Z",
      "last_duration_seconds": 12,
      "last_error": null,
      "consecutive_failures": 0,
      "dependencies": []
    },
    {
      "name": "candles",
      "display_name": "Daily Price Candles",
      "tier": 1,
      "is_fresh": false,
      "freshness_detail": "3 instruments missing today's candle",
      "last_success_at": "2026-04-15T22:05:00Z",
      "last_duration_seconds": 340,
      "last_error": null,
      "consecutive_failures": 0,
      "dependencies": ["universe"]
    }
  ]
}
```

---

## 5. Schema changes

### 5.1 New tables

```sql
-- Sync run envelope
CREATE TABLE sync_runs (
    sync_run_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope          TEXT NOT NULL CHECK (scope IN ('full', 'layer', 'high_frequency')),
    scope_detail   TEXT,
    trigger        TEXT NOT NULL CHECK (trigger IN ('manual', 'scheduled', 'catch_up')),
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         TEXT NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running', 'complete', 'partial', 'failed')),
    layers_planned INTEGER NOT NULL,
    layers_done    INTEGER NOT NULL DEFAULT 0,
    layers_failed  INTEGER NOT NULL DEFAULT 0,
    layers_skipped INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_sync_runs_started ON sync_runs(started_at DESC);

-- Per-layer progress within a sync run
CREATE TABLE sync_layer_progress (
    sync_run_id    BIGINT NOT NULL REFERENCES sync_runs(sync_run_id),
    layer_name     TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'complete', 'failed', 'skipped')),
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    items_total    INTEGER,
    items_done     INTEGER,
    row_count      INTEGER,
    error_msg      TEXT,
    skip_reason    TEXT,
    PRIMARY KEY (sync_run_id, layer_name)
);
```

### 5.2 Unchanged tables

- `job_runs` — still written to by each layer refresh (backward compatibility). Not removed.
- All existing data tables — untouched. Orchestrator calls existing refresh functions.

---

## 6. Layer registry (Python)

```python
@dataclass(frozen=True)
class DataLayer:
    name: str
    display_name: str
    tier: int                                    # 0=source, 1=raw, 2=computed, 3=decision
    is_fresh: Callable[[Connection], tuple[bool, str]]  # (fresh, detail)
    refresh: Callable[[Connection, ProgressCallback], RefreshResult]
    dependencies: tuple[str, ...]                # layer names
    is_blocking: bool = True                     # if True, failure skips dependents
    cadence: str = "daily"                       # for display only

LAYERS: dict[str, DataLayer] = {
    "universe": DataLayer(
        name="universe",
        display_name="Tradable Universe",
        tier=0,
        is_fresh=universe_is_fresh,
        refresh=refresh_universe,
        dependencies=(),
    ),
    "cik_mapping": DataLayer(
        name="cik_mapping",
        display_name="SEC CIK Mapping",
        tier=0,
        is_fresh=cik_is_fresh,
        refresh=refresh_cik,
        dependencies=("universe",),
    ),
    # ... all 14 layers
}
```

Each `refresh()` function wraps the existing job logic. No rewrite — just a thin adapter that:
1. Calls the existing job function
2. Periodically updates `items_done` via the `ProgressCallback`
3. Returns `RefreshResult(row_count, error_msg)`

### 6.1 Progress callback

```python
class ProgressCallback(Protocol):
    def __call__(self, items_done: int, items_total: int | None = None) -> None: ...
```

Existing job functions gain an optional `progress: ProgressCallback | None = None` parameter. When provided, they call `progress(i, total)` periodically. When `None` (backward compat), they skip progress updates.

This is the only change to existing job functions — adding an optional callback parameter.

---

## 7. Catch-up behavior

### 7.1 First run (empty database)

Orchestrator runs `FULL` scope. Every layer is stale. Runs in dependency order top-down. Progress visible per-layer. Expected duration: 15-30 minutes depending on universe size and API rate limits.

### 7.2 After extended downtime (1-2 weeks)

Same as first run — `FULL` scope, everything stale, full DAG traversal. But most data is idempotent on re-fetch:
- Universe: ON CONFLICT, only writes changes
- Candles: fetches 400-day lookback, fills gaps
- Financial facts: full re-fetch per CIK, ON CONFLICT handles restatements
- Thesis: stale instruments get new theses (Claude calls)

Duration depends on how many theses need regeneration (most expensive layer).

### 7.3 Normal daily operation

Full sync at 03:00 UTC. Most layers fresh from previous day. Only stale layers run. Typical: universe + candles + financial_facts + news + a few stale theses. Duration: 5-10 minutes.

### 7.4 High-frequency loop

Every 5 minutes: portfolio_sync + fx_rates only. These are independent layers with no DAG dependencies. Duration: <30 seconds.

---

## 8. Migration path

### 8.1 Phase 1: Orchestrator core

- Define `LAYERS` registry with freshness predicates
- Build `run_sync()` with DAG resolution
- Add `sync_runs` + `sync_layer_progress` tables
- Add `POST /sync` + `GET /sync/status` + `GET /sync/layers` endpoints
- Existing jobs still work — orchestrator calls them internally

### 8.2 Phase 2: Progress tracking

- Add optional `progress` callback to existing job functions
- Instrument the most important layers first: candles, financial_facts, thesis (longest-running)
- Other layers get progress tracking over time

### 8.3 Phase 3: Observability UI

- Replace AdminPage jobs section with system health dashboard
- Layer grid, sync status banner, activity log
- Auto-refresh with dynamic interval
- Error expansion and copy

### 8.4 Phase 4: Remove cron jobs

- Replace APScheduler cron triggers with orchestrator triggers (daily full + 5-min high-frequency)
- Remove individual job schedules from `SCHEDULED_JOBS`
- Keep `_INVOKERS` as the layer refresh functions
- Remove jobs management UI (replaced by health dashboard)

---

## 9. Non-goals (explicit scope boundaries)

- **No real-time price streaming.** Covered by [live pricing spec](2026-04-13-live-pricing-architecture-design.md).
- **No TradingView charts.** Covered by live pricing spec.
- **No currency conversion.** Covered by live pricing spec.
- **No new data sources.** Orchestrator manages existing sources with existing fetch logic.
- **No parallel layer execution.** Layers run sequentially in topological order. Parallelism is a future optimization.
- **No job-level configuration UI.** Operator sees layers and health, not cron expressions or job parameters.
- **No alerting/notifications.** Error visibility is in-dashboard only. Email/Slack alerting is a future concern.
- **No historical analytics.** No "average duration trend" or "failure rate over time" charts. Just current state and recent runs.

---

## 10. Relationship to other specs

| Spec | Relationship |
|---|---|
| [Live pricing architecture](2026-04-13-live-pricing-architecture-design.md) | Complementary. Pricing spec handles real-time WebSocket + charts. This spec handles batch data freshness + observability. Together they make the system self-managing. |
| [Financial data enrichment](2026-04-16-financial-data-enrichment-design.md) | `financial_facts` and `financial_normalization` layers wrap the Phase 1 pipeline. Orchestrator calls these as layer refresh functions. |
| [Portfolio and frontend redesign](2026-04-14-portfolio-and-frontend-redesign.md) | Observability dashboard is a new page, not a modification of the portfolio redesign. No conflict. |

---

## 11. Open questions for Codex review

1. Should high-frequency layers (portfolio_sync, fx_rates) use the same `sync_runs` tracking or a lighter-weight path? They run every 5 minutes — that's a lot of `sync_runs` rows.
2. Is `is_blocking = True` the right default? If `universe` fails, should we skip all downstream layers or attempt them with stale data?
3. Should the orchestrator support retry within a sync run (e.g. retry a failed layer once before marking it failed)?
4. The `progress` callback approach requires modifying existing job function signatures. Is there a less invasive way to track progress?
