# Freshness model unification — design

**Date:** 2026-04-19
**Scope:** Sub-project **A** of the Admin / data-freshness overhaul. This spec covers only freshness-model and autonomy changes. Sub-projects B (layer metrics history), C (Admin UI redesign), D (cleanup visibility), and E (delta-pull audit) are documented separately.

## 1. Problem

Two parallel staleness systems disagree, so the Admin page renders "11 problems" while the scheduler logs `catch-up: all jobs are current; nothing to fire`.

| System | File | Callers | Signal |
| --- | --- | --- | --- |
| `ops_monitor` | `app/services/ops_monitor.py` | kill switch, `/health`, row-count spike detection | Table watermarks (`MAX(price_date)` etc.) vs per-layer flat thresholds in `_STALENESS_THRESHOLDS`. |
| `sync_orchestrator.freshness` | `app/services/sync_orchestrator/freshness.py` | `/sync/layers`, `ProblemsPanel` on Admin page | Latest `job_runs` row + optional content predicate, flat window per layer. |

Failure modes caused by the split:

1. **Flat windows ignore cadence.** `fx_rates` and `portfolio_sync` declare `cadence="5m"` but their freshness predicate uses `timedelta(minutes=5)` with no grace — any transient skip flips them red even though the next fire is ~30s away.
2. **`_catch_up` and `_fresh_by_audit` disagree on the same job.** `_catch_up` computes `compute_next_run(last_success)` using the declared cron schedule; if the next fire is an hour away, catch-up is silent. `_fresh_by_audit` applies a flat 24h window; any layer whose last success was 24h+1s ago is marked stale, even though the scheduler is waiting by design. Operators see red when nothing is wrong.
3. **`prereq_missing:` skips are counted fresh by audit (correct per spec §1.3) but downstream content predicates still fail, producing a confusing "fresh audit, stale content" double-message.** The operator has no single answer to "is this OK?".
4. **Cascade failures flood the panel.** A single `daily_cik_refresh` failure produces five red rows (CIK + its four blocked downstream layers). Operator fixes one thing; five alarms were for one problem.
5. **Operator has nine buttons that do the same thing.** "Run now" per job, "Sync now" global, "Retry" buried in collapsibles. No guidance on which to press. No automatic scoped retry.

Non-goals:

- This spec **does not** redesign the Admin UI. That is sub-project C and depends on this work landing first.
- This spec **does not** introduce a new time-series store for layer history. That is sub-project B.
- This spec **does not** audit per-provider delta-pull discipline. That is sub-project E.

## 2. Design principles (from patterns in Airbyte, dbt Cloud, Dagster, Sentry)

1. **SLA-based, not flat-window.** A layer is fresh when `now - last_success < cadence × grace_multiplier`. A 5-minute cadence stops being "stale at 5:01" and becomes "stale at 6:15".
2. **One source of truth.** Orchestrator `LayerState` is the only place layer health is computed. `ops_monitor` becomes a thin adapter.
3. **Cascade collapse.** If an upstream layer is in `action_needed`, its downstream layers report `cascade_waiting` with a pointer to the root cause. Operator sees one problem, not five.
4. **Error taxonomy.** Every failure maps to one of a small set of categories. Each category has a plain-language remedy attached.
5. **Scoped autonomy.** System self-retries, self-skips, self-escalates. Operator control surface reduces to three levers: enable/disable per layer, fill in a missing secret, or trip the global kill switch.

## 3. Layer state model

Introduce a single `LayerState` enum that replaces the scattered boolean pairs (`is_fresh` × `is_blocking` × `consecutive_failures > 0`).

```python
class LayerState(StrEnum):
    HEALTHY = "healthy"              # fresh, latest run counted as success, no retries pending
    RUNNING = "running"              # fire in flight
    RETRYING = "retrying"             # latest run failed (self-heal category), retry budget still available
    DEGRADED = "degraded"             # no failure on the latest run, but age > grace — scheduler will catch up
    ACTION_NEEDED = "action_needed"   # local failure cannot self-heal; operator must act
    SECRET_MISSING = "secret_missing" # prerequisite (API key) not configured
    CASCADE_WAITING = "cascade_waiting"  # this layer has no local failure; an upstream is blocked
    DISABLED = "disabled"             # operator has turned this layer off
```

### 3.1 Attempt counter (derived, not stored)

`attempts` is not a new column. It is derived per layer on demand from `job_runs` via the existing helper at `app/services/sync_orchestrator/layer_failure_history.py` and is equivalent to `consecutive_failures`:

- Walk `job_runs` for the layer's backing job(s) in reverse chronological order.
- Count contiguous `status='failure'` rows. The first non-failure counting row terminates the count.
- Counting-row definition (unchanged from spec §1.3): `status='success'` OR (`status='skipped'` AND `error_msg LIKE 'prereq_missing:%'`). `partial` counts as a reset on the failure streak for freshness purposes (it advanced some items) but is surfaced separately in the UI.
- `SECRET_MISSING` skips and `DEP_SKIPPED` skips do **not** count as failure attempts — they reset the counter. The retry budget is for failures the layer itself produced, not upstream-caused or config-caused skips.

### 3.2 State decision flow

Evaluated top-down per layer, first match wins. Order is load-bearing; each precedence rule is justified in the comment column.

```text
1. layer.is_enabled is False                                     -> DISABLED
2. a run is in flight (sync_layer_progress row status='running') -> RUNNING
3. secret_refs have unmet prereqs                                -> SECRET_MISSING
   # Secrets checked before local-failure branching: a layer missing a key should never show a
   # stale failure from a previous (keyed) run. Operator sees the config fix, not stale rubble.
4. latest_run.status == 'failure' AND remedy(category).self_heal is False                  -> ACTION_NEEDED
   # Non-self-heal categories (AUTH_EXPIRED, SCHEMA_DRIFT, DB_CONSTRAINT) escalate on the first
   # failure. Retrying a bad API key or a broken schema wastes calls and never clears.
5. latest_run.status == 'failure' AND attempts >= retry_policy.max_attempts                 -> ACTION_NEEDED
   # Self-heal categories escalate after exhausting the retry budget.
6. latest_run.status == 'failure' AND attempts < retry_policy.max_attempts                  -> RETRYING
   # Self-heal category, budget remains, orchestrator will re-fire with backoff.
7. any upstream layer ∈ {ACTION_NEEDED, SECRET_MISSING}                                    -> CASCADE_WAITING
   # Order matters: local-failure checks (4–6) come BEFORE cascade so a downstream with its
   # own terminal failure surfaces as its own root cause, not as a waiter on an unrelated upstream.
8. content_predicate(conn) returns (False, ...)                                              -> DEGRADED
   # Audit-fresh but content-stale (e.g. candles job succeeded but a tradable instrument is
   # missing today's row). Orchestrator will re-queue a scoped catch-up.
9. age > cadence.grace_window                                                                -> DEGRADED
   # Audit-stale, no failure on record; scheduler will fire at the next cadence tick.
10. otherwise                                                                                -> HEALTHY
```

Two invariants follow from the order:

- `HEALTHY` ⇒ latest counting row is success, age ≤ grace, content predicate ok, no upstream problem, no pending retry, no missing secret. No loophole for "latest run failed but age is young".
- `CASCADE_WAITING` ⇒ local state is clean (no failure, secret present, content ok); the layer is only waiting because an upstream is blocked. This cleanly separates "waiter" from "independent failure".

### 3.3 Upstream-DEGRADED semantics

An upstream in `DEGRADED`, `RUNNING`, or `RETRYING` does **not** propagate as `CASCADE_WAITING`. Rationale: these are self-healing states, the orchestrator is actively catching up, and the downstream is free to run against the last-known-good upstream data. If running against stale upstream produces a local failure, the downstream's own state machine handles it via (4)–(6). Only terminal-blocked states (`ACTION_NEEDED`, `SECRET_MISSING`) cascade; those are the states the operator must unblock before any downstream can make progress.

## 4. Layer registry extension

Extend `DataLayer` in `app/services/sync_orchestrator/registry.py`:

```python
@dataclass(frozen=True)
class DataLayer:
    name: str
    display_name: str
    tier: int
    cadence: Cadence                  # typed, not string
    grace_multiplier: float = 1.25    # default tolerance
    is_blocking: bool = True
    dependencies: tuple[str, ...] = ()
    content_predicate: ContentPredicate | None = None
    retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY
    secret_refs: tuple[SecretRef, ...] = ()
    plain_language_sla: str = ""      # e.g. "Updated every trading day by 10:00 UTC"
```

`Cadence` becomes a typed value:

```python
@dataclass(frozen=True)
class Cadence:
    interval: timedelta     # window length
    schedule: CronSpec | None = None  # None = interval-driven (e.g. 5m), else cron
```

Derived properties:

- `cadence.grace_window = cadence.interval × grace_multiplier`
- `cadence.next_expected_fire(last_success)` returns the deterministic next fire (delegates to `compute_next_run` today).

`content_predicate` replaces the embedded watermark checks inside the current `is_fresh` functions. Pure read, returns `(ok: bool, detail: str)`. Moves the "per-instrument candle missing" and "fundamentals snapshot missing" logic out of the freshness module and into per-layer functions that can be reused by `ops_monitor` row-count checks.

`secret_refs` declares which environment secrets the refresh function needs. Evaluated on the planning connection before every fire. If a ref is missing, the layer moves to `SECRET_MISSING`. Replaces today's `prereq_missing:` skip string parsing.

`retry_policy` is inlined in the registry so a reader sees the backoff schedule next to the layer definition:

```python
@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: tuple[int, ...] = (60, 600, 3600)  # 1m, 10m, 1h
```

`DEFAULT_RETRY_POLICY` is sensible for daily jobs. Minute-cadence layers (`fx_rates`, `portfolio_sync`) get `RetryPolicy(max_attempts=5, backoff_seconds=(30, 60, 120, 300, 600))`.

## 5. Error taxonomy

Replace free-form error strings with a `FailureCategory` enum persisted alongside `job_runs.error_msg`:

```python
class FailureCategory(StrEnum):
    AUTH_EXPIRED = "auth_expired"       # 401/403, bad or expired API key
    RATE_LIMITED = "rate_limited"       # 429, quota exceeded
    SOURCE_DOWN = "source_down"          # 5xx, timeout, DNS failure
    SCHEMA_DRIFT = "schema_drift"        # unexpected payload shape; needs code change
    DB_CONSTRAINT = "db_constraint"      # unique/foreign key violations; recovery maybe possible
    DATA_GAP = "data_gap"                # upstream returned empty / zero rows
    UPSTREAM_WAITING = "upstream_waiting"  # dependency layer not healthy yet
    INTERNAL_ERROR = "internal_error"    # fallback; stack trace unclassified
```

Each category carries a remedy record held in a single Python dict (not DB-stored — these are engineering-owned mappings):

```python
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
    # ...
}
```

`self_heal=True` categories produce `RETRYING` until `retry_policy.max_attempts` is exhausted, then `ACTION_NEEDED`. `self_heal=False` categories (e.g. missing credential, schema drift, DB constraint violation) escalate to `ACTION_NEEDED` on the first failure — retrying a bad API key wastes calls and fills logs. This precedence is encoded in §3.2 rules 4 and 5.

Default self-heal table:

| Category | `self_heal` | Rationale |
| --- | --- | --- |
| `AUTH_EXPIRED` | false | Credentials don't fix themselves; only a secret update clears this. |
| `RATE_LIMITED` | true | Backoff is the fix. |
| `SOURCE_DOWN` | true | Provider usually recovers. |
| `SCHEMA_DRIFT` | false | Requires code change. |
| `DB_CONSTRAINT` | false | Usually indicates a data-model bug that needs investigation; retrying masks it. |
| `DATA_GAP` | true | Next upstream fetch may populate. |
| `UPSTREAM_WAITING` | true | Not really a failure — retriable once upstream clears. |
| `INTERNAL_ERROR` | true | Conservative default; engineer-triaged categorisation later promotes these to their true class. |

Categorisation happens at adapter boundaries (`app/services/sync_orchestrator/adapters.py`). Each adapter catches exceptions and raises `LayerRefreshFailed(category=..., detail=...)`. The executor persists both to `job_runs`.

## 6. Cascade collapse

Algorithm (pure function over the layer registry + current `LayerState` map):

```python
def collapse_cascades(states: dict[str, LayerState]) -> list[ProblemGroup]:
    """Group CASCADE_WAITING layers under their root cause."""
    roots = [
        n for n, s in states.items()
        if s in {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING}
    ]
    groups = []
    for root in roots:
        descendants = transitive_downstream(root)
        waiting = [
            d for d in descendants
            if states[d] == LayerState.CASCADE_WAITING
        ]
        groups.append(ProblemGroup(root=root, affected=waiting))
    return groups
```

A failed `cik_mapping` therefore produces one `ProblemGroup`, not five problem rows. The API emits the group; the UI renders one row with an "expand to see 4 affected layers" affordance.

Independent failures (two roots with disjoint descendant sets) produce two groups. Two roots with overlapping descendants produce two groups, each listing the overlap; the UI can dedupe visually if it prefers.

## 7. Scoped resync

`POST /sync/run` gains a `scope` query parameter:

| `scope` value | Behaviour |
| --- | --- |
| `behind` (default) | Fire only layers in `DEGRADED` or `ACTION_NEEDED`, plus their upstreams if those upstreams are not HEALTHY. Orchestrator already supports DAG-level gating; this just passes a filter. |
| `full` | Fire all non-`DISABLED` layers regardless of current state. Matches today's "Sync now" behaviour. |
| `layer:<name>` | Fire one specific layer and nothing else. Engineer debugging path. |

The UI's primary button issues `scope=behind`. The "Force full sync" action (buried under a secondary menu) issues `scope=full`. Per-layer "Run now" issues `scope=layer:<name>`. Three entry points, one endpoint, clear intent.

## 8. API surface

A new endpoint `GET /sync/layers/v2` returns the structured payload below. The existing `GET /sync/layers` flat payload is not changed by this spec; it remains the source for the current Admin page until sub-project C replaces that page.

```json
{
  "generated_at": "2026-04-19T12:00:00Z",
  "system_state": "needs_attention",
  "system_summary": "1 layer needs attention (SEC CIK mapping)",
  "action_needed": [
    {
      "root_layer": "cik_mapping",
      "display_name": "SEC CIK Mapping",
      "category": "db_constraint",
      "operator_message": "SEC filing conflict detected — investigate duplicate CIK.",
      "operator_fix": "Review duplicate rows in external_identifiers. [Open details]",
      "self_heal": false,
      "consecutive_failures": 3,
      "affected_downstream": ["financial_facts", "financial_normalization", "thesis", "scoring", "recommendations"]
    }
  ],
  "degraded": [],
  "secret_missing": [
    {
      "layer": "news",
      "display_name": "News & Sentiment",
      "missing_secret": "ANTHROPIC_API_KEY",
      "operator_fix": "Set ANTHROPIC_API_KEY in Settings → Providers"
    }
  ],
  "healthy": [
    {"layer": "portfolio_sync", "last_updated": "2026-04-19T11:59:30Z"},
    // ...
  ],
  "disabled": []
}
```

`system_state` ∈ `{ok, catching_up, needs_attention}`:

- `ok` — every enabled layer HEALTHY.
- `catching_up` — one or more DEGRADED/RUNNING, zero in `ACTION_NEEDED` or `SECRET_MISSING`.
- `needs_attention` — any layer in `ACTION_NEEDED` or `SECRET_MISSING`.

`GET /system/health` becomes a trivial derivation: `200 OK` when `system_state != needs_attention`, else `503` with the same payload.

## 9. `ops_monitor` retirement

`ops_monitor` keeps two responsibilities:

- **Row-count spike detection.** Moves into a new module `app/services/sync_orchestrator/row_count_spikes.py` and is invoked as a `content_predicate` on layers that care (candles, fundamentals). Signal stays in the layer state machine.
- **Kill switch audit writes.** Stays where it is. Kill switch is a separate concern from freshness and is orthogonal to this spec.

Everything else in `ops_monitor` (`LayerName`, `_STALENESS_THRESHOLDS`, `evaluate_staleness`, per-layer watermark checks that duplicate content predicates) is removed. A deprecation shim re-exports symbols used by callers outside `ops_monitor` itself for one release; the shim logs a warning on import. Callers get migrated in-spec. Nothing external should still depend on `LayerName` after this work.

## 10. Migration plan

Seven ordered chunks. Each is one PR, each is independently mergeable and revertible. Numbering matches the implementation-plan task order.

1. **Introduce `LayerState`, `FailureCategory`, `Cadence`, `RetryPolicy`, `SecretRef` types.** No behaviour change. Registry still uses old `cadence: str`. Pure additions, no imports from callers.
2. **Extend `DataLayer` with the new fields, defaulted so the existing registry literal compiles unchanged.** Add a `to_legacy_string()` shim for the string cadence so `/system/jobs` displays unchanged. Everything still runs through `is_fresh: Callable`.
3. **Wire error-category capture in `adapters.py`.** Adapter functions start raising `LayerRefreshFailed(category=..., detail=...)`. Executor persists category to `job_runs.error_category` (new nullable column, migration in this PR).
4. **Implement `LayerState` computation as a pure function.** New function `compute_layer_state(conn) -> dict[str, LayerState]` with no API caller yet. Covered by unit tests only. `/sync/layers` (v1) is untouched — no field additions, no shape changes.
5. **Ship the new payload at `/sync/layers/v2`; leave `/sync/layers` (v1) unchanged.** Sub-project C's Admin-UI rewrite is the only consumer of v2. v1 stays alive for the current Admin page and any external readers. No cross-version feature flag — a request hits exactly one endpoint, and each response is self-consistent. v1 removal happens only after v2 consumers are all migrated (tracked as a follow-up ticket; not in this spec's scope).
6. **Cascade collapse + scoped resync.** `POST /sync/run?scope=behind|full|layer:X`. Default on a no-query call is `behind` from day one (consistent with §7). The only in-tree caller today is the Admin `Sync now` button; this chunk updates it to issue `scope=behind` in the same PR. No transitional phase is needed because the API has no external consumers.
7. **Retire `ops_monitor`.** Remove `_STALENESS_THRESHOLDS` and `evaluate_staleness`. Move row-count spike logic into `row_count_spikes.py`. Deprecation shim for one release, then delete.

Each chunk has its own tests. Chunks 1–3 are pure additions (new types, new column, new adapter behaviour guarded by a fallback to today's free-form error persistence). Chunk 4 adds `compute_layer_state` as a pure function with no caller — safe. Chunk 5 adds a new endpoint `/sync/layers/v2` alongside the existing flat `/sync/layers`; no flag is needed because no existing caller hits the new URL. Chunk 6 adds the `scope` query parameter; the default flips from today's full-fire to `scope=behind` in the same PR and the in-tree Admin button is updated at the same time. Chunk 7 deletes the legacy `ops_monitor` path.

## 11. Tests

Each chunk ships with tests at the layer of the change:

- **Types (chunk 1).** Type-level only; no tests beyond a `pyright` pass.
- **Registry (chunk 2).** Snapshot test of every layer's `cadence.grace_window` and `plain_language_sla`; trips when a developer adds a layer without setting these.
- **Adapters (chunk 3).** Parametrized test over every `FailureCategory`: given a mocked exception, adapter raises `LayerRefreshFailed` with the expected category. Integration test that `job_runs.error_category` persists after a failed fire.
- **State computation (chunk 4).** Property-based fuzzing over the state decision flow — random `(enabled, running, failures, category, age, content_ok, upstream_state, secret_ok)` tuples. Assert:
  - `HEALTHY` ⇒ latest counting row is success, age ≤ grace, content predicate ok, no pending retry, no missing secret, no blocked upstream (rule 10 gate).
  - `ACTION_NEEDED` ⇒ latest run failed AND (`self_heal=False` OR attempts ≥ max_attempts).
  - `RETRYING` ⇒ latest run failed AND `self_heal=True` AND attempts < max_attempts.
  - `CASCADE_WAITING` ⇒ no local failure, no missing secret, content ok, AND an upstream is `ACTION_NEEDED` or `SECRET_MISSING`.
  - Upstream in `{DEGRADED, RUNNING, RETRYING}` never propagates to downstream `CASCADE_WAITING`.
  - `SECRET_MISSING` beats a prior failure row — verifies rule-3-before-4 ordering.
- **API (chunk 5).** Schema snapshot test on `/sync/layers/v2` response. Contract test asserting `system_state` derivation matches the state counts. Separate test confirming `/sync/layers` (v1) payload is byte-identical before and after the PR — guards against accidental field additions to v1.
- **Cascade + scoped resync (chunk 6).** Unit tests for `collapse_cascades` over hand-crafted state maps (single root, multiple roots, overlapping descendants). Integration test for `POST /sync/run?scope=behind`: seed one DEGRADED layer, fire, assert only that layer + its upstreams ran. Second integration test for `POST /sync/run` with no `scope` query: assert behaviour matches `scope=behind` (pins the default and prevents a silent flip back to full-fire).
- **ops_monitor shim (chunk 7).** Deprecation warning raises `DeprecationWarning` on import. All unit tests still green. `pyright --strict` no longer sees `LayerName`.

## 12. Open questions

None at time of writing. Decisions made during the conversation:

- Autonomy-first: operator levers reduce to enable/disable, secrets, and global kill switch.
- Scoped resync is default; full resync requires a second click.
- Error taxonomy is engineering-owned (in code), not runtime-editable.
- `ops_monitor` is retired, not rebuilt.
