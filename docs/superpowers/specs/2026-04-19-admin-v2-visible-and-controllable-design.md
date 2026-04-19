# Admin v2 — make A visible + operator-controllable (sub-project A.5)

**Date:** 2026-04-19
**Scope:** Ship the first operator-visible fruits of sub-project A (#328) and give the operator direct enable/disable control. Three chunks, all small.
**Relation to earlier work:** sub-project A (merged, chunks 1-7) landed the state machine + `/sync/layers/v2` + `layer_enabled` table, but nothing in the UI consumed them. The Admin page is still byte-identical to the session-start screenshot. This spec closes that gap.

## 1. Problem

Today the operator opens the Admin page and sees eleven red banners, most of which are self-healing or operator-silent by design. The state-machine work in sub-project A fixed the *model* of the problem — `DEGRADED`, `RETRYING`, `CASCADE_WAITING`, `SECRET_MISSING`, `ACTION_NEEDED` now classify every layer — but the old `ProblemsPanel` still reads the legacy `/sync/layers` endpoint and renders the old alarm-everything view.

Consequence: the user's stated goal — "open the Admin page, see green or one thing to fix" — is not yet delivered. They also cannot turn off a layer without editing the DB directly.

## 2. In scope

1. **UI swap.** `AdminPage` + `ProblemsPanel` read `/sync/layers/v2`. Cascade-grouped `action_needed`, plain-language `operator_message` + `operator_fix`, healthy/catching-up summary instead of alarm-on-every-stale-row.
2. **Enable/disable toggle.** One button per layer in the Admin diagnostics pane, backed by a new `POST /sync/layers/{name}/enabled` endpoint wrapping the existing `set_layer_enabled` service.
3. **`/health/data` retirement.** The deprecated endpoint + its `SystemHealth`/`get_system_health` backing are removed. `/system/status` stays alive because the Dashboard page still consumes it — its retirement rolls with sub-project C.

## 3. Out of scope

- **Auto-retry on self-heal failures** (was option (b) during brainstorming). Requires a dedicated spec covering APScheduler `date` trigger + JobLock race, retry-storm prevention, attempts-counter reset semantics, and operator observability of in-flight retries. File `A.6 — auto-retry` next.
- **Full three-zone Admin redesign** (sub-project C). This spec keeps the existing AdminPage structure (problems panel + fund data row + collapsible details) and swaps the data source + adds a toggle.
- **Dashboard page migration to v2.** Out of scope; its SystemStatusPanel is the only reason `/system/status` can't be deleted yet.
- **`LayerName` / `_STALENESS_THRESHOLDS` / `check_all_layers` retirement.** Still referenced by `/system/status`.

## 4. Architecture

```text
                   ┌──────────────────────────────────────────┐
                   │  GET /sync/layers/v2   (spec A §8)       │
                   │   — compute_layer_states_from_db         │
                   │   — collapse_cascades                    │
                   │   — REMEDIES lookup                      │
                   └──────────────────────────────────────────┘
                                  │
                                  ▼
     AdminPage.tsx  →  ProblemsPanel.tsx (new v2 consumer)
                    →  LayerHealthList.tsx (new, inside existing collapsible)
                    →  FundDataRow.tsx (unchanged)

                   ┌──────────────────────────────────────────┐
                   │  POST /sync/layers/{name}/enabled  (new) │
                   │   → set_layer_enabled (existing, chunk 4)│
                   │   → returns LayerSummary + optional       │
                   │     "warning" string                      │
                   └──────────────────────────────────────────┘
                                  │
                                  ▼
                   LayerHealthList row's ⋯ menu
```

No new DB tables. No new migrations. No new background jobs.

## 5. Chunk 1 — UI swap

### 5.1 Backend

The v2 endpoint already returns the shape this chunk needs. No backend change.

### 5.2 Frontend API

Add to `frontend/src/api/sync.ts`:

```ts
export function fetchSyncLayersV2(): Promise<SyncLayersV2Response> {
  return apiFetch<SyncLayersV2Response>("/sync/layers/v2");
}
```

Leave `fetchSyncLayers` in place — `SyncDashboard` (the Admin collapsible) + any other v1 consumer still use it until all downstream UI is ported. (Correction to an earlier brainstorm note: `SystemStatusPanel` in the Dashboard page consumes `/system/status`, not `/sync/layers` — distinct legacy endpoint, unrelated to this spec's scope.)

Add to `frontend/src/api/types.ts` typed mirrors of the Pydantic response, ordered to match the FastAPI-generated schema:

```ts
export interface ActionNeededItem {
  root_layer: string;
  display_name: string;
  category:
    | "auth_expired" | "rate_limited" | "source_down" | "schema_drift"
    | "db_constraint" | "data_gap" | "upstream_waiting" | "internal_error";
  operator_message: string;
  operator_fix: string | null;
  self_heal: boolean;
  consecutive_failures: number;
  affected_downstream: string[];
}

export interface SecretMissingItem {
  layer: string;
  display_name: string;
  missing_secret: string;
  operator_fix: string;
}

export interface LayerSummary {
  layer: string;
  display_name: string;
  last_updated: string | null;  // ISO 8601
}

export interface CascadeGroup {
  root: string;
  affected: string[];
}

export type LayerStateStr =
  | "healthy" | "running" | "retrying" | "degraded"
  | "action_needed" | "secret_missing" | "cascade_waiting" | "disabled";


export interface LayerEntry {
  layer: string;
  display_name: string;
  state: LayerStateStr;
  last_updated: string | null;
  plain_language_sla: string;
}


export interface SyncLayersV2Response {
  generated_at: string;
  system_state: "ok" | "catching_up" | "needs_attention";
  system_summary: string;
  action_needed: ActionNeededItem[];
  degraded: LayerSummary[];
  secret_missing: SecretMissingItem[];
  healthy: LayerSummary[];
  disabled: LayerSummary[];
  cascade_groups: CascadeGroup[];
  layers: LayerEntry[];   // Canonical per-layer list — LayerHealthList reads this
}
```

### 5.3 ProblemsPanel rewrite

`frontend/src/components/admin/ProblemsPanel.tsx` is rewritten to consume `SyncLayersV2Response`. Props change from `(layers, jobs, coverage, ...)` to `(v2, jobsError, coverageError, onOpenOrchestrator)`. `jobs` + `coverage` stay only because coverage-null-rows still surfaces there (chunk-3-era carve-out).

Render rules:

- `v2 === null` → neutral "Checking for problems…" skeleton (same treatment as today).
- `v2.system_state === "ok"` AND `v2.secret_missing.length === 0` AND coverage/jobs clean → panel hidden (unchanged behaviour).
- Otherwise, render:
  - Header: `{v2.system_summary}`.
  - One row per `v2.action_needed` entry:
    - Title: `{display_name} — {operator_message}`.
    - Below: `{operator_fix}`. If it references Settings / Providers, render as a `<Link to="/settings">` so the operator gets there in one click.
    - Count badge: `{consecutive_failures} consecutive failures`.
    - Expandable "+{affected_downstream.length} layers waiting" row when > 0.
  - One row per `v2.secret_missing` entry (amber, not red — not a failure, just config):
    - Title: `{display_name} — credential needed`.
    - Below: `Set {missing_secret} in Settings → Providers`, rendered as a link to `/settings#providers`.
  - Failing jobs + coverage null-rows carry-over from current implementation (already passes tests; orthogonal to v2).

Existing "Could not re-check X" amber stripe for `jobsError` / `coverageError` stays. The panel's stale-cache-on-refetch invariant from the current `ProblemsPanel` is preserved: v2 fetches that return `null` do NOT overwrite the last-good snapshot.

### 5.4 v2 payload extension — canonical `layers` array

Codex review flagged that LayerHealthList cannot render 15 rows from the v2 buckets alone: RUNNING/RETRYING have no per-layer bucket and CASCADE_WAITING is only reachable through `cascade_groups[].affected` without `display_name`/`last_updated`/SLA.

Fix: extend `SyncLayersV2Response` with a single canonical `layers` field before building any UI that depends on it. Backend-only change, fully additive — existing buckets stay.

```python
class LayerEntry(BaseModel):
    layer: str
    display_name: str
    state: Literal[
        "healthy", "running", "retrying", "degraded",
        "action_needed", "secret_missing", "cascade_waiting", "disabled",
    ]
    last_updated: datetime | None
    plain_language_sla: str


class SyncLayersV2Response(BaseModel):
    # ... existing fields ...
    layers: list[LayerEntry]
```

Populated from `compute_layer_states_from_db(conn)` + `LAYERS[name].display_name` + `LAYERS[name].plain_language_sla` + the already-computed `last_updates` map. Every registered layer appears exactly once. Bucket lists stay untouched for the v2 triage consumers (ProblemsPanel reads them); LayerHealthList reads only `layers`.

This is included as a **Chunk 0** backend change that lands ahead of the UI swap.

### 5.5 LayerHealthList

New component `frontend/src/components/admin/LayerHealthList.tsx`, mounted inside the existing `SyncDashboard` (already behind a collapsible). Replaces the 15-row cadence grid that currently renders via `layers.layers.map(...)`.

Renders one row per entry in `v2.layers`. No bucket-reconstruction needed — the server resolves the state and the client only renders.

Pill-coloured state badge:

| State | Pill | Row tone |
|---|---|---|
| `healthy` | green | plain |
| `catching_up` (DEGRADED / RUNNING / RETRYING / CASCADE_WAITING) | amber | plain |
| `needs_attention` (ACTION_NEEDED / SECRET_MISSING) | red | red-tinted |
| `disabled` | grey | dimmed 50% |

Each row shows `{display_name}`, state pill, `last_updated` relative ("4m ago", "2h ago"), plain-language SLA from registry, and the `⋯` menu (chunk 2).

### 5.5 AdminPage

`frontend/src/pages/AdminPage.tsx` mostly stays. Replace the `layers = useAsync(fetchSyncLayers, [])` with `v2 = useAsync(fetchSyncLayersV2, [])`. Pass through to `ProblemsPanel` and `LayerHealthList`. `FundDataRow` continues to read `coverage` + `recommendations` unchanged. `SyncDashboard` still exists as the inner collapsible but its body becomes `<LayerHealthList v2={v2.data} />` instead of the old freshness grid.

Delete the `ORCHESTRATOR_OWNED` carve-out (no longer relevant — jobs table is coming from a separate endpoint and still works).

Polling cadence unchanged: 10s when a sync is running (`status.data?.is_running`), 60s otherwise.

### 5.6 Drill-through

The existing `onOpenOrchestrator` callback that scrolls to the collapsible stays, but the v2 rewrite adds a per-layer deep-link: clicking a row scrolls to that layer's position in `LayerHealthList` and opens the parent collapsible. Implementation: `LayerHealthList` renders each row with `id="admin-layer-{name}"` and `ProblemsPanel` calls `scrollIntoView` on that id when the operator clicks the `Open orchestrator details` action on an `action_needed` row.

## 6. Chunk 2 — enable/disable toggle

### 6.1 API

New endpoint at `app/api/sync.py`:

```python
class LayerEnabledRequest(BaseModel):
    enabled: bool


class LayerEnabledResponse(BaseModel):
    layer: str
    display_name: str
    is_enabled: bool
    warning: str | None = None


@router.post("/layers/{layer_name}/enabled", response_model=LayerEnabledResponse)
def post_layer_enabled(
    layer_name: str,
    body: LayerEnabledRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> LayerEnabledResponse:
    if layer_name not in LAYERS:
        raise HTTPException(status_code=404, detail=f"unknown layer: {layer_name}")
    set_layer_enabled(conn, layer_name, enabled=body.enabled)
    warning = _safety_warning(layer_name, body.enabled)
    return LayerEnabledResponse(
        layer=layer_name,
        display_name=LAYERS[layer_name].display_name,
        is_enabled=body.enabled,
        warning=warning,
    )


_SAFETY_CRITICAL = {"fx_rates", "portfolio_sync"}


def _safety_warning(layer_name: str, enabled: bool) -> str | None:
    if enabled:
        return None
    if layer_name == "fx_rates":
        return (
            "FX rates disabled — portfolio valuations and P&L will drift. "
            "Re-enable before resuming live operation."
        )
    if layer_name == "portfolio_sync":
        return (
            "Portfolio sync disabled — broker positions will not refresh. "
            "Re-enable before resuming live operation."
        )
    return None
```

Auth: inherits the router's `require_session_or_service_token`.

Policy (a) from brainstorming — "any layer can be disabled, warn on safety-critical" — holds at the API layer. BUT the execution path must fail closed when disable puts trading at risk. Codex review caught that the current execution_guard does not consult `layer_enabled`, so a warning alone lets trading continue on intentionally-frozen valuation/position data.

### 6.2 Execution guard — fail-closed on safety-critical disable

`app/services/execution_guard.py` already partitions rules via the BUY/ADD-only block at line ~723. Add a new rule in that partition.

Contract changes:

- Extend `RuleName` (the `Literal` alias in execution_guard) with `"safety_layers_enabled"`.
- Add a helper:

  ```python
  def _check_safety_layers_enabled(
      conn: psycopg.Connection[Any],
  ) -> RuleResult:
      """Refuse BUY/ADD when fx_rates or portfolio_sync is disabled.

      FX disabled → USD valuations + budget drift silently.
      Portfolio sync disabled → position baseline goes stale,
      exposure and concentration checks lie. Blocking only BUY/ADD
      preserves the emergency-EXIT path operators need when
      intentionally de-risking.
      """
      from app.services.layer_enabled import is_layer_enabled

      disabled = [
          name for name in ("fx_rates", "portfolio_sync")
          if not is_layer_enabled(conn, name)
      ]
      if disabled:
          return RuleResult.failed(
              rule="safety_layers_enabled",
              detail=(
                  f"{' + '.join(disabled)} disabled — BUY/ADD blocked; "
                  "re-enable the layer or switch auto/live trading off to clear."
              ),
          )
      return RuleResult.passed(rule="safety_layers_enabled")
  ```

- Call it inside the BUY/ADD-only append block in `evaluate_recommendation`, alongside the existing `_check_live_trading(runtime.enable_live_trading)` line. The check fires whenever the recommendation `action ∈ {BUY, ADD}` — the framework already gates on action elsewhere, so no extra demo/live branching is needed here. SELL / EXIT recommendations never hit this block, preserving emergency-close behaviour.

- Add tests to `tests/test_execution_guard.py` (or the nearest pertinent file):
  - BUY blocked when `set_layer_enabled(conn, "fx_rates", enabled=False)` was called, detail mentions `fx_rates`.
  - ADD blocked when `portfolio_sync` disabled.
  - EXIT allowed (rule not on the EXIT path).
  - BUY allowed when both layers re-enabled.
  - Detail string pins the operator-facing text so the UI can show a sensible reason.

Execution-guard has no runtime-config gate here — disabling a safety layer always blocks BUY/ADD regardless of `enable_auto_trading` / `enable_live_trading`, because the risk is the same whether the trade is auto or manually triggered. Demo mode does not exempt because live-trading's live-only gate (`_check_live_trading`) already blocks BUYs in demo; the new rule layers on top without double-gating.

### 6.2 Frontend API

Add to `frontend/src/api/sync.ts`:

```ts
export interface LayerEnabledResponse {
  layer: string;
  display_name: string;
  is_enabled: boolean;
  warning: string | null;
}


export function setLayerEnabled(
  layerName: string,
  enabled: boolean,
): Promise<LayerEnabledResponse> {
  return apiFetch<LayerEnabledResponse>(`/sync/layers/${layerName}/enabled`, {
    method: "POST",
    body: JSON.stringify({ enabled }),
  });
}
```

### 6.3 UI wiring

In `LayerHealthList.tsx`, each row gets a `⋯` icon button. Click → small dropdown with `Disable layer` (or `Enable layer` if already disabled). Click:

- For non-safety-critical layers, calls `setLayerEnabled` immediately.
- For `fx_rates` / `portfolio_sync`, a `window.confirm` dialog fires first: "Disable FX rates? Portfolio valuations will drift until re-enabled." If confirmed, the POST fires.
- On success, the returned `warning` (if any) shows as a toast and the AdminPage auto-refreshes (`refetchAll()` already wired).
- On error (network / 404 / 500), toast with the status code; same pattern as the existing `handleRun` in AdminPage.

A disabled layer renders greyed out (opacity 0.5), state pill reads `Disabled`. Scheduling behaviour: **planner-time** skip is already wired (chunk 4 state machine → chunk 6 `scope=behind` planner). An already-planned or already-running sync will still invoke the disabled layer's adapter because the executor does not re-check `layer_enabled` mid-run. Accepted: toggle applies to future plans only. Documented in the UI toast when the operator disables mid-sync: "Layer will stop firing after the current sync completes."

## 7. Chunk 3 — `/health/data` retirement

Delete:

- `@app.get("/health/data", ...)` in `app/main.py` including the `get_system_health` call + its JSON response block.
- `SystemHealth` dataclass (`ops_monitor.py:156`).
- `get_system_health` function (`ops_monitor.py:706`).

Test consumers to update (verified via grep):

- `tests/test_api_main.py` — drop only `/health/data`-specific cases; keep anything testing `/health` or other endpoints.
- `tests/test_api_system.py` — review per-test; keep `/system/status` cases.
- `tests/test_api_auth.py:139` — `test_health_data_requires_auth` asserts `/health/data` is auth-gated. Replace that test with an equivalent assertion against another router-protected endpoint (e.g. `/system/status` or `/portfolio`) so the "protected endpoint rejects unauthenticated" invariant still has a guard.
- `tests/test_ops_monitor.py:35` — `TestGetSystemHealth` class imports + exercises `get_system_health`. Delete only that class and its import; keep every other test group in the file (row-count spikes, kill switch, job_runs helpers, staleness).

Keep (still consumed by `/system/status`):

- `check_all_layers`, `LayerHealth`, `LayerStatus`, `check_job_health`, `JobHealth`, `LayerName`, `ALL_LAYERS`, `_STALENESS_THRESHOLDS`, `_LAYER_QUERIES`.

The post-deletion grep should show these symbols used only by `app/api/system.py` + `check_all_layers`-exclusive tests + their own module. Any stray import elsewhere means the deletion is wrong and needs further investigation.

#340 (full ops_monitor retirement) remains open — its closure is gated on Dashboard migration which this spec explicitly does not touch.

## 8. Error handling

- v2 endpoint fetch failure: AdminPage renders the existing `SectionError` skeleton with retry. Same pattern as today.
- Toggle endpoint failure: toast with status code. Row state unchanged until a successful response arrives. No optimistic UI.
- A layer in v2 that is neither healthy/degraded/disabled/action_needed/secret_missing is classified as "catching up" in LayerHealthList. Never silently dropped.
- `ProblemsPanel` keeps the per-source cache so a refetch-in-flight does not blank the red banner.

## 9. Tests

### 9.1 pytest

- `tests/api/test_sync_layer_enabled_endpoint.py` (new):
  - `POST /sync/layers/candles/enabled` with `{"enabled": false}` → 200, `warning is None`, `is_layer_enabled(conn, "candles")` is `False`.
  - Same, `{"enabled": true}` → 200, `warning is None`, re-enabled.
  - `POST /sync/layers/fx_rates/enabled` with `{"enabled": false}` → 200, `warning` contains "drift".
  - `POST /sync/layers/portfolio_sync/enabled` with `{"enabled": false}` → 200, `warning` contains "broker".
  - `POST /sync/layers/not_a_real_layer/enabled` → 404.
  - Unauthenticated → 401.
- `tests/test_api_main.py`: delete `/health/data`-only tests; keep any that exercise other endpoints.

### 9.2 vitest

- `frontend/src/components/admin/ProblemsPanel.test.tsx` rewrite:
  - v2 with `system_state=ok` + no secrets → panel does not render.
  - v2 with one `action_needed` → red row with title, operator_fix rendered, `Open orchestrator details` fires callback.
  - v2 with one `secret_missing` → amber row with Settings link.
  - v2 with one `action_needed` + `cascade_groups[0].affected` of length 3 → expand reveals the 3 waiter rows.
  - `v2 === null` → "Checking for problems…" skeleton.
  - v2 fetched, then refetch in flight (temporarily null) → last-good snapshot still renders.
- `frontend/src/components/admin/LayerHealthList.test.tsx` (new): 15 rows, pill colours, disabled greyed, toggle click calls `setLayerEnabled`, safety-critical disable prompts `window.confirm`.

## 10. Migration order

Four PRs, in order:

1. **Chunk 0** (v2 payload extension). Adds `layers: list[LayerEntry]` to `SyncLayersV2Response`. Backend only. Additive — existing consumers (just the v2 schema test) unaffected. Tests: extend `test_sync_layers_v2_schema.py` with an assertion on the `layers` array shape + coverage of every registered layer exactly once.
2. **Chunk 1** (UI swap). AdminPage + ProblemsPanel + LayerHealthList consume v2's new `layers` field + existing buckets. `/sync/layers` v1 remains untouched.
3. **Chunk 2** (toggle + execution-guard fail-closed). New `POST /sync/layers/{name}/enabled` endpoint + LayerHealthList `⋯` menu + execution_guard fail-closed on BUY/ADD when `fx_rates` / `portfolio_sync` disabled in non-demo mode.
4. **Chunk 3** (`/health/data` retirement). Tiny deletion PR. No consumers of `/health/data` exist post-chunk-1. Test updates per §7.

Each chunk is independently revertible.

## 11. Follow-ups

Filed alongside the umbrella issue, not in this spec:

- **A.6 — auto-retry on self-heal failures.** APScheduler `date` trigger + JobLock semantics + attempts-reset invariant.
- **Extend v2 with explicit `running` + `retrying` buckets.** Today LayerHealthList folds these into "catching up"; a per-layer pill would make a fire-in-flight visible.
- **#340 unblock when sub-project C retires Dashboard's v1 consumer.** Then the legacy `LayerName` / `_STALENESS_THRESHOLDS` / `check_all_layers` deletion becomes safe.
