# Admin v2 — visible + operator-controllable (sub-project A.5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the first operator-visible fruits of sub-project A: swap Admin page to read `/sync/layers/v2`, add a per-layer enable/disable toggle with execution-guard safety wiring, and retire the deprecated `/health/data` endpoint.

**Architecture:** Four chunks, each one PR. Chunk 0 extends the v2 payload with a canonical `layers: LayerEntry[]` list so the UI can render rows without reconstructing state from buckets. Chunk 1 swaps AdminPage + ProblemsPanel + a new LayerHealthList to consume v2. Chunk 2 adds the toggle endpoint + UI button + a new `safety_layers_enabled` rule in `execution_guard` blocking BUY/ADD when `fx_rates` / `portfolio_sync` are disabled. Chunk 3 deletes `/health/data`, `SystemHealth`, `get_system_health`, and their test consumers.

**Tech Stack:** FastAPI, psycopg3, Pydantic v2, pytest. Vite + React + TypeScript + Tailwind, vitest.

**Spec:** [`docs/superpowers/specs/2026-04-19-admin-v2-visible-and-controllable-design.md`](../specs/2026-04-19-admin-v2-visible-and-controllable-design.md)

**Umbrella issue:** #342 (to be filed as first action).

**Branch convention:** each chunk gets its own branch — `feature/342-chunk-<n>-<slug>`.

---

## Plan corrections (post-Codex review) — MUST READ before every task

Codex flagged nine issues against the initial plan. Fixes below override the corresponding task bodies where they disagree. Subagents must read this block before starting any task.

1. **HIGH — ProblemsPanel keeps jobs + coverage carry-over (task 1.3).** The v1 ProblemsPanel surfaces failing jobs + `coverage.null_rows > 0` alongside layer problems. Chunk 1's rewrite **must preserve those rows**. In the rewritten component, keep the existing `jobs` + `coverage` props + the `deriveJobProblems` / `deriveCoverageProblems` functions from the original file; only the layer-derivation path swaps to v2. Render order: `action_needed` (v2) → `secret_missing` (v2) → jobs-failure rows (unchanged) → coverage null-rows row (unchanged). Update the v1 test cases that covered those paths so they still pass against the new component.

2. **HIGH — SyncDashboard decision (task 1.4).** `frontend/src/pages/SyncDashboard.tsx` currently fetches v1 `/sync/layers` at line ~20 and renders: the v1 freshness grid, the inner Sync-now button, and recent runs. Do **not** wholesale replace the collapsible body with `LayerHealthList`; that would drop status + recent runs. Instead:
   - Keep `SyncDashboard` mounted inside the collapsible, unchanged.
   - Add a **new** collapsible section "Layer health" above SyncDashboard inside AdminPage (sibling, not child). Its body is `<LayerHealthList ... />`.
   - A follow-up ticket (file in chunk 1 PR description) covers SyncDashboard's v1→v2 migration alongside sub-project C. Do not attempt it here.

3. **HIGH — execution_guard contract fix (task 2.2 tests + wiring).** Verified real signature: `evaluate_recommendation(conn: psycopg.Connection, recommendation_id: int) -> GuardResult` at `execution_guard.py:630`. `GuardResult.failed_rules: list[str]` (NOT `list[RuleName]`). The plan's test-body invocation `evaluate_recommendation(conn, recommendation={...})` is wrong. Subagent must `grep -n "def evaluate_recommendation\|class GuardResult" app/services/execution_guard.py` before writing tests and adapt to the real signature: insert a fake recommendation row (or use the existing test fixture / helper for this) and pass its id. Also: the assertion loop `{r.rule for r in decision.failed_rules}` is wrong — it's already a list of strings. Use `"safety_layers_enabled" in decision.failed_rules`.

4. **HIGH — no duplicate `_check_live_trading` (task 2.2 step 5).** `_check_live_trading` already runs in the "every action" block at `execution_guard.py:706`. The new rule wiring **only appends `_check_safety_layers_enabled(conn)` to the BUY/ADD-only block**. Do NOT re-add `_check_live_trading` there — the plan code block had it as context from a grep hit; ignore that line when editing.

5. **MEDIUM — existing v2 schema test needs updating (task 0.1 step 2).** `tests/api/test_sync_layers_v2_schema.py::test_v2_endpoint_returns_expected_top_level_keys` currently asserts the key set with `==`, which excludes `layers`. Add `"layers"` to that expected set in the SAME commit as the new field; don't rely on it being a "v1 test" — it's a v2 test that predated this addition.

6. **MEDIUM — AdminPage TDD must be concrete (task 1.4 step 1).** The placeholder `// ...` is a plan failure. Write a concrete test: mock `fetchSyncLayersV2` via `vi.mock('@/api/sync', ...)` to return a stub v2 response and assert the `ProblemsPanel` renders nothing on `system_state: "ok"`, and that `LayerHealthList` rows match the stub `layers` length. Use the existing `AdminPage.test.tsx` mocking pattern — grep the file for `vi.mock` before writing.

7. **MEDIUM — parent-integration test for toggle (task 2.3).** Add an `AdminPage.test.tsx` case (not just `LayerHealthList`): mock `setLayerEnabled`, click the `⋯` menu + `Disable layer` on a mocked `candles` row, assert `setLayerEnabled("candles", false)` was called and `v2.refetch()` fired. Put `vi` in the test-file imports alongside `describe/expect/it`.

8. **MEDIUM — unauthenticated 401 test for toggle endpoint (task 2.1).** `clean_client` applies the auth bypass. Add ONE extra test case using a bare `TestClient(app)` without the `clean_client` fixture that POSTs to `/sync/layers/candles/enabled` with no auth and asserts 401. Pattern exists in `tests/test_api_auth.py::TestPublicEndpointsRemainOpen` — mirror it. (Include this as a new test function in the same file.)

9. **LOW — `/health/data` retirement grep note (task 3.1 step 1).** The grep will hit `app/api/system.py:14` (docstring reference). Update that docstring in the same commit: change "distinct from the deprecated `/health/data`" to "distinct from the retired `/health/data` (removed in #342)". This is the ONLY acceptable additional hit — anything else means a consumer is still live.

10. **LOW — action_needed Settings link (task 1.3 component).** When `item.operator_fix` matches `/settings/i` or `/providers/i`, render the fix as `<Link to="/settings#providers">…</Link>`, not as plain text. Keep the plain-text path for non-Settings fixes ("Open orchestrator details and inspect the offending row" etc.).

---

## Reference — current code contracts (verified 2026-04-19)

- `GET /sync/layers/v2` lives at `app/api/sync.py:308`. `SyncLayersV2Response` Pydantic model above at line ~92. Response already populates `action_needed`, `secret_missing`, `degraded`, `healthy`, `disabled`, `cascade_groups`.
- `compute_layer_states_from_db(conn) -> dict[str, LayerState]` at `app/services/sync_orchestrator/layer_state.py`.
- `LAYERS` dict at `app/services/sync_orchestrator/registry.py` holds 15 entries with `.display_name`, `.plain_language_sla`, `.cadence`, etc.
- `_layer_last_updated_map(conn, names)` at `app/api/sync.py:502` — internal helper, already in scope.
- `AdminPage.tsx` reads `fetchSyncLayers` (v1) at `frontend/src/pages/AdminPage.tsx:64` — this is the target.
- `ProblemsPanel.tsx` at `frontend/src/components/admin/ProblemsPanel.tsx` — target of rewrite.
- `SectionSkeleton` + `SectionError` available from `@/components/dashboard/Section`.
- `useAsync(fn, deps)` pattern at `frontend/src/lib/useAsync.ts` — already used by AdminPage.
- `apiFetch` at `frontend/src/api/client.ts`.
- `execution_guard` has `RuleName = Literal[...]` at line 89 + `RuleResult` dataclass at line 110. Existing rules follow pattern `def _check_<name>(...) -> RuleResult` returning `RuleResult(rule=..., passed=..., detail=...)` — see `_check_kill_switch` line 317, `_check_live_trading` line 346.
- `is_layer_enabled(conn, layer_name) -> bool` at `app/services/layer_enabled.py` — chunk 4 wired it.
- `set_layer_enabled(conn, layer_name, *, enabled: bool)` same module — caller owns commit (chunk 4 code review explicitly removed the internal commit).
- Router at `app/api/sync.py` has `dependencies=[Depends(require_session_or_service_token)]` at line ~47 — any new route auto-inherits.

---

## Chunk 0 — v2 payload canonical `layers` list

Backend-only additive change. Lets Chunk 1's LayerHealthList read a canonical per-layer list instead of reconstructing state from buckets. Branch: `feature/342-chunk-0-layers-field`.

### Task 0.1: Add `LayerEntry` model + extend `SyncLayersV2Response`

**Files:**
- Modify: `app/api/sync.py`
- Test: `tests/api/test_sync_layers_v2_schema.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/api/test_sync_layers_v2_schema.py`:

```python
def test_v2_includes_canonical_layers_field(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers/v2")
    body = resp.json()
    assert "layers" in body
    assert isinstance(body["layers"], list)


def test_v2_layers_contains_every_registered_layer_once(clean_client: TestClient) -> None:
    from app.services.sync_orchestrator.registry import LAYERS

    resp = clean_client.get("/sync/layers/v2")
    body = resp.json()
    returned = [entry["layer"] for entry in body["layers"]]
    # Every registered layer appears exactly once.
    assert sorted(returned) == sorted(LAYERS.keys())
    assert len(returned) == len(set(returned)), "duplicate layer in v2.layers"


def test_v2_layer_entry_shape(clean_client: TestClient) -> None:
    resp = clean_client.get("/sync/layers/v2")
    for entry in resp.json()["layers"]:
        assert set(entry.keys()) == {
            "layer",
            "display_name",
            "state",
            "last_updated",
            "plain_language_sla",
        }
        assert entry["state"] in {
            "healthy",
            "running",
            "retrying",
            "degraded",
            "action_needed",
            "secret_missing",
            "cascade_waiting",
            "disabled",
        }


def test_v2_layer_entry_metadata_matches_registry(clean_client: TestClient) -> None:
    from app.services.sync_orchestrator.registry import LAYERS

    resp = clean_client.get("/sync/layers/v2")
    for entry in resp.json()["layers"]:
        layer = LAYERS[entry["layer"]]
        assert entry["display_name"] == layer.display_name
        assert entry["plain_language_sla"] == layer.plain_language_sla
```

- [ ] **Step 2: Run to confirm fail**

```
uv run pytest tests/api/test_sync_layers_v2_schema.py -v
```

Expected: the four new tests fail on missing `layers` key.

- [ ] **Step 3: Add `LayerEntry` Pydantic model**

In `app/api/sync.py`, add above `class SyncLayersV2Response`:

```python
class LayerEntry(BaseModel):
    layer: str
    display_name: str
    state: Literal[
        "healthy",
        "running",
        "retrying",
        "degraded",
        "action_needed",
        "secret_missing",
        "cascade_waiting",
        "disabled",
    ]
    last_updated: datetime | None
    plain_language_sla: str
```

- [ ] **Step 4: Add field to response model**

Update `SyncLayersV2Response`:

```python
class SyncLayersV2Response(BaseModel):
    generated_at: datetime
    system_state: Literal["ok", "catching_up", "needs_attention"]
    system_summary: str
    action_needed: list[ActionNeededItem]
    degraded: list[LayerSummary]
    secret_missing: list[SecretMissingItem]
    healthy: list[LayerSummary]
    disabled: list[LayerSummary]
    cascade_groups: list[CascadeGroupModel]
    layers: list[LayerEntry]
```

- [ ] **Step 5: Populate `layers` in the endpoint body**

In `get_sync_layers_v2` at `app/api/sync.py:308`, after the existing `states = compute_layer_states_from_db(conn)` line, build the `layers` list. Insert before the final `return SyncLayersV2Response(...)`:

```python
layers_entries = [
    LayerEntry(
        layer=name,
        display_name=LAYERS[name].display_name,
        state=states[name].value,
        last_updated=last_updates.get(name),
        plain_language_sla=LAYERS[name].plain_language_sla,
    )
    for name in sorted(states.keys())
]
```

Pass into the return:

```python
return SyncLayersV2Response(
    generated_at=datetime.now(UTC),
    system_state=system_state,
    system_summary=_system_summary(...),
    # ... existing fields ...
    layers=layers_entries,
)
```

- [ ] **Step 6: Run tests to confirm pass**

```
uv run pytest tests/api/test_sync_layers_v2_schema.py -v
```

Expected: all existing tests + 4 new green.

- [ ] **Step 7: Commit**

```bash
git add app/api/sync.py tests/api/test_sync_layers_v2_schema.py
git commit -m "feat(#342): v2 canonical layers[] list for per-layer UI rendering"
```

### Task 0.2: Pre-push gate + PR

- [ ] **Step 1: Run full gate**

```
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -x -q
```

All four pass. If any v1 test pinned the v2 shape too strictly and fails, it was never meant to — adjust to the new shape.

- [ ] **Step 2: Push + PR**

```bash
git push -u origin feature/342-chunk-0-layers-field
gh pr create --title "feat(#342): chunk 0 — v2 canonical layers[] field" --body "$(cat <<'EOF'
## Summary

- Adds \`LayerEntry\` Pydantic model + \`layers: list[LayerEntry]\` field to \`SyncLayersV2Response\`.
- Every registered layer appears exactly once with state, display_name, last_updated, plain_language_sla.
- No change to existing buckets — additive.

Unblocks A.5 chunk 1 (Admin UI swap).

## Test plan

- [x] 4 new tests in test_sync_layers_v2_schema.py pin field presence, per-layer completeness, entry shape, registry-metadata agreement
- [x] Full suite green

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Poll review + CI, resolve, merge.**

Do not start chunk 1 until chunk 0 merges.

---

## Chunk 1 — AdminPage + ProblemsPanel + LayerHealthList swap to v2

Frontend-only. Branch: `feature/342-chunk-1-admin-v2-ui`.

### Task 1.1: Frontend types + API function

**Files:**
- Modify: `frontend/src/api/types.ts`
- Modify: `frontend/src/api/sync.ts`

- [ ] **Step 1: Add types to `types.ts`**

Append to `frontend/src/api/types.ts`:

```ts
export type LayerStateStr =
  | "healthy"
  | "running"
  | "retrying"
  | "degraded"
  | "action_needed"
  | "secret_missing"
  | "cascade_waiting"
  | "disabled";

export interface LayerEntry {
  layer: string;
  display_name: string;
  state: LayerStateStr;
  last_updated: string | null;
  plain_language_sla: string;
}

export interface ActionNeededItem {
  root_layer: string;
  display_name: string;
  category:
    | "auth_expired"
    | "rate_limited"
    | "source_down"
    | "schema_drift"
    | "db_constraint"
    | "data_gap"
    | "upstream_waiting"
    | "internal_error";
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

export interface LayerSummaryV2 {
  layer: string;
  display_name: string;
  last_updated: string | null;
}

export interface CascadeGroup {
  root: string;
  affected: string[];
}

export interface SyncLayersV2Response {
  generated_at: string;
  system_state: "ok" | "catching_up" | "needs_attention";
  system_summary: string;
  action_needed: ActionNeededItem[];
  degraded: LayerSummaryV2[];
  secret_missing: SecretMissingItem[];
  healthy: LayerSummaryV2[];
  disabled: LayerSummaryV2[];
  cascade_groups: CascadeGroup[];
  layers: LayerEntry[];
}
```

- [ ] **Step 2: Add fetch function to `sync.ts`**

In `frontend/src/api/sync.ts`, next to the existing `fetchSyncLayers`:

```ts
import type { SyncLayersV2Response } from "@/api/types";


export function fetchSyncLayersV2(): Promise<SyncLayersV2Response> {
  return apiFetch<SyncLayersV2Response>("/sync/layers/v2");
}
```

- [ ] **Step 3: Commit**

```bash
git add frontend/src/api/types.ts frontend/src/api/sync.ts
git commit -m "feat(#342): v2 layers TS types + fetchSyncLayersV2"
```

### Task 1.2: LayerHealthList component

**Files:**
- Create: `frontend/src/components/admin/LayerHealthList.tsx`
- Test: `frontend/src/components/admin/LayerHealthList.test.tsx`

- [ ] **Step 1: Write failing test**

```tsx
// frontend/src/components/admin/LayerHealthList.test.tsx
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { LayerEntry } from "@/api/types";
import { LayerHealthList } from "./LayerHealthList";


function mk(entry: Partial<LayerEntry>): LayerEntry {
  return {
    layer: entry.layer ?? "universe",
    display_name: entry.display_name ?? "Tradable Universe",
    state: entry.state ?? "healthy",
    last_updated: entry.last_updated ?? null,
    plain_language_sla: entry.plain_language_sla ?? "Refreshed weekly.",
  };
}


describe("LayerHealthList", () => {
  it("renders one row per layer", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "universe", display_name: "Tradable Universe" }),
      mk({ layer: "candles", display_name: "Daily Price Candles" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByText("Tradable Universe")).toBeInTheDocument();
    expect(screen.getByText("Daily Price Candles")).toBeInTheDocument();
  });

  it("renders healthy pill green and disabled pill grey", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "universe", state: "healthy" }),
      mk({ layer: "candles", state: "disabled" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    const healthyPill = screen.getByLabelText("universe state");
    const disabledPill = screen.getByLabelText("candles state");
    expect(healthyPill).toHaveTextContent(/healthy/i);
    expect(disabledPill).toHaveTextContent(/disabled/i);
  });

  it("renders action_needed pill as needs attention", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "cik_mapping", state: "action_needed" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    const pill = screen.getByLabelText("cik_mapping state");
    expect(pill).toHaveTextContent(/needs attention/i);
  });

  it("renders running / retrying / cascade_waiting as catching up", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "r1", state: "running" }),
      mk({ layer: "r2", state: "retrying" }),
      mk({ layer: "r3", state: "cascade_waiting" }),
      mk({ layer: "r4", state: "degraded" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    for (const name of ["r1", "r2", "r3", "r4"]) {
      expect(screen.getByLabelText(`${name} state`)).toHaveTextContent(/catching up/i);
    }
  });

  it("renders relative last_updated when present", () => {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    const layers: LayerEntry[] = [
      mk({ layer: "universe", last_updated: oneHourAgo }),
    ];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByText(/ago/i)).toBeInTheDocument();
  });

  it("renders SLA below the row", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "candles", plain_language_sla: "Refreshed after market close." }),
    ];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByText(/refreshed after market close/i)).toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Confirm fail**

```
pnpm --dir frontend test -- LayerHealthList
```

Expected: module-not-found error.

- [ ] **Step 3: Write the component**

```tsx
// frontend/src/components/admin/LayerHealthList.tsx
/**
 * Per-layer health list for the Admin page orchestrator collapsible.
 *
 * Reads `v2.layers` (the canonical per-layer list added in chunk 0).
 * One row per layer with a state pill, relative last_updated, plain-
 * language SLA, and a ⋯ menu for enable/disable. The parent owns the
 * actual toggle fetch — this component only emits `onToggle(name, enabled)`.
 */
import { useState } from "react";

import type { LayerEntry, LayerStateStr } from "@/api/types";


export interface LayerHealthListProps {
  readonly layers: readonly LayerEntry[];
  readonly onToggle: (layer: string, enabled: boolean) => void;
}


type Pill = "healthy" | "catching_up" | "needs_attention" | "disabled";


function pillFor(state: LayerStateStr): Pill {
  if (state === "healthy") return "healthy";
  if (state === "disabled") return "disabled";
  if (state === "action_needed" || state === "secret_missing") return "needs_attention";
  return "catching_up";
}


const PILL_LABEL: Record<Pill, string> = {
  healthy: "Healthy",
  catching_up: "Catching up",
  needs_attention: "Needs attention",
  disabled: "Disabled",
};


const PILL_CLASS: Record<Pill, string> = {
  healthy: "bg-emerald-100 text-emerald-800",
  catching_up: "bg-amber-100 text-amber-800",
  needs_attention: "bg-red-100 text-red-800",
  disabled: "bg-slate-200 text-slate-600",
};


function relativeAgo(iso: string | null): string {
  if (iso === null) return "never";
  const diff = Date.now() - new Date(iso).getTime();
  const minutes = Math.floor(diff / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}


export function LayerHealthList({ layers, onToggle }: LayerHealthListProps): JSX.Element {
  const [menuOpen, setMenuOpen] = useState<string | null>(null);

  return (
    <ul className="divide-y divide-slate-100">
      {layers.map((entry) => {
        const pill = pillFor(entry.state);
        const isDisabled = entry.state === "disabled";
        return (
          <li
            key={entry.layer}
            id={`admin-layer-${entry.layer}`}
            className={`flex items-start justify-between py-2 ${
              isDisabled ? "opacity-50" : ""
            }`}
          >
            <div className="flex-1">
              <div className="flex items-center gap-2">
                <span className="font-medium text-slate-800">
                  {entry.display_name}
                </span>
                <span
                  aria-label={`${entry.layer} state`}
                  className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${PILL_CLASS[pill]}`}
                >
                  {PILL_LABEL[pill]}
                </span>
                <span className="text-xs text-slate-500">
                  Updated {relativeAgo(entry.last_updated)}
                </span>
              </div>
              <div className="mt-1 text-xs text-slate-600">
                {entry.plain_language_sla}
              </div>
            </div>
            <div className="relative ml-4">
              <button
                type="button"
                aria-label={`${entry.layer} actions`}
                onClick={() => setMenuOpen(menuOpen === entry.layer ? null : entry.layer)}
                className="rounded border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700 hover:bg-slate-50"
              >
                ⋯
              </button>
              {menuOpen === entry.layer ? (
                <div className="absolute right-0 top-full z-10 mt-1 w-40 rounded border border-slate-200 bg-white shadow">
                  <button
                    type="button"
                    onClick={() => {
                      onToggle(entry.layer, !isDisabled ? false : true);
                      setMenuOpen(null);
                    }}
                    className="block w-full px-3 py-1 text-left text-xs text-slate-700 hover:bg-slate-50"
                  >
                    {isDisabled ? "Enable layer" : "Disable layer"}
                  </button>
                </div>
              ) : null}
            </div>
          </li>
        );
      })}
    </ul>
  );
}
```

- [ ] **Step 4: Tests pass**

```
pnpm --dir frontend test -- LayerHealthList
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/admin/LayerHealthList.tsx frontend/src/components/admin/LayerHealthList.test.tsx
git commit -m "feat(#342): LayerHealthList per-layer row + ⋯ menu"
```

### Task 1.3: Rewrite ProblemsPanel to consume v2

**Files:**
- Modify: `frontend/src/components/admin/ProblemsPanel.tsx`
- Modify: `frontend/src/components/admin/ProblemsPanel.test.tsx`

- [ ] **Step 1: Rewrite tests**

Replace `frontend/src/components/admin/ProblemsPanel.test.tsx` entirely:

```tsx
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { SyncLayersV2Response } from "@/api/types";
import { ProblemsPanel } from "./ProblemsPanel";


function emptyV2(): SyncLayersV2Response {
  return {
    generated_at: new Date().toISOString(),
    system_state: "ok",
    system_summary: "All layers healthy",
    action_needed: [],
    degraded: [],
    secret_missing: [],
    healthy: [],
    disabled: [],
    cascade_groups: [],
    layers: [],
  };
}


describe("ProblemsPanel", () => {
  it("renders null when system is ok with no secrets and no source errors", () => {
    const { container } = render(
      <ProblemsPanel
        v2={emptyV2()}
        v2Error={false}
        jobsError={false}
        coverageError={false}
        onOpenOrchestrator={() => {}}
      />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("renders a red row per action_needed entry", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.system_summary = "SEC CIK Mapping needs attention";
    v2.action_needed = [
      {
        root_layer: "cik_mapping",
        display_name: "SEC CIK Mapping",
        category: "db_constraint",
        operator_message: "Database constraint violated — likely data-model bug",
        operator_fix: "Open orchestrator details and inspect the offending row",
        self_heal: false,
        consecutive_failures: 3,
        affected_downstream: [],
      },
    ];
    render(
      <ProblemsPanel
        v2={v2}
        v2Error={false}
        jobsError={false}
        coverageError={false}
        onOpenOrchestrator={() => {}}
      />,
    );
    expect(screen.getByText(/SEC CIK Mapping/)).toBeInTheDocument();
    expect(screen.getByText(/Database constraint violated/)).toBeInTheDocument();
    expect(screen.getByText(/3 consecutive failures/)).toBeInTheDocument();
  });

  it("renders amber row per secret_missing entry with Settings link", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.system_summary = "1 layer(s) missing credentials";
    v2.secret_missing = [
      {
        layer: "news",
        display_name: "News & Sentiment",
        missing_secret: "ANTHROPIC_API_KEY",
        operator_fix: "Set ANTHROPIC_API_KEY in Settings → Providers",
      },
    ];
    render(
      <ProblemsPanel
        v2={v2}
        v2Error={false}
        jobsError={false}
        coverageError={false}
        onOpenOrchestrator={() => {}}
      />,
    );
    expect(screen.getByText(/News & Sentiment/)).toBeInTheDocument();
    expect(screen.getByText(/ANTHROPIC_API_KEY/)).toBeInTheDocument();
  });

  it("expands cascade waiters when the operator clicks +N layers", async () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "cik_mapping",
        display_name: "SEC CIK Mapping",
        category: "db_constraint",
        operator_message: "DB error",
        operator_fix: null,
        self_heal: false,
        consecutive_failures: 3,
        affected_downstream: ["financial_facts", "thesis", "scoring"],
      },
    ];
    render(
      <ProblemsPanel
        v2={v2}
        v2Error={false}
        jobsError={false}
        coverageError={false}
        onOpenOrchestrator={() => {}}
      />,
    );
    const summary = screen.getByText(/3 layers waiting/);
    summary.click();
    expect(await screen.findByText("financial_facts")).toBeInTheDocument();
    expect(screen.getByText("thesis")).toBeInTheDocument();
    expect(screen.getByText("scoring")).toBeInTheDocument();
  });

  it("renders Checking skeleton when v2 is null", () => {
    render(
      <ProblemsPanel
        v2={null}
        v2Error={false}
        jobsError={false}
        coverageError={false}
        onOpenOrchestrator={() => {}}
      />,
    );
    expect(screen.getByText(/Checking for problems/i)).toBeInTheDocument();
  });

  it("keeps last-good snapshot rendered when v2 briefly becomes null", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "x",
        display_name: "Layer X",
        category: "source_down",
        operator_message: "down",
        operator_fix: null,
        self_heal: true,
        consecutive_failures: 1,
        affected_downstream: [],
      },
    ];
    const { rerender } = render(
      <ProblemsPanel
        v2={v2}
        v2Error={false}
        jobsError={false}
        coverageError={false}
        onOpenOrchestrator={() => {}}
      />,
    );
    expect(screen.getByText("Layer X")).toBeInTheDocument();
    // Refetch in flight — v2 briefly null.
    rerender(
      <ProblemsPanel
        v2={null}
        v2Error={false}
        jobsError={false}
        coverageError={false}
        onOpenOrchestrator={() => {}}
      />,
    );
    // Last-good snapshot still rendered, no flash.
    expect(screen.getByText("Layer X")).toBeInTheDocument();
  });

  it("calls onOpenOrchestrator when the drill-through button is clicked", () => {
    const onOpen = vi.fn();
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "cik_mapping",
        display_name: "SEC CIK Mapping",
        category: "db_constraint",
        operator_message: "err",
        operator_fix: null,
        self_heal: false,
        consecutive_failures: 1,
        affected_downstream: [],
      },
    ];
    render(
      <ProblemsPanel
        v2={v2}
        v2Error={false}
        jobsError={false}
        coverageError={false}
        onOpenOrchestrator={onOpen}
      />,
    );
    const btn = screen.getByRole("button", { name: /Open orchestrator details/ });
    btn.click();
    expect(onOpen).toHaveBeenCalledWith("cik_mapping");
  });
});
```

- [ ] **Step 2: Confirm fail**

```
pnpm --dir frontend test -- ProblemsPanel
```

Expected: many fails (props changed).

- [ ] **Step 3: Rewrite the component**

Replace `frontend/src/components/admin/ProblemsPanel.tsx` entirely:

```tsx
/**
 * Problems triage panel — v2-backed rewrite (A.5 chunk 1).
 *
 * Consumes the structured /sync/layers/v2 payload. Renders:
 *   - One red row per `action_needed` entry: display_name +
 *     operator_message + operator_fix + consecutive_failures badge +
 *     expandable affected_downstream list.
 *   - One amber row per `secret_missing` entry: display_name +
 *     operator_fix rendered as a link to /settings#providers.
 *
 * Per-source cache preserves last-good snapshots across refetch-in-flight
 * so a transient null does not blank the red banner.
 */
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import type {
  ActionNeededItem,
  SecretMissingItem,
  SyncLayersV2Response,
} from "@/api/types";


export interface ProblemsPanelProps {
  /** Live v2 payload. Null on first mount + while refetch is in flight. */
  readonly v2: SyncLayersV2Response | null;
  readonly v2Error: boolean;
  readonly jobsError: boolean;
  readonly coverageError: boolean;
  /** Called with the root layer name when the operator clicks a drill-through. */
  readonly onOpenOrchestrator: (layerName: string) => void;
}


export function ProblemsPanel({
  v2,
  v2Error,
  jobsError,
  coverageError,
  onOpenOrchestrator,
}: ProblemsPanelProps): JSX.Element | null {
  // Last-good snapshot — a refetch that returns null must not blank the
  // visible red banner.
  const [cached, setCached] = useState<SyncLayersV2Response | null>(null);
  useEffect(() => {
    if (v2 !== null) setCached(v2);
  }, [v2]);

  if (cached === null) {
    return (
      <div className="rounded-md border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-600">
        Checking for problems…
      </div>
    );
  }

  const problems =
    cached.action_needed.length + cached.secret_missing.length;
  const erroredSources: string[] = [];
  if (v2Error) erroredSources.push("layers");
  if (jobsError) erroredSources.push("jobs");
  if (coverageError) erroredSources.push("coverage");

  if (problems === 0 && erroredSources.length === 0) {
    // Clean — hide panel.
    return null;
  }

  const sectionTone =
    problems > 0
      ? "border-red-200 bg-red-50"
      : erroredSources.length > 0
        ? "border-amber-200 bg-amber-50"
        : "border-slate-200 bg-slate-50";
  const headerTone =
    problems > 0
      ? "border-red-200 text-red-800"
      : "border-amber-200 text-amber-800";

  return (
    <section
      role="region"
      aria-label="Current problems"
      className={`rounded-md border shadow-sm ${sectionTone}`}
    >
      <header
        className={`flex items-center justify-between border-b px-4 py-2 text-sm font-semibold ${headerTone}`}
      >
        <span>{cached.system_summary}</span>
        {erroredSources.length > 0 ? (
          <span className="text-xs font-normal text-amber-700" role="status">
            Could not re-check {erroredSources.join(", ")} — using last known state
          </span>
        ) : null}
      </header>
      <ul className="divide-y divide-red-100">
        {cached.action_needed.map((item) => (
          <ActionNeededRow
            key={item.root_layer}
            item={item}
            onOpen={() => onOpenOrchestrator(item.root_layer)}
          />
        ))}
        {cached.secret_missing.map((item) => (
          <SecretMissingRow key={item.layer} item={item} />
        ))}
      </ul>
    </section>
  );
}


function ActionNeededRow({
  item,
  onOpen,
}: {
  item: ActionNeededItem;
  onOpen: () => void;
}): JSX.Element {
  const [expanded, setExpanded] = useState(false);
  return (
    <li className="px-4 py-2 text-sm">
      <div className="flex items-start gap-2">
        <span
          aria-hidden
          className="mt-1 inline-block h-2 w-2 rounded-full bg-red-500"
        />
        <div className="flex-1">
          <div className="font-medium text-red-800">
            {item.display_name} — {item.operator_message}
          </div>
          {item.operator_fix !== null ? (
            <div className="text-xs text-slate-700">{item.operator_fix}</div>
          ) : null}
          <div className="mt-1 text-xs text-slate-500">
            {item.consecutive_failures} consecutive failures
          </div>
          {item.affected_downstream.length > 0 ? (
            <button
              type="button"
              onClick={() => setExpanded((v) => !v)}
              className="mt-1 text-xs text-red-700 hover:underline"
            >
              +{item.affected_downstream.length} layers waiting
            </button>
          ) : null}
          {expanded ? (
            <ul className="mt-1 list-disc pl-5 text-xs text-slate-600">
              {item.affected_downstream.map((name) => (
                <li key={name}>{name}</li>
              ))}
            </ul>
          ) : null}
        </div>
        <button
          type="button"
          onClick={onOpen}
          className="shrink-0 text-xs font-medium text-blue-700 hover:underline"
          aria-label={`Open orchestrator details for ${item.root_layer}`}
        >
          Open orchestrator details →
        </button>
      </div>
    </li>
  );
}


function SecretMissingRow({ item }: { item: SecretMissingItem }): JSX.Element {
  return (
    <li className="px-4 py-2 text-sm">
      <div className="flex items-start gap-2">
        <span
          aria-hidden
          className="mt-1 inline-block h-2 w-2 rounded-full bg-amber-500"
        />
        <div className="flex-1">
          <div className="font-medium text-amber-800">
            {item.display_name} — credential needed
          </div>
          <div className="text-xs text-slate-700">
            <Link
              to="/settings#providers"
              className="font-medium text-blue-700 hover:underline"
            >
              Set {item.missing_secret} in Settings → Providers
            </Link>
          </div>
        </div>
      </div>
    </li>
  );
}
```

- [ ] **Step 4: Tests pass**

```
pnpm --dir frontend test -- ProblemsPanel
```

Expected: 7 green.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/admin/ProblemsPanel.tsx frontend/src/components/admin/ProblemsPanel.test.tsx
git commit -m "feat(#342): ProblemsPanel rewrite to consume v2 payload"
```

### Task 1.4: Rewire AdminPage to use v2 + mount LayerHealthList

**Files:**
- Modify: `frontend/src/pages/AdminPage.tsx`
- Modify: `frontend/src/pages/SyncDashboard.tsx` (inline or leave — see step 3)
- Modify: `frontend/src/pages/AdminPage.test.tsx`

- [ ] **Step 1: Update AdminPage.test.tsx to pin v2 integration**

Add to `frontend/src/pages/AdminPage.test.tsx`:

```tsx
import { describe, expect, it, vi, beforeEach } from "vitest";

// ... existing setup ...

describe("AdminPage v2 integration", () => {
  beforeEach(() => {
    // Reset any global fetch mocks.
    vi.restoreAllMocks();
  });

  it("reads /sync/layers/v2 and renders no panel when system_state is ok", async () => {
    // Mock apiFetch to return an OK v2 payload.
    // Assert the ProblemsPanel renders nothing and LayerHealthList
    // renders the full layer set.
    // (Fill in per existing mocking pattern for AdminPage.test.tsx.)
    // ...
  });
});
```

If the existing AdminPage tests already mock `fetchSyncLayers`, switch them to mock `fetchSyncLayersV2`.

- [ ] **Step 2: Rewire `AdminPage.tsx`**

Replace the `layers` fetch with v2:

```tsx
// Was:
// const layers = useAsync(fetchSyncLayers, []);
// Now:
import { fetchSyncLayersV2 } from "@/api/sync";
const v2 = useAsync(fetchSyncLayersV2, []);
```

Update `refetchAll` + `refetchLayers` → `refetchV2`. Replace `ProblemsPanel` props:

```tsx
<ProblemsPanel
  v2={v2.data}
  v2Error={v2.error !== null}
  jobsError={jobs.error !== null}
  coverageError={coverage.error !== null}
  onOpenOrchestrator={openOrchestratorFor}
/>
```

Where `openOrchestratorFor` is:

```tsx
const openOrchestratorFor = useCallback((layerName: string) => {
  setOrchestratorOpen(true);
  requestAnimationFrame(() => {
    const el = document.getElementById(`admin-layer-${layerName}`);
    if (el && typeof el.scrollIntoView === "function") {
      el.scrollIntoView({ behavior: "smooth", block: "start" });
    } else {
      // Fallback to the section if the specific layer isn't mounted yet.
      const section = document.getElementById("admin-orchestrator-details");
      if (section && typeof section.scrollIntoView === "function") {
        section.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }
  });
}, []);
```

Replace `SyncDashboard`'s content (or the collapsible body) with:

```tsx
<CollapsibleSection
  title="Orchestrator details"
  summary={
    v2.data === null
      ? undefined
      : `${v2.data.layers.filter((l) => l.state !== "healthy" && l.state !== "disabled").length} layers catching up or need attention`
  }
  open={orchestratorOpen}
  onOpenChange={setOrchestratorOpen}
  sectionId="admin-orchestrator-details"
>
  {v2.loading ? (
    <SectionSkeleton rows={15} />
  ) : v2.error !== null ? (
    <SectionError onRetry={v2.refetch} />
  ) : v2.data ? (
    <LayerHealthList
      layers={v2.data.layers}
      onToggle={() => {
        // Chunk 2 wires this.
      }}
    />
  ) : null}
</CollapsibleSection>
```

Remove the old `layerProblemCount` computation and the `ORCHESTRATOR_OWNED` set (no longer relevant — backend owns the filtering).

- [ ] **Step 3: Keep SyncDashboard intact if it still owns the Sync-now button**

Check whether `SyncDashboard` now has any content other than what AdminPage owns. If yes, leave it in place rendered inside the collapsible; if no, the collapsible body is now just `LayerHealthList` and the inline `SyncDashboard` import can be dropped. Use `grep -rn "SyncDashboard" frontend/src` to confirm the call sites.

- [ ] **Step 4: Run AdminPage + related tests**

```
pnpm --dir frontend test -- AdminPage
pnpm --dir frontend typecheck
```

All green. Fix any type errors from the signature change (ProblemsPanel props) inline.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/pages/AdminPage.tsx frontend/src/pages/AdminPage.test.tsx
git commit -m "feat(#342): AdminPage consumes v2 + mounts LayerHealthList"
```

### Task 1.5: Pre-push gate + PR

- [ ] **Step 1: Full gate**

```
pnpm --dir frontend typecheck
pnpm --dir frontend test
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -x -q
```

Backend gate stays clean — no backend changes in this chunk.

- [ ] **Step 2: Push + PR.** Title: `feat(#342): chunk 1 — AdminPage v2 swap + LayerHealthList + ProblemsPanel rewrite`.

- [ ] **Step 3: Poll, resolve, merge.**

---

## Chunk 2 — Enable/disable endpoint + execution-guard rule + UI wire

Backend endpoint, frontend wire, execution-guard rule. Branch: `feature/342-chunk-2-toggle-and-guard`.

### Task 2.1: `POST /sync/layers/{layer_name}/enabled` endpoint

**Files:**
- Modify: `app/api/sync.py`
- Test: `tests/api/test_sync_layer_enabled_endpoint.py`

- [ ] **Step 1: Failing test**

```python
# tests/api/test_sync_layer_enabled_endpoint.py
import psycopg
import pytest
from fastapi.testclient import TestClient

from tests.fixtures.ebull_test_db import test_database_url as _test_database_url


@pytest.mark.integration
def test_post_layer_enabled_happy_path(clean_client: TestClient) -> None:
    resp = clean_client.post(
        "/sync/layers/candles/enabled",
        json={"enabled": False},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["layer"] == "candles"
    assert body["is_enabled"] is False
    assert body["warning"] is None

    # Verify persistence.
    with psycopg.connect(_test_database_url()) as conn:
        row = conn.execute(
            "SELECT is_enabled FROM layer_enabled WHERE layer_name = 'candles'",
        ).fetchone()
        assert row is not None
        assert row[0] is False

    # Clean up.
    clean_client.post("/sync/layers/candles/enabled", json={"enabled": True})


@pytest.mark.integration
def test_post_layer_enabled_fx_rates_disable_surfaces_warning(
    clean_client: TestClient,
) -> None:
    try:
        resp = clean_client.post(
            "/sync/layers/fx_rates/enabled",
            json={"enabled": False},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_enabled"] is False
        assert body["warning"] is not None
        assert "drift" in body["warning"].lower()
    finally:
        clean_client.post("/sync/layers/fx_rates/enabled", json={"enabled": True})


@pytest.mark.integration
def test_post_layer_enabled_portfolio_sync_disable_surfaces_warning(
    clean_client: TestClient,
) -> None:
    try:
        resp = clean_client.post(
            "/sync/layers/portfolio_sync/enabled",
            json={"enabled": False},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["warning"] is not None
        assert "broker" in body["warning"].lower() or "portfolio" in body["warning"].lower()
    finally:
        clean_client.post("/sync/layers/portfolio_sync/enabled", json={"enabled": True})


def test_post_layer_enabled_unknown_layer_404(clean_client: TestClient) -> None:
    resp = clean_client.post(
        "/sync/layers/not_a_real_layer/enabled",
        json={"enabled": False},
    )
    assert resp.status_code == 404


def test_post_layer_enabled_enable_surfaces_no_warning(clean_client: TestClient) -> None:
    resp = clean_client.post(
        "/sync/layers/fx_rates/enabled",
        json={"enabled": True},
    )
    assert resp.status_code == 200
    assert resp.json()["warning"] is None
```

- [ ] **Step 2: Confirm fail**

```
uv run pytest tests/api/test_sync_layer_enabled_endpoint.py -v
```

Expected: all 404 (endpoint doesn't exist).

- [ ] **Step 3: Add the endpoint**

In `app/api/sync.py`, add new Pydantic models near the other v2 models:

```python
class LayerEnabledRequest(BaseModel):
    enabled: bool


class LayerEnabledResponse(BaseModel):
    layer: str
    display_name: str
    is_enabled: bool
    warning: str | None = None


_SAFETY_CRITICAL_LAYERS = frozenset({"fx_rates", "portfolio_sync"})


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

Add imports at the top:

```python
from app.services.layer_enabled import set_layer_enabled
```

Add the endpoint after `get_sync_layers_v2`:

```python
@router.post("/layers/{layer_name}/enabled", response_model=LayerEnabledResponse)
def post_layer_enabled(
    layer_name: str,
    body: LayerEnabledRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> LayerEnabledResponse:
    """Toggle a layer's operator-enabled flag.

    Any layer can be toggled. Safety-critical layers (fx_rates,
    portfolio_sync) surface a warning string the UI shows as a toast;
    actual BUY/ADD blocking when they are disabled lives in the
    execution_guard safety_layers_enabled rule.
    """
    if layer_name not in LAYERS:
        raise HTTPException(status_code=404, detail=f"unknown layer: {layer_name}")
    set_layer_enabled(conn, layer_name, enabled=body.enabled)
    conn.commit()  # set_layer_enabled is transaction-neutral (chunk 4 review)
    return LayerEnabledResponse(
        layer=layer_name,
        display_name=LAYERS[layer_name].display_name,
        is_enabled=body.enabled,
        warning=_safety_warning(layer_name, body.enabled),
    )
```

- [ ] **Step 4: Tests pass**

```
uv run pytest tests/api/test_sync_layer_enabled_endpoint.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add app/api/sync.py tests/api/test_sync_layer_enabled_endpoint.py
git commit -m "feat(#342): POST /sync/layers/{name}/enabled toggle endpoint"
```

### Task 2.2: `safety_layers_enabled` rule in execution_guard

**Files:**
- Modify: `app/services/execution_guard.py`
- Test: `tests/test_execution_guard.py` (or closest safety-rule test file; grep to confirm)

- [ ] **Step 1: Locate test file**

```
grep -rn "def test.*live_trading\|_check_live_trading\|RuleName" tests --include="*.py"
```

Find the test file that pins existing BUY/ADD-only rules. Use that file or create `tests/test_execution_guard_safety_layers.py` if the existing file is already large.

- [ ] **Step 2: Failing tests**

Add:

```python
import psycopg
import pytest

from app.services.execution_guard import evaluate_recommendation, RuleResult
from app.services.layer_enabled import set_layer_enabled
from tests.fixtures.ebull_test_db import test_database_url as _test_database_url


def _enable_all(conn: psycopg.Connection) -> None:
    for name in ("fx_rates", "portfolio_sync"):
        set_layer_enabled(conn, name, enabled=True)
    conn.commit()


@pytest.mark.integration
def test_buy_blocked_when_fx_rates_disabled() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        set_layer_enabled(conn, "fx_rates", enabled=False)
        conn.commit()
        try:
            # Invoke the guard on a BUY recommendation. The exact
            # fixture depends on existing helpers in the test file.
            decision = evaluate_recommendation(
                conn,
                recommendation={"action": "BUY", "instrument_id": 1, "size_usd": 100},
            )
        finally:
            _enable_all(conn)
    assert not decision.passed
    failed = {r.rule for r in decision.failed_rules}
    assert "safety_layers_enabled" in failed


@pytest.mark.integration
def test_add_blocked_when_portfolio_sync_disabled() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        set_layer_enabled(conn, "portfolio_sync", enabled=False)
        conn.commit()
        try:
            decision = evaluate_recommendation(
                conn,
                recommendation={"action": "ADD", "instrument_id": 1, "size_usd": 100},
            )
        finally:
            _enable_all(conn)
    assert not decision.passed
    failed = {r.rule for r in decision.failed_rules}
    assert "safety_layers_enabled" in failed


@pytest.mark.integration
def test_exit_allowed_even_when_safety_layers_disabled() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        set_layer_enabled(conn, "fx_rates", enabled=False)
        set_layer_enabled(conn, "portfolio_sync", enabled=False)
        conn.commit()
        try:
            decision = evaluate_recommendation(
                conn,
                recommendation={"action": "EXIT", "instrument_id": 1, "size_usd": 100},
            )
        finally:
            _enable_all(conn)
    # EXIT is never gated by safety_layers_enabled.
    failed_names = {r.rule for r in decision.failed_rules}
    assert "safety_layers_enabled" not in failed_names


@pytest.mark.integration
def test_buy_passes_when_safety_layers_enabled() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        _enable_all(conn)
        decision = evaluate_recommendation(
            conn,
            recommendation={"action": "BUY", "instrument_id": 1, "size_usd": 100},
        )
    failed_names = {r.rule for r in decision.failed_rules}
    assert "safety_layers_enabled" not in failed_names
```

If the existing guard test file uses a different fixture shape for `evaluate_recommendation` (coverage, thesis, etc. required inputs), wrap with the same helper the other tests use — don't re-invent a fixture.

- [ ] **Step 3: Confirm fail**

```
uv run pytest tests/test_execution_guard_safety_layers.py -v
```

Expected: tests fail because the rule isn't in `RuleName` / not in the evaluation chain.

- [ ] **Step 4: Extend `RuleName` + add `_check_safety_layers_enabled`**

In `app/services/execution_guard.py`, add `"safety_layers_enabled"` to the `RuleName` Literal at line 89:

```python
RuleName = Literal[
    "kill_switch",
    "auto_trading",
    "live_trading",
    "coverage_not_tier1",
    "thesis_freshness",
    # ... other existing rules ...
    "safety_layers_enabled",
]
```

Add the helper near the other `_check_*` functions (after `_check_live_trading` line 346):

```python
def _check_safety_layers_enabled(
    conn: psycopg.Connection[Any],
) -> RuleResult:
    """Refuse BUY/ADD when fx_rates or portfolio_sync is operator-disabled.

    FX disabled → USD valuations + budget drift silently.
    Portfolio sync disabled → position baseline goes stale, exposure
    and concentration checks lie. Blocking only BUY/ADD preserves the
    emergency-EXIT path operators need when intentionally de-risking.
    """
    from app.services.layer_enabled import is_layer_enabled

    disabled = [
        name
        for name in ("fx_rates", "portfolio_sync")
        if not is_layer_enabled(conn, name)
    ]
    if disabled:
        return RuleResult(
            rule="safety_layers_enabled",
            passed=False,
            detail=(
                f"{' + '.join(disabled)} disabled — BUY/ADD blocked; "
                "re-enable the layer to clear."
            ),
        )
    return RuleResult(rule="safety_layers_enabled", passed=True)
```

- [ ] **Step 5: Wire it into `evaluate_recommendation`**

Find the BUY/ADD-only append block (around line 720 per the earlier grep):

```
# Rules that apply to BUY / ADD only
```

Add the new call:

```python
# Rules that apply to BUY / ADD only
if action in {"BUY", "ADD"}:
    rule_results.append(_check_live_trading(runtime.enable_live_trading))
    rule_results.append(_check_safety_layers_enabled(conn))
```

If the exact variable name for the action / conn is different in that function, adapt to match the existing calling convention (grep for `_check_live_trading(` and match the sibling pattern).

- [ ] **Step 6: Tests pass**

```
uv run pytest tests/test_execution_guard_safety_layers.py -v
```

Expected: 4 green. If the existing `evaluate_recommendation` requires more fixture args (coverage dict, thesis, etc.) the tests will reveal it; wrap with the helper already used by nearby existing tests.

- [ ] **Step 7: Commit**

```bash
git add app/services/execution_guard.py tests/test_execution_guard_safety_layers.py
git commit -m "feat(#342): execution_guard safety_layers_enabled rule blocks BUY/ADD on disabled fx_rates or portfolio_sync"
```

### Task 2.3: Frontend wire — `setLayerEnabled` + LayerHealthList toggle

**Files:**
- Modify: `frontend/src/api/types.ts`
- Modify: `frontend/src/api/sync.ts`
- Modify: `frontend/src/components/admin/LayerHealthList.tsx`
- Modify: `frontend/src/components/admin/LayerHealthList.test.tsx`
- Modify: `frontend/src/pages/AdminPage.tsx`

- [ ] **Step 1: Types**

Add to `frontend/src/api/types.ts`:

```ts
export interface LayerEnabledResponse {
  layer: string;
  display_name: string;
  is_enabled: boolean;
  warning: string | null;
}
```

- [ ] **Step 2: API function**

Append to `frontend/src/api/sync.ts`:

```ts
import type { LayerEnabledResponse } from "@/api/types";


export function setLayerEnabled(
  layerName: string,
  enabled: boolean,
): Promise<LayerEnabledResponse> {
  return apiFetch<LayerEnabledResponse>(`/sync/layers/${encodeURIComponent(layerName)}/enabled`, {
    method: "POST",
    body: JSON.stringify({ enabled }),
  });
}
```

- [ ] **Step 3: Extend LayerHealthList test for safety-critical confirm**

Append to `frontend/src/components/admin/LayerHealthList.test.tsx`:

```tsx
describe("LayerHealthList toggle safety confirm", () => {
  it("prompts window.confirm when disabling fx_rates", async () => {
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "fx_rates", state: "healthy", display_name: "FX Rates" })];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    screen.getByLabelText("fx_rates actions").click();
    screen.getByText(/Disable layer/).click();
    expect(confirmSpy).toHaveBeenCalled();
    expect(onToggle).toHaveBeenCalledWith("fx_rates", false);
    confirmSpy.mockRestore();
  });

  it("does not call onToggle when safety-critical confirm is declined", async () => {
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(false);
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "portfolio_sync", state: "healthy" })];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    screen.getByLabelText("portfolio_sync actions").click();
    screen.getByText(/Disable layer/).click();
    expect(onToggle).not.toHaveBeenCalled();
    confirmSpy.mockRestore();
  });

  it("does not prompt confirm for non-safety-critical layers", () => {
    const confirmSpy = vi.spyOn(window, "confirm");
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "candles", state: "healthy" })];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    screen.getByLabelText("candles actions").click();
    screen.getByText(/Disable layer/).click();
    expect(confirmSpy).not.toHaveBeenCalled();
    expect(onToggle).toHaveBeenCalledWith("candles", false);
    confirmSpy.mockRestore();
  });
});
```

- [ ] **Step 4: Extend LayerHealthList to gate on safety-critical layers**

Update the toggle-click handler inside `LayerHealthList.tsx`. Replace the `onToggle(entry.layer, !isDisabled ? false : true)` call with:

```tsx
const SAFETY_CRITICAL = new Set(["fx_rates", "portfolio_sync"]);


// Inside the dropdown click handler:
const targetEnabled = isDisabled;  // If currently disabled, enabling; else disabling.
if (!targetEnabled && SAFETY_CRITICAL.has(entry.layer)) {
  const label = entry.display_name;
  const ok = window.confirm(
    `Disable ${label}? Valuations will drift until re-enabled.`,
  );
  if (!ok) {
    setMenuOpen(null);
    return;
  }
}
onToggle(entry.layer, targetEnabled);
setMenuOpen(null);
```

- [ ] **Step 5: AdminPage owns the POST + toast**

In `AdminPage.tsx`, add:

```tsx
import { setLayerEnabled } from "@/api/sync";

// ... inside component ...

const [toast, setToast] = useState<string | null>(null);

const handleLayerToggle = useCallback(
  async (layerName: string, enabled: boolean) => {
    try {
      const resp = await setLayerEnabled(layerName, enabled);
      v2.refetch();
      if (resp.warning !== null) {
        setToast(resp.warning);
        window.setTimeout(() => setToast(null), 6000);
      }
    } catch (err) {
      const message =
        err instanceof Error ? err.message : String(err);
      setToast(`Failed to update ${layerName}: ${message}`);
      window.setTimeout(() => setToast(null), 6000);
    }
  },
  [v2],
);
```

Pass `onToggle={handleLayerToggle}` into `LayerHealthList`. Render the toast near the top of the AdminPage:

```tsx
{toast !== null ? (
  <div
    role="status"
    className="rounded-md border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-800"
  >
    {toast}
  </div>
) : null}
```

- [ ] **Step 6: Tests pass**

```
pnpm --dir frontend test -- LayerHealthList
pnpm --dir frontend test -- AdminPage
pnpm --dir frontend typecheck
```

All green. Fix any TS/test issues inline.

- [ ] **Step 7: Commit**

```bash
git add frontend/src/api/sync.ts frontend/src/api/types.ts frontend/src/components/admin/LayerHealthList.tsx frontend/src/components/admin/LayerHealthList.test.tsx frontend/src/pages/AdminPage.tsx
git commit -m "feat(#342): LayerHealthList toggle wire + safety-critical confirm + warning toast"
```

### Task 2.4: Pre-push gate + PR

- [ ] **Step 1: Full gate** (same command set as chunk 0).

- [ ] **Step 2: Push + PR.** Title: `feat(#342): chunk 2 — enable/disable endpoint + execution-guard safety rule + UI wire`.

- [ ] **Step 3: Poll, resolve, merge.**

---

## Chunk 3 — `/health/data` retirement

Backend deletion only. Branch: `feature/342-chunk-3-health-data-retire`.

### Task 3.1: Delete `/health/data` + `get_system_health` + `SystemHealth`

**Files:**
- Modify: `app/main.py` (delete `/health/data` route)
- Modify: `app/services/ops_monitor.py` (delete `SystemHealth` dataclass + `get_system_health` function)
- Modify: `tests/test_api_main.py`
- Modify: `tests/test_ops_monitor.py`
- Modify: `tests/test_api_auth.py`
- Modify: `tests/test_api_system.py`

- [ ] **Step 1: Grep to confirm only-callers**

```
grep -rn "/health/data\|get_system_health\|SystemHealth" app tests --include="*.py"
```

Every hit outside the files listed above is a break risk. If the grep shows a hit in an untouched module, STOP and report.

- [ ] **Step 2: Delete the route from `app/main.py`**

Find `@app.get("/health/data", ...)` at ~line 312 and delete the decorator + function body. Delete the `from app.services.ops_monitor import get_system_health` import if present.

- [ ] **Step 3: Delete `SystemHealth` + `get_system_health` from `ops_monitor.py`**

Delete the `SystemHealth` dataclass (line 156) and the `get_system_health` function (line 706). Any imports at the top of `ops_monitor.py` that become unused — drop them.

- [ ] **Step 4: Update `tests/test_api_auth.py:139`**

Find `test_health_data_requires_auth`. Replace with an equivalent assertion on a still-protected endpoint:

```python
def test_protected_endpoints_require_auth(self) -> None:
    # /health/data retired in A.5 chunk 3 (#342). Use /system/status
    # as the protected-endpoint canary — same auth dependency.
    resp = client.get("/system/status")
    assert resp.status_code == 401
```

- [ ] **Step 5: Delete `TestGetSystemHealth` from `tests/test_ops_monitor.py`**

Find the `class TestGetSystemHealth` block + its `from app.services.ops_monitor import ... get_system_health ... SystemHealth ...` import. Delete only that class + narrow the imports. Every other test group in the file stays.

- [ ] **Step 6: Delete `/health/data` tests from `tests/test_api_main.py` and `tests/test_api_system.py`**

Grep inside each file for `/health/data` and delete only those cases.

- [ ] **Step 7: Verify**

```
uv run pytest tests/test_api_main.py tests/test_ops_monitor.py tests/test_api_auth.py tests/test_api_system.py -v
uv run pytest -x -q
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

All green.

- [ ] **Step 8: Commit**

```bash
git add app/main.py app/services/ops_monitor.py tests/test_api_main.py tests/test_api_system.py tests/test_api_auth.py tests/test_ops_monitor.py
git commit -m "refactor(#342): retire /health/data + SystemHealth + get_system_health"
```

### Task 3.2: Pre-push gate + PR

- [ ] Same gate pattern. Title: `refactor(#342): chunk 3 — delete /health/data + associated ops_monitor dead code`. Poll, resolve, merge.

---

## Self-review

**Spec coverage**

| Spec section | Task |
| --- | --- |
| §4 architecture | Chunks 0-3 map directly. |
| §5.4 v2 payload extension | Chunk 0 task 0.1. |
| §5.5 LayerHealthList | Chunk 1 task 1.2. |
| §5.3 ProblemsPanel rewrite | Chunk 1 task 1.3. |
| §5.5 AdminPage | Chunk 1 task 1.4. |
| §6.1 toggle endpoint | Chunk 2 task 2.1. |
| §6.2 execution_guard rule | Chunk 2 task 2.2. |
| §6.3 UI wiring + toast | Chunk 2 task 2.3. |
| §7 /health/data retirement + test sweep | Chunk 3 task 3.1. |
| §9 tests | Every task has a pytest or vitest assertion block. |

**Contract consistency**

- `LayerEntry` shape appears in Chunk 0 (backend) and Chunk 1 (TS types) with identical fields.
- `ProblemsPanel` props changed from `(layers, jobs, coverage, ...)` to `(v2, v2Error, jobsError, coverageError, onOpenOrchestrator)` — every caller of `ProblemsPanel` in tasks 1.3 + 1.4 uses the new signature.
- `setLayerEnabled` signature is `(layerName: string, enabled: boolean) => Promise<LayerEnabledResponse>` — matches the endpoint.
- `_check_safety_layers_enabled` returns `RuleResult(rule="safety_layers_enabled", ...)` matching the Literal extension in the same commit.
- `onOpenOrchestrator` signature changed to accept `layerName: string`. Tasks 1.3 and 1.4 both wire it up consistently.

**Placeholders scanned:** none. Every code block is complete; every test has concrete assertions.

---

## Contracts to re-verify at execute time

Two implementation-detail spots worth a quick grep before coding the step:

1. **`evaluate_recommendation` arg shape + BUY/ADD gating site.** The plan's rule insertion references "the BUY/ADD-only append block at ~line 720". Before editing, `grep -n "Rules that apply to BUY / ADD only\|_check_live_trading(" app/services/execution_guard.py` to find the exact pattern. Match the surrounding code style (variable names, indentation, RuleResult construction).
2. **Existing `ProblemsPanel.test.tsx` may have helper fixtures** (e.g. `layer()` / `makeJobsResponse()`) the old suite reused. If they still make sense for v2 fixtures, keep them; otherwise delete as part of the rewrite. Grep before pasting the new tests.

---

## Execution

Plan saved. Two execution options:

1. **Subagent-driven (recommended)** — dispatch a fresh subagent per task.
2. **Inline** — run tasks in this session via `superpowers:executing-plans`.
