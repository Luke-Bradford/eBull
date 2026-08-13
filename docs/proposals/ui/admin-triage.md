# AdminPage triage rewrite — design spec

**Issue:** [#323](https://github.com/Luke-Bradford/eBull/issues/323)
**Size:** M (~1-2 days, frontend + narrow backend additions — see §3a)
**Depends on:** nothing (uses existing `/sync/layers`, `/sync/status`, `/sync/runs`, `/coverage/summary`, `/system/jobs`, `/recommendations` endpoints).

---

## 1. Goal

Redesign `AdminPage` so the operator can answer three questions in order, without scrolling or decoding the current flat 15-card grid:

1. **What is broken right now?** — failing layers, stuck layers, jobs with consecutive failures, coverage anomalies.
2. **What does the fund data actually look like?** — compact stat cards for the data available today (universe size, analysable coverage, needs-review bucket, latest-recommendation freshness). Cells that depend on endpoints we do not yet have (tier distribution, score/thesis summaries) render as `"–" (pending)` placeholders and are tracked in §14.
3. **What is the orchestrator doing long-term?** — current layer grid + recent sync runs + background jobs, but **collapsed by default**.

Operator should see "good / bad / running" in <2 seconds, drill down only when needed.

## 2. Diagnostic findings that motivate the design

From 2026-04-19 audit (`data/raw/` cleanup session):

- `cik_mapping` failed 3× in 7d with `db_constraint` error_category. **Invisible on current page.**
- `universe` + `candles` haven't run in 2 days. **Current dashboard shows them "stale" but buried next to 13 healthy cards.**
- `thesis` layer: 0 rows ever; `prereq_skip` on every run. **Dashboard shows a freshness detail but nothing that signals "this pillar of the product has never worked".**
- 5 trade_recommendations total, all rejected. **Dashboard doesn't mention recommendations at all.**
- 5 of 12,362 instruments on Tier 1 (tradable universe). **Dashboard has coverage cells but no tier-1/tier-2/tier-3 count.**
- `raw_persistence_state` empty — Plan A compaction never ran. **Out of scope for this PR (no existing endpoint surfaces this); note for follow-up.**

## 3. Non-goals (pin)

- **No new backend endpoints.** (Small backend additions to existing endpoint response objects are permitted — see §3a.)
- **No raw-storage visibility** (`data/raw/` disk usage, `raw_persistence_state` rows). Both require a new backend surface. Track as tech-debt (§14).
- **No notifications / alerting / toast system.**
- **No preservation of current top-level layout.** The Sync-dashboard-first framing is the exact source of the noise; it flips.
- **No new tier-1 / tier-2 breakdown endpoint.** `/coverage/summary` already exposes `analysable`, `insufficient`, etc., but not coverage_tier counts. We surface what exists and flag the tier distribution as a follow-up (§14).
- **No rewrite of `SyncDashboard`'s internals.** The 15-card grid, recent-runs table, and layer-card internals stay as-is. The ONE change is that `SyncDashboard` consumes a shared `useSyncTrigger` via a required prop (§11) instead of owning its own trigger state — so the top-level and inner buttons cannot race.
- **No mutation of background-jobs UX** — the existing JobsTable + Run-Now buttons stay verbatim, just hidden by default.

## 3a. In-scope backend additions

`/sync/layers` currently hardcodes `consecutive_failures: 0` and only sets `last_error_category` on freshness-predicate exceptions (`app/api/sync.py:219-220`). Both fields exist on the response model but are lies against live data. The Problems panel depends on them being truthful.

Narrow backend work shipped with this PR:

- Populate `SyncLayer.consecutive_failures` = number of consecutive `status='failed'` rows for that `layer_name` in `sync_layer_progress`, ordered by `started_at DESC`, stopping at the first non-failed row (or zero rows).
- Populate `SyncLayer.last_error_category` = `error_category` from the most recent `sync_layer_progress` row where that column is non-null for the layer, regardless of status (so "last known error" survives a later skipped/partial run).

Schema: both columns already exist on `sync_layer_progress`; no migration. Service-layer change only — `app/services/sync_orchestrator/freshness.py` (or wherever `build_layer_response` lives). Response shape of `/sync/layers` is unchanged; only the values become accurate.

Backend tests:

- Unit test for the consecutive-failures counter (happy path, interrupted-by-success resets to 0, zero-rows returns 0).
- Unit test for the last-error-category lookup (returns most-recent non-null, null when no history).

## 4. Layout

```
┌─────────────────────────────────────────────────────────────┐
│ Problems panel (hidden when empty)                          │
│  - One row per problem                                       │
│  - Red for blocking, amber for warning                       │
│  - Click-through expands each row or jumps to details        │
├─────────────────────────────────────────────────────────────┤
│ Fund data at a glance                                        │
│  [tradable] [analysable] [needs review] [latest rec] [tier1/2/3 pending] [scores pending] ... │
│  — always visible, one row of stat cards                     │
├─────────────────────────────────────────────────────────────┤
│ Orchestrator details   [▸]        (collapsed by default)     │
│   On expand: current SyncDashboard unchanged inside          │
├─────────────────────────────────────────────────────────────┤
│ Background tasks       [▸]        (collapsed by default)     │
│   On expand: current JobsTable unchanged inside              │
├─────────────────────────────────────────────────────────────┤
│ Filings coverage       [▸]        (collapsed by default)     │
│   On expand: current CoverageSummaryCard unchanged           │
└─────────────────────────────────────────────────────────────┘
```

**Sync-now button stays visible at the top** (outside the collapsible) — triggering a sync is a top-level verb.

## 5. Problems panel

### Inputs (all from already-loaded data)

From `/sync/layers`:
- `layer.last_error_category !== null` AND `layer.consecutive_failures >= 1` → **blocking problem** (red).
- `layer.is_fresh === false` AND `layer.is_blocking` → **stale-blocking problem** (amber unless already red above).
- `layer.is_fresh === false` AND NOT `layer.is_blocking` → **stale-non-blocking problem** (amber, low-priority).

From `/system/jobs`:
- `job.last_status === "failure"` → **job-failure problem** (red).
- `job.last_status === "skipped"` is **NOT a problem** by default — `retry_deferred_recommendations` and `execute_approved_orders` skip routinely with "no work to do" reasons. This is the single biggest current noise source; we deliberately filter it out.

From `/coverage/summary`:
- `summary.null_rows > 0` → **data-audit problem** (amber; same convention as the existing CoverageSummaryCard which already renders null_rows red).

### Rendering

Each row shows:
- Icon / tone
- One-line title (`"cik_mapping layer — 3 consecutive failures (db_constraint)"`)
- Secondary line: last-success timestamp, action link (e.g. "Open orchestrator details" — scrolls + expands the orchestrator section).

Empty state: panel is not rendered at all. No "No problems!" banner. Empty = good.

### What the panel is NOT

- Not a running tally of every warning in the system. Only current problems, not history.
- Not an inbox — no dismissal, no snooze. Fix the underlying problem and the row disappears on the next refresh.
- Not a notifications feed — no timestamps older than the last successful run.

## 6. Fund data at a glance

Stat cards, single row at ≥sm. Two rows at xs.

| Stat | Source | Format |
|---|---|---|
| Tradable universe | `coverage.total_tradable` | count |
| Analysable | `coverage.analysable` | count + % |
| Needs review | `coverage.insufficient + coverage.structurally_young` | count + % |
| Recommendations | `/recommendations?limit=1` | freshness only (count is post-HOLD-dedupe; not meaningful) |
| Tier 1/2/3 | pending (§14 tech-debt) | placeholder "–" |
| Scores | pending (§14 tech-debt) | placeholder "–" |
| Theses | pending (§14 tech-debt) | placeholder "–" |

**Constraint:** several of the stat cells above need backend data we do not currently expose. In-scope for this PR:

- Tradable universe — already present as `coverage.total_tradable`.
- Analysable — already present as `coverage.analysable`.
- "Needs review" bucket — already present as `coverage.insufficient + coverage.structurally_young`.
- Recommendations **freshness** — `fetchRecommendations({action:null,status:null,instrument_id:null}, 0, 1)` returns `items[0].created_at`. We render only the freshness timestamp from this endpoint, NOT a total count — the endpoint's `total` field is post-HOLD-dedupe (see `app/api/recommendations.py:177`), which is not a meaningful "how many recommendations exist" number. If items is empty, render "never".

**Out of scope (rendered as placeholder "–" with a small tooltip "data pending"):**

- Tier 1/2/3 count — coverage summary lacks this; surface as tech-debt.
- Raw trade-recommendation count — HOLD-deduped endpoint is not fit for purpose; tech-debt.
- Latest scores count + freshness — no summary endpoint; tech-debt.
- Latest theses count + freshness — same; tech-debt.

The fund-data row still adds value with 4 live cells (tradable, analysable, needs-review, latest recommendation time) even before we fill the rest.

## 7. Collapsible sections

Each has `useState<boolean>(false)` for open state. On `true`, render the existing component with its internal layout preserved. Only `SyncDashboard` carries the prop-level refactor for the shared trigger (§11); its rendered grid / runs table stay identical.

- **Orchestrator details** → `<SyncDashboard syncTrigger={trigger} />`. Grid + recent-runs unchanged. The Problems panel's "Open orchestrator details" link auto-expands this section and scrolls to it.
- **Background tasks** → `<JobsTable />` unchanged, plus the intro sentence and Run-Now wiring. Keep `ORCHESTRATOR_OWNED` filter.
- **Filings coverage** → `<CoverageSummaryCard />` unchanged.

Click-to-expand header: chevron + title + secondary stat (e.g. "Orchestrator details — 1 problem"). Makes the summary informative even without expanding.

## 8. State + auto-refresh

AdminPage itself owns a top-level auto-refresh loop. Rationale: `SyncDashboard` is collapsed by default (§7), so its internal `setInterval` does not run when the section is unmounted — the Problems panel would go stale without its own driver. We must not depend on `SyncDashboard` being mounted.

Top-level fetches owned by AdminPage:

- `fetchSyncLayers()` — drives Problems panel + orchestrator-details collapsible header summary.
- `fetchSyncStatus()` — drives the top-level "running" banner + Sync-now button state.
- `fetchCoverageSummary()` — drives fund-data cells + CoverageSummaryCard when expanded.
- `fetchJobsOverview()` — drives Problems panel job rows + JobsTable when expanded.
- `fetchRecommendations({action:null,status:null,instrument_id:null}, 0, 1)` — drives Fund-data "latest recommendation" freshness cell. (Count is NOT used — see §6 revision.)

Each fetch uses `useAsync` independently (per `async-data-loading.md`). AdminPage starts a `setInterval` with cadence: 10s when `status.data?.is_running === true`, 60s otherwise. Matches the prior `SyncDashboard` contract.

`SyncDashboard` keeps its own internal fetches (for its own grid + runs table). When it mounts, both AdminPage and the inner dashboard are hitting `/sync/layers` and `/sync/status` — acceptable duplication matching the `/config` pattern; shared cache is tech-debt #320.

### ProblemsPanel render contract during loading / error / refetch

`useAsync` clears `data` to `null` at refetch start (prevention #93 / async-data-loading.md). The Problems panel depends on THREE independent sources (`/sync/layers`, `/system/jobs`, `/coverage/summary`). A naive "combine live values, render nothing if any is null" would drop an entire source's problems the moment that source refetches.

Apply the `safety-state-ui.md` cached-snapshot pattern **per source**, then combine at render time. The cache is NOT a single `Problem[]` — it is a per-source struct:

```ts
interface ProblemSources {
  readonly layers: Problem[] | null;   // null = never resolved
  readonly jobs: Problem[] | null;
  readonly coverage: Problem[] | null;
}
```

Contract for each source independently:

- On a non-null fresh response for source `X`, replace `cache.X` with the newly-derived `Problem[]` for that source.
- On a null fresh value (loading or error), LEAVE `cache.X` unchanged — do not overwrite with `null`.
- Render `cache.layers ∪ cache.jobs ∪ cache.coverage`, filtering out any source that has never resolved (value still `null`).

Resulting behaviour:

- **First mount (all caches `null`)**: render one-line neutral banner `"Checking for problems…"`. Panel is NOT hidden and not rendered empty.
- **After first source resolves**: render whatever problems it surfaced. Neutral banner persists with `"Checking {remaining} more sources…"` suffix until all three have returned at least once.
- **All three sources have resolved at least once, combined problems empty**: panel is hidden. "Hidden = good" invariant holds from this point on.
- **Refetch in flight on one source**: that source's last-good snapshot keeps rendering. Other sources unaffected.
- **Refetch errored on one source**: inline amber "Could not re-check {source_name} — using last known state" line at top of the panel; cached problems for that source still render.

This closes the under-specification gap: no single source can drop another source's problems by virtue of being in-flight.

## 9. File plan

### New files (frontend)

- `frontend/src/components/admin/ProblemsPanel.tsx` — the triage list with per-source cached snapshots.
- `frontend/src/components/admin/FundDataRow.tsx` — stat cards.
- `frontend/src/components/admin/CollapsibleSection.tsx` — shared reusable wrapper with chevron header.
- `frontend/src/lib/useSyncTrigger.ts` — shared sync-now hook (§11).
- `frontend/src/components/admin/ProblemsPanel.test.tsx`
- `frontend/src/components/admin/FundDataRow.test.tsx`
- `frontend/src/lib/useSyncTrigger.test.ts`

### Modified files (frontend)

- `frontend/src/pages/AdminPage.tsx` — new top-level composition. Owns the auto-refresh loop, the `useSyncTrigger` instance, and the five top-level fetches listed in §8 (`/sync/layers`, `/sync/status`, `/coverage/summary`, `/system/jobs`, `/recommendations`). Wraps `SyncDashboard` + `JobsTable` + `CoverageSummaryCard` in `CollapsibleSection`.
- `frontend/src/pages/AdminPage.test.tsx` — minor updates for new layout assertions + top-level Sync-now button wiring.
- `frontend/src/pages/SyncDashboard.tsx` — **not verbatim.** Refactored to consume `useSyncTrigger` via a required prop instead of owning its own `triggerState`. The grid, recent-runs table, and layer-card internals are unchanged; only the button's trigger wiring moves out.
- `frontend/src/pages/SyncDashboard.test.tsx` — updated fixtures to pass a fake `useSyncTrigger` result; otherwise unchanged in spirit.

### New/modified files (backend, §3a)

- `app/services/sync_orchestrator/freshness.py` (or the existing layer-response builder — to be confirmed during implementation) — populate `consecutive_failures` + `last_error_category` from `sync_layer_progress` rather than hardcoding.
- `tests/test_sync_orchestrator_api.py` (or adjacent unit test file) — add coverage for the two new calculations.

### Unchanged

- `api/sync.ts` — no new types; `SyncLayer` already declares both fields.
- Any other `frontend/src/api/*.ts` file — no new fetchers.
- Migrations — none; both DB columns already exist on `sync_layer_progress`.

## 10. Test plan

**ProblemsPanel**:
- Renders "Checking for problems…" banner on first mount (cache empty).
- Renders nothing once all three sources have resolved at least once and the combined problem list is empty.
- Renders last-good cached problems for source X while source X is refetching (null fresh value).
- Renders one row per failing layer (with `consecutive_failures > 0`).
- Renders one row per stale blocking layer.
- Renders one row per failed job, but NOT for skipped jobs.
- Renders one row when `coverage.null_rows > 0`.
- Shows error_category on the row.
- Clicking "Open orchestrator details" expands that section (callback fires).

**FundDataRow**:
- Renders 4 live cells.
- Renders placeholder "–" with tooltip for the 3 data-pending cells.
- No crash when `/coverage/summary` or `/recommendations` fail individually — affected cells render "–" with error tone.

**AdminPage**:
- Orchestrator details section is collapsed on mount.
- Click chevron expands it; SyncDashboard rows render.
- Problems panel's "Open orchestrator details" link expands the section programmatically.
- Top-level Sync-now button fires via `useSyncTrigger` and both the top-level button AND the inner `SyncDashboard` button reflect the same state; clicking either fires exactly one POST (§11).

## 11. Sync-now placement

Currently inside `SyncDashboard`. If `SyncDashboard` is collapsed, the button is not visible. Operator needs a top-level Sync-now. Two concurrent buttons (top + inner when expanded) calling the same `triggerSync({scope:"full"})` must not both fire a POST.

### Decision: hoist the trigger state out of `SyncDashboard` into a shared hook

`useSyncTrigger()` — new hook at `frontend/src/lib/useSyncTrigger.ts`:

```ts
export interface SyncTriggerState {
  readonly kind: "idle" | "running" | "queued" | "error";
  readonly queuedRunId: number | null;
  readonly message: string | null;
  readonly trigger: () => Promise<void>;
  readonly clearError: () => void;
}

export function useSyncTrigger(onTriggered: () => void): SyncTriggerState;
```

`SyncDashboard` is refactored to consume `useSyncTrigger` from a prop rather than owning its own `triggerState`. `AdminPage` creates ONE `useSyncTrigger` instance and passes it to both the top-level button AND `SyncDashboard`. Both buttons reflect the same state; the trigger is only invoked once; `kind === "running"` OR `kind === "queued"` disables every button that references the hook. The inner `SyncDashboard` tests remain valid once a fake hook is plugged in.

This is the only code change to `SyncDashboard` — and it's the minimum needed to uphold the "never double-trigger" invariant.

### Display contract

Top-level button ALWAYS visible. Inner button only visible when orchestrator details is expanded. Both render the same label based on the shared state:

- `idle` → "Sync now"
- `running` or queued → "Running…" / "Queued" (disabled)
- `error` → error message inline (disabled briefly, then re-enabled on next `idle` transition via clearError).

If the operator clicks the top button and a sync is already running (backend-initiated by scheduler, not by us), the trigger returns 409. The hook surfaces that as `kind: "error", message: "Sync already running"` — same contract as today's `SyncDashboard` local handler.

## 12. Settled-decisions + prevention alignment

- **Kill switch separate from config flags** — unchanged; we don't touch that surface.
- **async-data-loading.md** — every `useAsync` in this PR owns its own error surface; Problems panel is an inline view, not a global banner.
- **safety-state-ui.md** — no safety indicators in scope (kill-switch / demo-live are elsewhere).
- **#127 API shape** — mirrors `SyncLayer` / `CoverageSummaryResponse` / `RecommendationsListResponse` verbatim; no fabricated fields.
- **#319 Duplicate types** — `CollapsibleSection` exports one `Props` interface, imported by every caller.

## 13. Rollout + verification

Before push:
1. `pnpm --dir frontend typecheck` + `test` pass.
2. `uv run ruff/format/pyright/pytest` — no-ops but run.

Browser verify (operator):
3. Load `/admin` with current live data. Expect:
   - Problems panel shows at least 2-3 rows (`cik_mapping`, `universe`, `candles`).
   - Fund-data row renders 4 live cells with real numbers + 3 placeholder "–".
   - Orchestrator details + background tasks collapsed.
4. Click Orchestrator details chevron → SyncDashboard renders unchanged.
5. Click "Open orchestrator details" from Problems → section auto-expands + smooth-scrolls to it.

## 14. Tech debt to file alongside this PR

- **Tier 1/2/3 breakdown endpoint** (`/coverage/summary` expanded to return `tier_1_count` / `tier_2_count` / `tier_3_count`).
- **Scores summary endpoint** (`/rankings/summary` returning count + latest_scored_at).
- **Theses summary endpoint** (`/theses/summary` returning count + latest_created_at).
- **Raw-storage visibility endpoint** (`/system/raw-storage` returning per-source byte counts + last-compaction timestamps).
- **`/system/jobs` consecutive-failure count** — currently each job's "skipped" is opaque; extend response so the Problems panel can distinguish "routine skipped (nothing to do)" from "skipped-but-problematic".

All are independent follow-ups; none block #323.

## 15. Reviewer cheat-sheet

- [ ] Problems panel is not rendered when there are zero problems.
- [ ] Skipped jobs are NOT surfaced as problems (noise filter).
- [ ] Failing jobs ARE surfaced.
- [ ] `cik_mapping` + `universe` + `candles` all appear in the Problems panel against the current dev DB.
- [ ] Orchestrator details is collapsed by default.
- [ ] "Open orchestrator details" link from a problem row expands that section.
- [ ] Top-level Sync-now button uses the same guard as the inner one (no double-trigger possible).
- [ ] No new backend endpoint introduced; no `apiFetch` calls outside `frontend/src/api/`.
- [ ] Stat cards with no backing data render "–" with a small tooltip, NOT zero (which would lie).
