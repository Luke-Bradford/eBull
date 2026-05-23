# Admin problems panel — drill-through + clear-when hints + 409 UX fix

Issue: #415
Branch: `feature/415-admin-problems-drillthrough`
Date: 2026-04-23

## Scope (locked)

### In scope

1. Differentiate **409 "sync already running"** from real errors in `useSyncTrigger.ts`. New kind `"conflict"`. Render as amber info pill (not red). In the 409 branch, also call `onTriggered()` so the caller's status poll fires immediately — the amber "conflict" pill resolves into the grey "Running" disabled state within one poll cycle instead of waiting up to 60 s for the idle-cadence tick.
2. `ProblemsPanel.tsx` — for every alert row, render:

   - a **"Clears when …"** line (static text keyed to alert type), and
   - a **drill-through** for job failures (new).

3. New page `AdminJobDetailPage` at route `/admin/jobs/:name` using existing `GET /jobs/runs?job_name=<name>`. Table of latest 50 runs, newest first. Only rows with `status === 'failure'` and non-null `error_msg` are expandable; success/skipped/running rows are non-interactive (no misleading blank expansions).
4. Route wiring in `App.tsx` + nav from `ProblemsPanel` failing-job row. Job names are passed through `encodeURIComponent` both when building the link and when calling the API, decoded with `decodeURIComponent` for display.
5. Tests: `ProblemsPanel.test.tsx` updates (clearsWhen + drill href), new `AdminJobDetailPage.test.tsx`, `useSyncTrigger.test.ts` conflict-vs-error split.

### Out of scope (deferred → follow-up tickets)

- Ack / snooze state on alerts. Filed as follow-up after this PR merges.
- Coverage null-filings-status drill page (needs a new list endpoint; route table currently has only `/admin/coverage/insufficient`).
- Structured per-job log endpoint (`/admin/jobs/<name>/logs`) — requires log tagging rework.
- `monitor_positions` `red_flag_score` SQL bug — separate ticket (fix at source, not UI).

### Not touched (explicit non-goals)

- Alert engine re-architecture. No changes to v2 `action_needed` / `secret_missing` shape.
- No backend endpoint additions (`/jobs/runs` already serves the detail page).

## File-by-file plan

### Backend

None. `GET /jobs/runs?job_name=<name>&limit=50` already returns the full shape needed ([app/api/jobs.py:141-214](app/api/jobs.py#L141-L214)). `fetchJobRuns(jobName?, limit?)` already exists at [frontend/src/api/jobs.ts:23-35](frontend/src/api/jobs.ts#L23-L35) and returns a `JobRunsListResponse` (with `.items`, `.count`, `.limit`, `.job_name`) — reuse it directly.

### Frontend

#### `frontend/src/lib/useSyncTrigger.ts`

- Extend `SyncTriggerKind` with `"conflict"`.
- In the `catch` block, `err.status === 409` → `setState({ kind: "conflict", queuedRunId: null, message: "Another sync is already running" })`, then call `onTriggered()` so the caller's status polling fires immediately. Non-409 unchanged.
- `inFlightRef` effect: conflict is **not** in-flight — include it alongside `error` and `idle` in the "reset ref" branch so a retry click is not blocked.
- `clearQueued`: no-op branch for `"conflict"` (user must click again after reading, same as `"error"`).

#### `frontend/src/pages/AdminPage.tsx`

- `SyncNowButton` `triggerKind` prop type: include `"conflict"`.
- Render: `triggerKind === "conflict"` → amber info pill `text-amber-700 bg-amber-50` inline text. Button stays re-clickable (not disabled by `conflict`) — user can retry when orchestrator finishes.
- Error styling (`text-red-600`) reserved for `"error"` only.

#### `frontend/src/pages/SyncDashboard.tsx`

- Same branch addition at [SyncDashboard.tsx:195](frontend/src/pages/SyncDashboard.tsx#L195): amber text for conflict, red text for error.

#### `frontend/src/components/admin/ProblemsPanel.tsx`

- Extend every `<li>` with a `clearsWhen` line rendered in `text-xs text-slate-600`:

  - `ActionNeededRow` → `"Clears when the next run of <root_layer> succeeds"`
  - `SecretMissingRow` → `"Clears when the credential is supplied in Settings → Providers"`
  - failing-job row → `"Clears when the next run of <job.name> succeeds"`
  - coverage-null row → `"Clears after the fundamentals/coverage audit tags these instruments. If the count is not falling, the audit job is stuck — check its last run."` — phrased so the operator gets the *actionable* condition, not just the symptom restated.

- Failing-job row: wrap the name in a `<Link to={/admin/jobs/${encodeURIComponent(job.name)}}>` with a "View runs →" affordance matching `ActionNeededRow`'s "Open orchestrator details →" style.
- No new props. No plumbing changes.

#### `frontend/src/pages/AdminJobDetailPage.tsx` (new)

- Route: `/admin/jobs/:name`. Read `:name` via `useParams()` — React Router 6 already URL-decodes path params, so do **not** call `decodeURIComponent` again (avoids double-decode on names containing `%`). Use the value as-is for both display and API call.
- Fetch: `fetchJobRuns(name, 50)` (existing helper; it re-encodes via `URLSearchParams` internally).
- States: loading, error, empty, list. Follow `.claude/skills/frontend/loading-error-empty-states.md`.
- Table columns: Started at, Finished at, Status pill, Duration (finished − started, or "—" if running), Row count.
- Expansion policy: only rows with `status === 'failure'` AND `error_msg !== null` are clickable/expandable. Expanded content is `<pre class="whitespace-pre-wrap">` of `error_msg`. Other rows render as plain non-interactive rows. This prevents the "every row clickable but most expand to blank" failure mode.
- Empty state copy: `"No recent runs for this job. If you arrived from an old bookmark, the job may have been renamed."` + "Back to Admin" link. Deliberately does not distinguish unknown-job from known-job-no-history — the endpoint cannot tell us which, and conflating the two in a neutral message is honest.
- Back link to `/admin`.
- Breadcrumb: `Admin / Jobs / <name>`. Match existing inline-link convention (no new shared breadcrumb component — none exists).

#### `frontend/src/App.tsx`

- Add `<Route path="admin/jobs/:name" element={<AdminJobDetailPage />} />` inside the authenticated block, alongside `admin`, before the `*` catch-all.

### Tests

#### `frontend/src/lib/useSyncTrigger.test.ts`

- Update: 409 → `kind === "conflict"`, `message === "Another sync is already running"`, `onTriggered` was called exactly once (confirms immediate reconcile).
- Keep existing 503 / non-409 assertions unchanged (`kind === "error"`, `onTriggered` not called).

#### `frontend/src/components/admin/ProblemsPanel.test.tsx`

- Assert `clearsWhen` text is rendered for each of the four row types.
- Assert failing-job row has an anchor with `href="/admin/jobs/<encoded-name>"`.
- Smoke-test a job name containing `/` to confirm encoding: e.g. `"etl/fundamentals_sync"` → `href="/admin/jobs/etl%2Ffundamentals_sync"`.

#### `frontend/src/pages/AdminJobDetailPage.test.tsx` (new)

- Loading state → skeleton visible.
- Error state → friendly error + retry.
- Empty state → "No recent runs …" copy + Back link.
- Populated state → rows ordered newest-first, status pill colour matches status, failure row expands to show `error_msg`, success row is not interactive.
- URL-encoded route param (e.g. `/admin/jobs/etl%2Ffundamentals_sync`) → component uses the router-decoded value as-is (assert `fetchJobRuns` is called with `"etl/fundamentals_sync"`, not double-decoded or re-encoded).

## Definition of done

- `ruff check . && ruff format --check . && pyright && pytest` all green (backend unaffected but run per CLAUDE.md).
- `pnpm --dir frontend typecheck && pnpm --dir frontend test:unit` green.
- Tests written first, failing before implementation (TDD per `.claude/skills/engineering/test-quality.md`).
- PR description: What / Why / Test plan bullets only (per `feedback_pr_description_brevity`).

## Settled-decisions check

- Product-visibility pivot (2026-04-18) — passes: operator-facing admin UX directly improves the "I can manage my fund" surface. When the fund surface shows a problem, drill-through is what the operator needs next.
- No infra / orchestrator re-architecture. No provider changes.

## Review-prevention-log check

- No `_tracked_job` edits (PREREQ_SKIP rule N/A).
- No new SQL. Endpoint reuse.
- Frontend-only changes + one new route. No async/lifecycle edits beyond the hook kind rename.
