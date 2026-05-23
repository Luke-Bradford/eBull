# AlertsStrip unified feed — design spec

**Ticket:** #399
**Depends on:** #394 (merged — guard-rejection strip), #396 / #401 (merged — position-alert event persistence), #397 / #402 (merged — coverage status transition log).

## Problem

`AlertsStrip` today shows only guard rejections. With `/alerts/position-alerts` (#401) and `/alerts/coverage-status-drops` (#402) now live, the dashboard needs to render all three feeds in one unified strip so operator sees everything needing attention in one place.

## Decisions (brainstorm 2026-04-22)

| # | Decision | Reason |
| --- | --- | --- |
| 1 | **Three parallel fetches + client-side merge.** No new backend endpoint. | Existing per-type cursor semantics stay untouched. Parallel fetches over HTTP/2 ≈ one RTT. YAGNI on a unified backend endpoint that would duplicate cursor-read logic server-side. |
| 2 | **Single timestamp-sorted list (DESC).** No grouping or priority tiers. | Matches operator's mental model of "what's new". Type identity preserved via per-kind badge + click-through. Grouping forces a layer to parse every render. Priority is subjective — a guard BUY block can be as urgent as a position SL breach. |
| 3 | **Unified header + fan-out POSTs with Promise.allSettled.** One badge, one button set; on action issue 3 parallel POSTs to each feed's `/seen` or `/dismiss-all`. | Single-feed UX preserved. Idempotent GREATEST cursors tolerate partial failure — next action round covers any missed one. |
| 4 | **Hide strip on all-empty or all-error; partial-ok renders what it has.** Loading in any feed → render null (no flash). | Matches today's silent-on-error pattern. Partial degrade beats "hide everything when one feed dies". |
| 5 | **Per-feed overflow; not global.** Each feed's server query is independently LIMIT 500 — so per-feed `unseen_count > rendered-rows-of-that-kind` is the only correct overflow signal. Global `totalUnseen > merged.length` conflates feed-A hidden rows with feed-B seen padding. | Codex ckpt 1 finding: global math lets seen rows from feed B mask hidden unseen rows from feed A, so `"Mark all read"` could falsely appear and acknowledge invisible rows. |
| 6 | **`dismiss-all` fan-out skips errored feeds.** If a feed's GET failed, operator does not know that feed's unseen state; dismissing it would ack invisible alerts. | Codex ckpt 1 finding. Fix: only POST `/dismiss-all` to feeds with `status === "ok"`. Mark-all-read already naturally skips via max-id = 0 for missing feeds. |
| 7 | **Timestamp sort is presentation only.** Each backend feed is PK-ordered (monotonic) so each feed's own 500-row cap is race-safe (`app/api/alerts.py:165-168`, `347-350`, `408-410`). Client re-sort by `ts` DESC just interleaves three PK-ordered slices. Cursor math (per-kind PK compare) is unaffected. | Codex ckpt 1 finding. Documenting explicitly so future devs don't assume client sort preserves the backend ordering invariant. |

## Architecture

Frontend-only. Three parallel fetches to existing endpoints, client-side merge into one timestamp-sorted list with discriminated-union rows.

### Surfaces

1. `frontend/src/api/types.ts` — add types mirroring backend pydantic shapes for position + coverage.
2. `frontend/src/api/alerts.ts` — add 6 fetcher fns.
3. `frontend/src/components/dashboard/AlertsStrip.tsx` — refactor to render discriminated union.
4. `frontend/src/components/dashboard/AlertsStrip.test.tsx` — extend with merge + per-kind + fan-out + partial-failure tests.

No new backend code. No migrations.

## Types

```ts
// frontend/src/api/types.ts additions

export type PositionAlertType = "sl_breach" | "tp_breach" | "thesis_break";

export interface PositionAlert {
  alert_id: number;
  alert_type: PositionAlertType;
  instrument_id: number;
  symbol: string;
  opened_at: string;
  resolved_at: string | null;
  detail: string;
  current_bid: string | null; // Decimal serialized as string
}

export interface PositionAlertsResponse {
  alerts_last_seen_position_alert_id: number | null;
  unseen_count: number;
  alerts: PositionAlert[];
}

export interface CoverageStatusDrop {
  event_id: number;
  instrument_id: number;
  symbol: string;
  changed_at: string;
  old_status: string;
  new_status: string | null;
}

export interface CoverageStatusDropsResponse {
  alerts_last_seen_coverage_event_id: number | null;
  unseen_count: number;
  drops: CoverageStatusDrop[];
}
```

### Discriminated union (component-local)

```ts
// frontend/src/components/dashboard/AlertsStrip.tsx

type AlertRow =
  | { kind: "guard";    ts: string; sortKey: number; row: GuardRejection }
  | { kind: "position"; ts: string; sortKey: number; row: PositionAlert }
  | { kind: "coverage"; ts: string; sortKey: number; row: CoverageStatusDrop };
```

- `ts` sources: `decision_time` / `opened_at` / `changed_at`.
- `sortKey` = `Date.parse(ts)` — cached so merge sort doesn't re-parse per compare.
- `row` preserves raw backend payload for type-specific rendering.

## API client

```ts
// frontend/src/api/alerts.ts additions

export function fetchPositionAlerts(): Promise<PositionAlertsResponse> {
  return apiFetch<PositionAlertsResponse>("/alerts/position-alerts");
}

export function markPositionAlertsSeen(seenThroughPositionAlertId: number): Promise<void> {
  return apiFetch<void>("/alerts/position-alerts/seen", {
    method: "POST",
    body: JSON.stringify({ seen_through_position_alert_id: seenThroughPositionAlertId }),
  });
}

export function dismissAllPositionAlerts(): Promise<void> {
  return apiFetch<void>("/alerts/position-alerts/dismiss-all", { method: "POST" });
}

export function fetchCoverageStatusDrops(): Promise<CoverageStatusDropsResponse> {
  return apiFetch<CoverageStatusDropsResponse>("/alerts/coverage-status-drops");
}

export function markCoverageStatusDropsSeen(seenThroughEventId: number): Promise<void> {
  return apiFetch<void>("/alerts/coverage-status-drops/seen", {
    method: "POST",
    body: JSON.stringify({ seen_through_event_id: seenThroughEventId }),
  });
}

export function dismissAllCoverageStatusDrops(): Promise<void> {
  return apiFetch<void>("/alerts/coverage-status-drops/dismiss-all", { method: "POST" });
}
```

## Component

### Loader

Three parallel states; use per-feed `FeedState` discriminated union rather than `useAsync` (single-promise).

```ts
type FeedState<T> = { status: "loading" } | { status: "ok"; data: T } | { status: "err" };
```

`useEffect` on a `refetchKey` fires all three in parallel; each resolves its own state. Partial failure is explicit; no single-promise error throws away successful feeds.

### Hide policy

- **Any** feed still `loading` → render `null` (no flash of empty strip).
- **All three** `err` → render `null`.
- **All three** `ok` with empty `rejections`/`alerts`/`drops` → render `null`.
- Otherwise render the strip using whatever `ok` feeds exist. Errored feed silently absent; logged via `console.error` per existing silent-on-error pattern.

### Merge + unseen

```ts
function buildRows(
  guard: FeedState<GuardRejectionsResponse>,
  position: FeedState<PositionAlertsResponse>,
  coverage: FeedState<CoverageStatusDropsResponse>,
): AlertRow[] {
  const rows: AlertRow[] = [];
  if (guard.status === "ok") {
    for (const r of guard.data.rejections) {
      rows.push({ kind: "guard", ts: r.decision_time, sortKey: Date.parse(r.decision_time), row: r });
    }
  }
  if (position.status === "ok") {
    for (const r of position.data.alerts) {
      rows.push({ kind: "position", ts: r.opened_at, sortKey: Date.parse(r.opened_at), row: r });
    }
  }
  if (coverage.status === "ok") {
    for (const r of coverage.data.drops) {
      rows.push({ kind: "coverage", ts: r.changed_at, sortKey: Date.parse(r.changed_at), row: r });
    }
  }
  rows.sort((a, b) => b.sortKey - a.sortKey);
  return rows;
}

type Cursors = { guard: number | null; position: number | null; coverage: number | null };

function isUnseen(r: AlertRow, c: Cursors): boolean {
  switch (r.kind) {
    case "guard":    return c.guard    === null || r.row.decision_id > c.guard;
    case "position": return c.position === null || r.row.alert_id    > c.position;
    case "coverage": return c.coverage === null || r.row.event_id    > c.coverage;
  }
}
```

### Totals + overflow

**Per-feed overflow is the only correct signal.** Each backend feed independently caps at LIMIT 500. A global `totalUnseenCount > merged.length` conflates "feed-A has hidden unseen rows" with "feed-B's seen rows padding merged.length" → would falsely surface "Mark all read" while unseen rows of feed A are invisible.

```ts
function renderedCount(feed: FeedState<{ rejections?: unknown[]; alerts?: unknown[]; drops?: unknown[] }>): number {
  if (feed.status !== "ok") return 0;
  return feed.data.rejections?.length ?? feed.data.alerts?.length ?? feed.data.drops?.length ?? 0;
}

function unseenCount(feed: FeedState<{ unseen_count: number }>): number {
  return feed.status === "ok" ? feed.data.unseen_count : 0;
}

const guardOverflow    = unseenCount(guard)    > renderedCount(guard);
const positionOverflow = unseenCount(position) > renderedCount(position);
const coverageOverflow = unseenCount(coverage) > renderedCount(coverage);

const anyOverflow   = guardOverflow || positionOverflow || coverageOverflow;
const totalUnseen   = unseenCount(guard) + unseenCount(position) + unseenCount(coverage);

const overflowAck = anyOverflow;
const normalAck   = totalUnseen > 0 && !anyOverflow;
```

Header badge shows `totalUnseen` when > 0. Errored feeds contribute 0 to `totalUnseen` (silent-on-error).

### Row variants

```tsx
function RowView({ row, unseen }: { row: AlertRow; unseen: boolean }) {
  switch (row.kind) {
    case "guard":    return <GuardRow    row={row.row} unseen={unseen} />;
    case "position": return <PositionRow row={row.row} unseen={unseen} />;
    case "coverage": return <CoverageRow row={row.row} unseen={unseen} />;
  }
}
```

Each variant renders identical columns: `[kind-pill] [symbol] [detail-column-A] [detail-column-B] [ts]`.

| Kind | Pill colour / label | Column A | Column B |
| --- | --- | --- | --- |
| guard | amber / `GUARD` | action (BUY/ADD/HOLD/EXIT) | explanation |
| position | red / `POSITION` | alert_type (SL/TP/THESIS) | detail |
| coverage | slate / `COVERAGE` | old → new status | — |

`unseen` toggles left-border colour (amber if unseen, slate if seen) — existing guard pattern extended.

### Click-through

All three variants wrap their row in `<Link to={`/instruments/${instrument_id}`}>` when `instrument_id` is non-null — matches existing guard `RowView` pattern (`AlertsStrip.tsx:55-61`). Route `/instruments/:instrumentId` is the legacy-id shim in `frontend/src/App.tsx`; passing an `instrument_id` (BIGINT) is the supported shape. Using symbol would route through the wrong path and fail parse in `InstrumentDetailRedirect`.

- Guard: `instrument_id` nullable — inline fallback when null.
- Position: `instrument_id` NOT NULL per backend (`app/api/alerts.py:84`). Always a link.
- Coverage: `instrument_id` NOT NULL per backend (`app/api/alerts.py:106`). Always a link.

No position-tab / research-tab deep-link in this PR — deferred.

### Actions

#### Mark all read

```ts
async function onMarkAllRead() {
  const guardMax = Math.max(0, ...merged.filter(r => r.kind === "guard").map(r => r.row.decision_id));
  const positionMax = Math.max(0, ...merged.filter(r => r.kind === "position").map(r => r.row.alert_id));
  const coverageMax = Math.max(0, ...merged.filter(r => r.kind === "coverage").map(r => r.row.event_id));

  const promises: Promise<void>[] = [];
  if (guardMax > 0)    promises.push(markAlertsSeen(guardMax));
  if (positionMax > 0) promises.push(markPositionAlertsSeen(positionMax));
  if (coverageMax > 0) promises.push(markCoverageStatusDropsSeen(coverageMax));

  const results = await Promise.allSettled(promises);
  for (const r of results) {
    if (r.status === "rejected") console.error("[AlertsStrip] mark-all-read partial failure", r.reason);
  }
  refetch();
}
```

#### Dismiss all

Skips POSTs to errored feeds — operator does not see an errored feed's state, must not ack invisible alerts.

```ts
async function onDismissAll() {
  // Hidden count = sum of per-feed (unseen - rendered); errored feeds contribute 0.
  const hiddenCount =
    Math.max(0, unseenCount(guard)    - renderedCount(guard)) +
    Math.max(0, unseenCount(position) - renderedCount(position)) +
    Math.max(0, unseenCount(coverage) - renderedCount(coverage));

  const msg = `Dismiss all ${totalUnseen} unseen alerts? ${hiddenCount} are not shown above. Review them at /recommendations before dismissing if they might matter.`;
  if (!window.confirm(msg)) return;

  const promises: Promise<void>[] = [];
  if (guard.status === "ok")    promises.push(dismissAllAlerts());
  if (position.status === "ok") promises.push(dismissAllPositionAlerts());
  if (coverage.status === "ok") promises.push(dismissAllCoverageStatusDrops());

  const results = await Promise.allSettled(promises);
  for (const r of results) {
    if (r.status === "rejected") console.error("[AlertsStrip] dismiss-all partial failure", r.reason);
  }
  refetch();
}
```

Mark-all-read naturally skips an errored feed because its `merged.filter(r=>r.kind===K)` returns empty → max = 0 → POST skipped (same mechanism as "feed has no visible rows").

### Header

- Heading text: **"Alerts"** (not "Guard rejections").
- Total-new badge: `{totalUnseenCount} new` when > 0.
- `normalAck` → "Mark all read" button.
- `overflowAck` → "Dismiss all ({totalUnseenCount}) as acknowledged" + `/recommendations` triage link (existing pattern).

## Edge cases

| Case | Behaviour |
| --- | --- |
| All three feeds return empty | Strip hidden. |
| One feed errors, others return rows | Strip shows the other two feeds' rows. Errored feed silently absent. `console.error` logged. |
| Any feed still loading | Strip renders null (no flash). |
| Null symbol on a row | Guard: inline (no link). Position + coverage: always have `symbol` per backend SELECT JOIN — won't happen; defensive treat as inline. |
| Total unseen > merged rows (overflow) | "Dismiss all" button shown with full count + hidden-count warning. |
| Mark-all-read with one feed errored | Max-id for that feed is 0; no POST issued for it. Other feeds' POSTs fire normally. |
| Mark-all-read POST fails | `Promise.allSettled` handles it; other POSTs succeed; `refetch()` still runs; subsequent action via GREATEST idempotency recovers. |
| All three `unseen_count === 0` | Strip still renders (there are seen rows to display), but no badge + no ack buttons. |
| Dismiss-all confirm rejected | No POSTs issued; state unchanged. |
| One feed errored, operator clicks Dismiss-all | POST skipped for that feed. Other two `/dismiss-all` POSTs fire. Errored feed's unseen state untouched. |
| One feed has per-feed overflow, others don't | Dismiss-all button shown (`anyOverflow === true`). Mark-all-read hidden. Avoids falsely appearing to have acknowledged hidden rows. |

## Testing

Extend `frontend/src/components/dashboard/AlertsStrip.test.tsx`. Mock all three fetchers via `vi.mock("@/api/alerts")`. Update existing guard-only tests to also return empty position + coverage responses.

### Test groups

**Merge + ordering**
- All three populated, mixed timestamps → rendered DESC by ts across kinds.
- Two feeds ok, one ok-empty → only the non-empty feeds' rows appear, DESC by ts.

**Per-kind rendering**
- Guard row: amber pill, action, explanation.
- Position row: red pill, alert_type, detail.
- Coverage row: slate pill, `old_status → new_status`.

**Click-through**
- Each kind with symbol wraps in `<Link to="/instruments/${symbol}">`.
- Guard with null symbol renders inline.

**Hide/show**
- Any feed still loading → null.
- All three empty + ok → null.
- All three errored → null.
- Two errored + one ok with rows → strip renders the one feed.

**Unseen totals + per-kind cursor**
- Sum of `unseen_count` = header badge.
- Row unseen iff its id > its kind's cursor.

**Mark-all-read fan-out**
- All three non-empty → 3 POSTs with each kind's merged-slice MAX id.
- One feed empty in merged → no POST for that kind.
- One POST rejects → other two still fire; `console.error` logged; `refetch` called.

**Dismiss-all fan-out**
- Confirm text names total unseen + hidden-count.
- 3 POSTs fan out on confirm, skipping if window.confirm rejected.
- Partial rejection logged; refetch runs.

**Overflow trigger (per-feed)**
- Only feed A has `unseen > rendered` → Dismiss-all visible; Mark-all-read hidden.
- All three under cap, `totalUnseen > 0` → Mark-all-read visible; Dismiss-all hidden.
- Feed A errored + B has overflow → Dismiss-all visible; POSTs fan-out skips A.

No Playwright / end-to-end tests in this PR — component tests + existing pre-push gates cover the behaviour.

## Pre-push checklist

Per CLAUDE.md:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

Python gates included even though this PR is frontend-only — type annotations in test payloads (via pydantic schema drift checks) would regress silently otherwise. Frontend typecheck + vitest are the primary gates here.

## PR description skeleton

Title: `feat(#399): unified AlertsStrip — guard + position + coverage`

Body:

> **What**
>
> - `AlertsStrip` renders guard rejections + position alerts + coverage-status drops in one timestamp-sorted list.
> - Three parallel fetches + client merge; per-kind badge + cursor; unified header with fan-out `Promise.allSettled` actions.
> - New API client fns for position + coverage endpoints; types mirror backend pydantic shapes.
>
> **Why**
>
> - Closes #399. Backend pieces shipped in #394 (guard), #401 (position), #402 (coverage) — this wires them into the dashboard.
>
> **Test plan**
>
> - Vitest: merge ordering, per-kind render, click-through, hide policy, unseen counts, mark-all-read + dismiss-all fan-out, partial-failure tolerance, overflow.
> - Manual: load the dashboard with all three feeds populated; confirm ordering, ack flow, and silent partial-failure.
>
> **Called out**
>
> - Partial-failure silent on any single feed POST/GET — matches existing silent-on-error pattern. Cursors recover via GREATEST on next action.
> - No new backend routes. No migrations.
> - Position/coverage click-through resolves to `/instruments/<symbol>` only (no tab deep-link) — deferred.
