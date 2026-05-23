# AlertsStrip Unified Feed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `AlertsStrip` from guard-rejections-only to a unified feed that also renders position alerts (#401) and coverage-status drops (#402), with per-feed overflow math, per-kind cursor semantics, and fan-out POSTs for mark-all-read / dismiss-all.

**Architecture:** Frontend-only. Three parallel GETs via existing `/alerts/*` endpoints, client-side merge into a timestamp-sorted discriminated-union list, per-feed `FeedState<T>` for explicit partial-failure handling, `Promise.allSettled` fan-out POSTs skipping errored feeds.

**Tech Stack:** React + TypeScript + Vite, vitest + @testing-library/react, Tailwind CSS.

**Spec:** `docs/superpowers/specs/2026-04-22-alerts-strip-unified.md`

**Ticket:** #399. Branch `feature/399-alerts-strip-unified` created; spec committed on it.

---

## File Structure

| Path | Responsibility | Action |
| --- | --- | --- |
| `frontend/src/api/types.ts` | Backend-payload type definitions | Modify (add 4 interfaces + 1 union) |
| `frontend/src/api/alerts.ts` | Alert endpoint fetcher/mutation wrappers | Modify (add 6 functions) |
| `frontend/src/components/dashboard/AlertsStrip.tsx` | Dashboard unified-alerts component | Rewrite (preserves public export; internal refactor) |
| `frontend/src/components/dashboard/AlertsStrip.test.tsx` | Component tests | Extend (update existing + add new test groups) |

No backend code. No migrations.

---

## Task 1: API types

**Files:**

- Modify: `frontend/src/api/types.ts` — append 5 new type declarations below existing `GuardRejection*` block.

- [ ] **Step 1: Read existing GuardRejection block to confirm style**

Run: `grep -n "GuardRejection" frontend/src/api/types.ts`

Confirm `GuardRejection`, `GuardRejectionAction`, `GuardRejectionsResponse` all exist.

- [ ] **Step 2: Append new types**

Add to end of `frontend/src/api/types.ts`:

```ts
// --- #396/#401 position alerts ----------------------------------------------

export type PositionAlertType = "sl_breach" | "tp_breach" | "thesis_break";

export interface PositionAlert {
  alert_id: number;
  alert_type: PositionAlertType;
  instrument_id: number;
  symbol: string;
  opened_at: string;
  resolved_at: string | null;
  detail: string;
  current_bid: string | null; // Decimal serialized as string by pydantic
}

export interface PositionAlertsResponse {
  alerts_last_seen_position_alert_id: number | null;
  unseen_count: number;
  alerts: PositionAlert[];
}

// --- #397/#402 coverage status drops ----------------------------------------

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

- [ ] **Step 3: Typecheck — expect pass**

Run: `pnpm --dir frontend typecheck`
Expected: PASS (types are standalone, no usage sites yet).

- [ ] **Step 4: Commit**

```bash
git add frontend/src/api/types.ts
git commit -m "feat(#399): types — position alerts + coverage status drops"
```

---

## Task 2: API client functions

**Files:**

- Modify: `frontend/src/api/alerts.ts` — append 6 new functions after existing `dismissAllAlerts`.

- [ ] **Step 1: Read existing structure**

Run: `cat frontend/src/api/alerts.ts`

Confirm existing fns: `fetchGuardRejections`, `markAlertsSeen`, `dismissAllAlerts`. Note imports (`apiFetch`, type imports).

- [ ] **Step 2: Update imports at top of file**

Change the existing `import type` line to:

```ts
import type {
  CoverageStatusDropsResponse,
  GuardRejectionsResponse,
  PositionAlertsResponse,
} from "@/api/types";
```

- [ ] **Step 3: Append fetchers below `dismissAllAlerts`**

```ts
// --- #396/#401 position-alert endpoints -------------------------------------

export function fetchPositionAlerts(): Promise<PositionAlertsResponse> {
  return apiFetch<PositionAlertsResponse>("/alerts/position-alerts");
}

export function markPositionAlertsSeen(
  seenThroughPositionAlertId: number,
): Promise<void> {
  return apiFetch<void>("/alerts/position-alerts/seen", {
    method: "POST",
    body: JSON.stringify({
      seen_through_position_alert_id: seenThroughPositionAlertId,
    }),
  });
}

export function dismissAllPositionAlerts(): Promise<void> {
  return apiFetch<void>("/alerts/position-alerts/dismiss-all", {
    method: "POST",
  });
}

// --- #397/#402 coverage-status-drops endpoints ------------------------------

export function fetchCoverageStatusDrops(): Promise<CoverageStatusDropsResponse> {
  return apiFetch<CoverageStatusDropsResponse>("/alerts/coverage-status-drops");
}

export function markCoverageStatusDropsSeen(
  seenThroughEventId: number,
): Promise<void> {
  return apiFetch<void>("/alerts/coverage-status-drops/seen", {
    method: "POST",
    body: JSON.stringify({ seen_through_event_id: seenThroughEventId }),
  });
}

export function dismissAllCoverageStatusDrops(): Promise<void> {
  return apiFetch<void>("/alerts/coverage-status-drops/dismiss-all", {
    method: "POST",
  });
}
```

- [ ] **Step 4: Typecheck — expect pass**

Run: `pnpm --dir frontend typecheck`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/api/alerts.ts
git commit -m "feat(#399): API client fns — position alerts + coverage status drops"
```

---

## Task 3: AlertsStrip refactor — test suite (red)

**Files:**

- Modify: `frontend/src/components/dashboard/AlertsStrip.test.tsx` — rewrite top-level setup + adapt existing tests + add new test groups.

The test rewrite comes first (red). Next task implements the component to pass.

- [ ] **Step 1: Rewrite mock block**

Replace the existing `vi.mock("@/api/alerts", ...)` call at the top with:

```ts
vi.mock("@/api/alerts", () => ({
  fetchGuardRejections: vi.fn(),
  markAlertsSeen: vi.fn(),
  dismissAllAlerts: vi.fn(),
  fetchPositionAlerts: vi.fn(),
  markPositionAlertsSeen: vi.fn(),
  dismissAllPositionAlerts: vi.fn(),
  fetchCoverageStatusDrops: vi.fn(),
  markCoverageStatusDropsSeen: vi.fn(),
  dismissAllCoverageStatusDrops: vi.fn(),
}));
```

- [ ] **Step 2: Replace stub helpers with three-feed stubs**

Remove the existing `mockedFetch` / `stubFetch` / `stubFetchError` helpers and replace with:

```ts
import type {
  CoverageStatusDrop,
  CoverageStatusDropsResponse,
  GuardRejection,
  GuardRejectionsResponse,
  PositionAlert,
  PositionAlertsResponse,
} from "@/api/types";

const mockedGuardFetch = vi.mocked(alertsApi.fetchGuardRejections);
const mockedPositionFetch = vi.mocked(alertsApi.fetchPositionAlerts);
const mockedCoverageFetch = vi.mocked(alertsApi.fetchCoverageStatusDrops);

const mockedMarkGuard = vi.mocked(alertsApi.markAlertsSeen);
const mockedMarkPosition = vi.mocked(alertsApi.markPositionAlertsSeen);
const mockedMarkCoverage = vi.mocked(alertsApi.markCoverageStatusDropsSeen);

const mockedDismissGuard = vi.mocked(alertsApi.dismissAllAlerts);
const mockedDismissPosition = vi.mocked(alertsApi.dismissAllPositionAlerts);
const mockedDismissCoverage = vi.mocked(alertsApi.dismissAllCoverageStatusDrops);

const EMPTY_GUARD: GuardRejectionsResponse = {
  alerts_last_seen_decision_id: null,
  unseen_count: 0,
  rejections: [],
};
const EMPTY_POSITION: PositionAlertsResponse = {
  alerts_last_seen_position_alert_id: null,
  unseen_count: 0,
  alerts: [],
};
const EMPTY_COVERAGE: CoverageStatusDropsResponse = {
  alerts_last_seen_coverage_event_id: null,
  unseen_count: 0,
  drops: [],
};

function stubAll(
  overrides: {
    guard?: Partial<GuardRejectionsResponse> | Error;
    position?: Partial<PositionAlertsResponse> | Error;
    coverage?: Partial<CoverageStatusDropsResponse> | Error;
  } = {},
) {
  if (overrides.guard instanceof Error) {
    mockedGuardFetch.mockRejectedValue(overrides.guard);
  } else {
    mockedGuardFetch.mockResolvedValue({ ...EMPTY_GUARD, ...overrides.guard });
  }
  if (overrides.position instanceof Error) {
    mockedPositionFetch.mockRejectedValue(overrides.position);
  } else {
    mockedPositionFetch.mockResolvedValue({ ...EMPTY_POSITION, ...overrides.position });
  }
  if (overrides.coverage instanceof Error) {
    mockedCoverageFetch.mockRejectedValue(overrides.coverage);
  } else {
    mockedCoverageFetch.mockResolvedValue({ ...EMPTY_COVERAGE, ...overrides.coverage });
  }
}

function makeGuard(overrides: Partial<GuardRejection> = {}): GuardRejection {
  return {
    decision_id: 501,
    decision_time: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
    instrument_id: 42,
    symbol: "AAPL",
    action: "BUY",
    explanation: "FAIL — cash_available",
    ...overrides,
  };
}

function makePosition(overrides: Partial<PositionAlert> = {}): PositionAlert {
  return {
    alert_id: 701,
    alert_type: "sl_breach",
    instrument_id: 43,
    symbol: "MSFT",
    opened_at: new Date(Date.now() - 10 * 60 * 1000).toISOString(),
    resolved_at: null,
    detail: "bid=320 < sl=330",
    current_bid: "320",
    ...overrides,
  };
}

function makeCoverage(overrides: Partial<CoverageStatusDrop> = {}): CoverageStatusDrop {
  return {
    event_id: 301,
    instrument_id: 44,
    symbol: "TSLA",
    changed_at: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
    old_status: "analysable",
    new_status: "insufficient",
    ...overrides,
  };
}
```

- [ ] **Step 3: Update the existing `renders nothing when rejections list is empty` and `renders nothing on fetch error` tests to use `stubAll`**

Replace:

```ts
it("renders nothing when all feeds are empty", async () => {
  stubAll();
  const { container } = renderStrip();
  await vi.waitFor(() => {
    expect(alertsApi.fetchGuardRejections).toHaveBeenCalled();
    expect(alertsApi.fetchPositionAlerts).toHaveBeenCalled();
    expect(alertsApi.fetchCoverageStatusDrops).toHaveBeenCalled();
  });
  expect(container).toBeEmptyDOMElement();
});

it("renders nothing when all feeds error (silent-on-error)", async () => {
  stubAll({
    guard: new Error("boom"),
    position: new Error("boom"),
    coverage: new Error("boom"),
  });
  const { container } = renderStrip();
  await vi.waitFor(() => {
    expect(alertsApi.fetchGuardRejections).toHaveBeenCalled();
  });
  expect(container).toBeEmptyDOMElement();
});
```

- [ ] **Step 4: Adapt remaining existing guard-only tests**

For each existing `it("renders row symbol / action / explanation", ...)` etc., wrap the stub in `stubAll({ guard: { rejections: [baseRow], unseen_count: 1 } })`. Keep the same assertions — behaviour for a guard-only feed must not change.

- [ ] **Step 5: Add new test — merge across three kinds, DESC by ts**

```ts
it("merges three feeds into a single list DESC by timestamp", async () => {
  const coverage = makeCoverage({
    changed_at: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
  });
  const guard = makeGuard({
    decision_time: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
  });
  const position = makePosition({
    opened_at: new Date(Date.now() - 15 * 60 * 1000).toISOString(),
  });
  stubAll({
    guard: { rejections: [guard], unseen_count: 1 },
    position: { alerts: [position], unseen_count: 1 },
    coverage: { drops: [coverage], unseen_count: 1 },
  });
  renderStrip();
  const rows = await screen.findAllByTestId("alerts-row");
  expect(rows).toHaveLength(3);
  // DESC by timestamp: guard (5m) → position (15m) → coverage (30m)
  expect(rows[0]!.textContent).toContain(guard.symbol);
  expect(rows[1]!.textContent).toContain(position.symbol);
  expect(rows[2]!.textContent).toContain(coverage.symbol);
});
```

- [ ] **Step 6: Add test — per-kind pill badges rendered**

```ts
it("renders kind pill badge for each row type", async () => {
  stubAll({
    guard: { rejections: [makeGuard()], unseen_count: 1 },
    position: { alerts: [makePosition()], unseen_count: 1 },
    coverage: { drops: [makeCoverage()], unseen_count: 1 },
  });
  renderStrip();
  expect(await screen.findByText("GUARD")).toBeInTheDocument();
  expect(screen.getByText("POSITION")).toBeInTheDocument();
  expect(screen.getByText("COVERAGE")).toBeInTheDocument();
});
```

- [ ] **Step 7: Add test — click-through routes by instrument_id**

```ts
it("links rows with non-null instrument_id to /instruments/<id>", async () => {
  stubAll({
    guard: { rejections: [makeGuard({ instrument_id: 42 })], unseen_count: 1 },
    position: { alerts: [makePosition({ instrument_id: 43 })], unseen_count: 1 },
    coverage: { drops: [makeCoverage({ instrument_id: 44 })], unseen_count: 1 },
  });
  renderStrip();
  await screen.findAllByTestId("alerts-row");
  const links = screen
    .getAllByRole("link")
    .map((l) => l.getAttribute("href"));
  expect(links).toEqual(
    expect.arrayContaining(["/instruments/42", "/instruments/43", "/instruments/44"]),
  );
});

it("renders guard row with null instrument_id inline (no link)", async () => {
  stubAll({
    guard: {
      rejections: [makeGuard({ instrument_id: null, symbol: null })],
      unseen_count: 1,
    },
  });
  renderStrip();
  const row = await screen.findByTestId("alerts-row");
  expect(row.closest("a")).toBeNull();
});
```

- [ ] **Step 8: Add test — hide when any feed still loading**

```ts
it("renders null while any feed is still loading (no flash)", () => {
  mockedGuardFetch.mockResolvedValue(EMPTY_GUARD);
  mockedPositionFetch.mockImplementation(() => new Promise(() => {})); // never resolves
  mockedCoverageFetch.mockResolvedValue(EMPTY_COVERAGE);
  const { container } = renderStrip();
  // No await — strip must render nothing at first paint.
  expect(container).toBeEmptyDOMElement();
});
```

- [ ] **Step 9: Add test — partial error renders the ok feeds**

```ts
it("renders ok feeds when one feed errored", async () => {
  stubAll({
    guard: new Error("boom"),
    position: { alerts: [makePosition({ symbol: "MSFT" })], unseen_count: 1 },
    coverage: { drops: [makeCoverage({ symbol: "TSLA" })], unseen_count: 1 },
  });
  renderStrip();
  expect(await screen.findByText("MSFT")).toBeInTheDocument();
  expect(screen.getByText("TSLA")).toBeInTheDocument();
});
```

- [ ] **Step 10: Add test — unseen badge sums across feeds**

```ts
it("header badge is sum of per-feed unseen_count", async () => {
  stubAll({
    guard: { rejections: [makeGuard()], unseen_count: 3 },
    position: { alerts: [makePosition()], unseen_count: 2 },
    coverage: { drops: [makeCoverage()], unseen_count: 1 },
  });
  renderStrip();
  expect(await screen.findByText("6 new")).toBeInTheDocument();
});
```

- [ ] **Step 11: Add test — per-feed overflow surfaces Dismiss-all**

```ts
it("shows Dismiss-all when any feed has unseen > rendered (per-feed overflow)", async () => {
  // guard renders 1 row, unseen_count=2 → per-feed overflow.
  stubAll({
    guard: { rejections: [makeGuard()], unseen_count: 2 },
  });
  renderStrip();
  expect(await screen.findByRole("button", { name: /Dismiss all/i })).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: /Mark all read/i })).toBeNull();
});

it("shows Mark-all-read when all feeds have unseen == rendered (no overflow)", async () => {
  stubAll({
    guard: { rejections: [makeGuard()], unseen_count: 1 },
    position: { alerts: [makePosition()], unseen_count: 1 },
  });
  renderStrip();
  expect(await screen.findByRole("button", { name: /Mark all read/i })).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: /Dismiss all/i })).toBeNull();
});
```

- [ ] **Step 12: Add test — mark-all-read fan-out with per-kind MAX ids**

```ts
it("Mark-all-read fans out to each non-empty feed with its MAX id", async () => {
  const g = makeGuard({ decision_id: 510 });
  const p = makePosition({ alert_id: 710 });
  // coverage empty → no POST expected
  stubAll({
    guard: { rejections: [g], unseen_count: 1 },
    position: { alerts: [p], unseen_count: 1 },
  });
  mockedMarkGuard.mockResolvedValue(undefined);
  mockedMarkPosition.mockResolvedValue(undefined);
  renderStrip();
  const btn = await screen.findByRole("button", { name: /Mark all read/i });
  await userEvent.click(btn);
  await vi.waitFor(() => {
    expect(mockedMarkGuard).toHaveBeenCalledWith(510);
    expect(mockedMarkPosition).toHaveBeenCalledWith(710);
    expect(mockedMarkCoverage).not.toHaveBeenCalled();
  });
});

it("Mark-all-read tolerates a single POST failure and still calls others", async () => {
  stubAll({
    guard: { rejections: [makeGuard({ decision_id: 510 })], unseen_count: 1 },
    position: { alerts: [makePosition({ alert_id: 710 })], unseen_count: 1 },
  });
  mockedMarkGuard.mockRejectedValue(new Error("guard seen boom"));
  mockedMarkPosition.mockResolvedValue(undefined);
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  renderStrip();
  await userEvent.click(await screen.findByRole("button", { name: /Mark all read/i }));
  await vi.waitFor(() => {
    expect(mockedMarkGuard).toHaveBeenCalled();
    expect(mockedMarkPosition).toHaveBeenCalled();
  });
  expect(consoleSpy).toHaveBeenCalled();
  consoleSpy.mockRestore();
});
```

- [ ] **Step 13: Add test — dismiss-all skips errored feeds**

```ts
it("Dismiss-all skips POST for any errored feed", async () => {
  // guard errored, position has overflow.
  stubAll({
    guard: new Error("boom"),
    position: { alerts: [makePosition()], unseen_count: 5 }, // overflow 5 > 1
  });
  mockedDismissPosition.mockResolvedValue(undefined);
  // Simulate operator confirming the dialog.
  const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
  renderStrip();
  const btn = await screen.findByRole("button", { name: /Dismiss all/i });
  await userEvent.click(btn);
  await vi.waitFor(() => {
    expect(mockedDismissPosition).toHaveBeenCalled();
  });
  expect(mockedDismissGuard).not.toHaveBeenCalled();
  expect(mockedDismissCoverage).toHaveBeenCalled(); // coverage was ok + empty, but still ok → called
  confirmSpy.mockRestore();
});

it("Dismiss-all no-op when operator cancels confirm", async () => {
  stubAll({
    guard: { rejections: [makeGuard()], unseen_count: 5 },
  });
  const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(false);
  renderStrip();
  await userEvent.click(await screen.findByRole("button", { name: /Dismiss all/i }));
  expect(mockedDismissGuard).not.toHaveBeenCalled();
  confirmSpy.mockRestore();
});
```

- [ ] **Step 14: Run test file — expect all new tests to FAIL**

Run: `pnpm --dir frontend test -- AlertsStrip`

Expected: every new test fails (component still only knows about guard). Some existing tests may still pass if they happen to not break on the new mocks.

- [ ] **Step 15: Commit the red test suite**

```bash
git add frontend/src/components/dashboard/AlertsStrip.test.tsx
git commit -m "test(#399): red suite for unified AlertsStrip"
```

---

## Task 4: AlertsStrip refactor — implementation (green)

**Files:**

- Modify: `frontend/src/components/dashboard/AlertsStrip.tsx` — full rewrite of internals. Public export `AlertsStrip` preserved.

- [ ] **Step 1: Replace the entire file contents with the refactored implementation**

```tsx
/**
 * AlertsStrip — unified dashboard alert feed (#399).
 *
 * Renders three independent alert streams in a single timestamp-sorted list:
 *
 *   1. Guard rejections (#394)
 *   2. Position alerts — SL/TP/thesis breach episodes (#401)
 *   3. Coverage status drops from 'analysable' (#402)
 *
 * Each feed keeps its own BIGSERIAL cursor column on `operators`. Partial
 * failure is tolerated: one feed GET erroring does not hide the others; one
 * POST failing does not block the siblings (Promise.allSettled).
 *
 * Overflow math is per-feed — each backend query caps at LIMIT 500 so
 * global `totalUnseen > merged.length` would conflate feed-A hidden rows
 * with feed-B seen padding. See spec
 * docs/superpowers/specs/2026-04-22-alerts-strip-unified.md for rationale.
 */
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import {
  dismissAllAlerts,
  dismissAllCoverageStatusDrops,
  dismissAllPositionAlerts,
  fetchCoverageStatusDrops,
  fetchGuardRejections,
  fetchPositionAlerts,
  markAlertsSeen,
  markCoverageStatusDropsSeen,
  markPositionAlertsSeen,
} from "@/api/alerts";
import type {
  CoverageStatusDrop,
  CoverageStatusDropsResponse,
  GuardRejection,
  GuardRejectionsResponse,
  PositionAlert,
  PositionAlertsResponse,
} from "@/api/types";
import { formatRelativeTime } from "@/lib/format";

type FeedState<T> =
  | { status: "loading" }
  | { status: "ok"; data: T }
  | { status: "err" };

type AlertRow =
  | { kind: "guard";    ts: string; sortKey: number; row: GuardRejection }
  | { kind: "position"; ts: string; sortKey: number; row: PositionAlert }
  | { kind: "coverage"; ts: string; sortKey: number; row: CoverageStatusDrop };

type Cursors = {
  guard: number | null;
  position: number | null;
  coverage: number | null;
};

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

function isUnseen(r: AlertRow, c: Cursors): boolean {
  switch (r.kind) {
    case "guard":    return c.guard    === null || r.row.decision_id > c.guard;
    case "position": return c.position === null || r.row.alert_id    > c.position;
    case "coverage": return c.coverage === null || r.row.event_id    > c.coverage;
  }
}

function renderedCount(
  guard: FeedState<GuardRejectionsResponse>,
  position: FeedState<PositionAlertsResponse>,
  coverage: FeedState<CoverageStatusDropsResponse>,
): { guard: number; position: number; coverage: number } {
  return {
    guard: guard.status === "ok" ? guard.data.rejections.length : 0,
    position: position.status === "ok" ? position.data.alerts.length : 0,
    coverage: coverage.status === "ok" ? coverage.data.drops.length : 0,
  };
}

function unseenCount(
  guard: FeedState<GuardRejectionsResponse>,
  position: FeedState<PositionAlertsResponse>,
  coverage: FeedState<CoverageStatusDropsResponse>,
): { guard: number; position: number; coverage: number } {
  return {
    guard: guard.status === "ok" ? guard.data.unseen_count : 0,
    position: position.status === "ok" ? position.data.unseen_count : 0,
    coverage: coverage.status === "ok" ? coverage.data.unseen_count : 0,
  };
}

function KindPill({ kind }: { kind: AlertRow["kind"] }) {
  const style = {
    guard:    "bg-amber-100 text-amber-800",
    position: "bg-red-100 text-red-800",
    coverage: "bg-slate-100 text-slate-700",
  }[kind];
  const label = { guard: "GUARD", position: "POSITION", coverage: "COVERAGE" }[kind];
  return (
    <span className={`w-16 rounded px-1.5 py-0.5 text-center text-[10px] font-semibold uppercase ${style}`}>
      {label}
    </span>
  );
}

function RowShell({
  kind,
  unseen,
  instrumentId,
  children,
}: {
  kind: AlertRow["kind"];
  unseen: boolean;
  instrumentId: number | null;
  children: React.ReactNode;
}) {
  const border = unseen ? "border-l-4 border-amber-400" : "border-l-4 border-slate-200";
  const content = (
    <div
      data-testid="alerts-row"
      role="listitem"
      className={`flex items-center gap-3 px-3 py-2 text-sm ${border} bg-white`}
    >
      <KindPill kind={kind} />
      {children}
    </div>
  );
  if (instrumentId !== null) {
    return (
      <Link to={`/instruments/${instrumentId}`} className="block hover:bg-slate-50">
        {content}
      </Link>
    );
  }
  return content;
}

function GuardRow({ row, unseen }: { row: GuardRejection; unseen: boolean }) {
  return (
    <RowShell kind="guard" unseen={unseen} instrumentId={row.instrument_id}>
      <span className="w-16 font-semibold tabular-nums">{row.symbol ?? "—"}</span>
      <span className="w-16 text-xs uppercase text-slate-500">{row.action ?? "—"}</span>
      <span className="flex-1 truncate text-slate-700" title={row.explanation}>
        {row.explanation}
      </span>
      <span className="w-20 text-right text-xs text-slate-400">
        {formatRelativeTime(row.decision_time)}
      </span>
    </RowShell>
  );
}

function PositionRow({ row, unseen }: { row: PositionAlert; unseen: boolean }) {
  const alertLabel = {
    sl_breach:    "SL",
    tp_breach:    "TP",
    thesis_break: "THESIS",
  }[row.alert_type];
  return (
    <RowShell kind="position" unseen={unseen} instrumentId={row.instrument_id}>
      <span className="w-16 font-semibold tabular-nums">{row.symbol}</span>
      <span className="w-16 text-xs uppercase text-slate-500">{alertLabel}</span>
      <span className="flex-1 truncate text-slate-700" title={row.detail}>
        {row.detail}
      </span>
      <span className="w-20 text-right text-xs text-slate-400">
        {formatRelativeTime(row.opened_at)}
      </span>
    </RowShell>
  );
}

function CoverageRow({ row, unseen }: { row: CoverageStatusDrop; unseen: boolean }) {
  const transition = `${row.old_status} → ${row.new_status ?? "—"}`;
  return (
    <RowShell kind="coverage" unseen={unseen} instrumentId={row.instrument_id}>
      <span className="w-16 font-semibold tabular-nums">{row.symbol}</span>
      <span
        className="flex-1 truncate text-slate-700"
        title={transition}
      >
        {transition}
      </span>
      <span className="w-20 text-right text-xs text-slate-400">
        {formatRelativeTime(row.changed_at)}
      </span>
    </RowShell>
  );
}

function RowView({ row, cursors }: { row: AlertRow; cursors: Cursors }) {
  const unseen = isUnseen(row, cursors);
  switch (row.kind) {
    case "guard":    return <GuardRow    row={row.row} unseen={unseen} />;
    case "position": return <PositionRow row={row.row} unseen={unseen} />;
    case "coverage": return <CoverageRow row={row.row} unseen={unseen} />;
  }
}

export function AlertsStrip(): JSX.Element | null {
  const [guard, setGuard] = useState<FeedState<GuardRejectionsResponse>>({ status: "loading" });
  const [position, setPosition] = useState<FeedState<PositionAlertsResponse>>({ status: "loading" });
  const [coverage, setCoverage] = useState<FeedState<CoverageStatusDropsResponse>>({ status: "loading" });
  const [refetchKey, setRefetchKey] = useState(0);

  useEffect(() => {
    setGuard({ status: "loading" });
    setPosition({ status: "loading" });
    setCoverage({ status: "loading" });
    fetchGuardRejections()
      .then((d) => setGuard({ status: "ok", data: d }))
      .catch((err) => {
        console.error("[AlertsStrip] fetchGuardRejections failed", err);
        setGuard({ status: "err" });
      });
    fetchPositionAlerts()
      .then((d) => setPosition({ status: "ok", data: d }))
      .catch((err) => {
        console.error("[AlertsStrip] fetchPositionAlerts failed", err);
        setPosition({ status: "err" });
      });
    fetchCoverageStatusDrops()
      .then((d) => setCoverage({ status: "ok", data: d }))
      .catch((err) => {
        console.error("[AlertsStrip] fetchCoverageStatusDrops failed", err);
        setCoverage({ status: "err" });
      });
  }, [refetchKey]);

  const refetch = () => setRefetchKey((k) => k + 1);

  // Hide while any feed still loading — no flash of empty strip.
  if (guard.status === "loading" || position.status === "loading" || coverage.status === "loading") {
    return null;
  }
  // Hide when all three errored.
  if (guard.status === "err" && position.status === "err" && coverage.status === "err") {
    return null;
  }

  const merged = buildRows(guard, position, coverage);

  // Hide when all ok-feeds are empty.
  if (merged.length === 0) {
    return null;
  }

  const unseen = unseenCount(guard, position, coverage);
  const rendered = renderedCount(guard, position, coverage);
  const totalUnseen = unseen.guard + unseen.position + unseen.coverage;

  const guardOverflow    = unseen.guard    > rendered.guard;
  const positionOverflow = unseen.position > rendered.position;
  const coverageOverflow = unseen.coverage > rendered.coverage;
  const anyOverflow = guardOverflow || positionOverflow || coverageOverflow;

  const overflowAck = anyOverflow;
  const normalAck = totalUnseen > 0 && !anyOverflow;

  const cursors: Cursors = {
    guard: guard.status === "ok" ? guard.data.alerts_last_seen_decision_id : null,
    position: position.status === "ok" ? position.data.alerts_last_seen_position_alert_id : null,
    coverage: coverage.status === "ok" ? coverage.data.alerts_last_seen_coverage_event_id : null,
  };

  async function onMarkAllRead() {
    const guardMax    = Math.max(0, ...merged.filter((r) => r.kind === "guard").map((r) => (r.row as GuardRejection).decision_id));
    const positionMax = Math.max(0, ...merged.filter((r) => r.kind === "position").map((r) => (r.row as PositionAlert).alert_id));
    const coverageMax = Math.max(0, ...merged.filter((r) => r.kind === "coverage").map((r) => (r.row as CoverageStatusDrop).event_id));

    const promises: Promise<void>[] = [];
    if (guardMax > 0)    promises.push(markAlertsSeen(guardMax));
    if (positionMax > 0) promises.push(markPositionAlertsSeen(positionMax));
    if (coverageMax > 0) promises.push(markCoverageStatusDropsSeen(coverageMax));

    const results = await Promise.allSettled(promises);
    for (const r of results) {
      if (r.status === "rejected") {
        console.error("[AlertsStrip] mark-all-read partial failure", r.reason);
      }
    }
    refetch();
  }

  async function onDismissAll() {
    const hiddenCount =
      Math.max(0, unseen.guard    - rendered.guard)    +
      Math.max(0, unseen.position - rendered.position) +
      Math.max(0, unseen.coverage - rendered.coverage);
    const msg = `Dismiss all ${totalUnseen} unseen alerts? ${hiddenCount} are not shown above. Review them at /recommendations before dismissing if they might matter.`;
    if (!window.confirm(msg)) return;

    const promises: Promise<void>[] = [];
    if (guard.status === "ok")    promises.push(dismissAllAlerts());
    if (position.status === "ok") promises.push(dismissAllPositionAlerts());
    if (coverage.status === "ok") promises.push(dismissAllCoverageStatusDrops());

    const results = await Promise.allSettled(promises);
    for (const r of results) {
      if (r.status === "rejected") {
        console.error("[AlertsStrip] dismiss-all partial failure", r.reason);
      }
    }
    refetch();
  }

  return (
    <section
      aria-labelledby="alerts-strip-heading"
      className="rounded-md border border-slate-200 bg-white shadow-sm"
    >
      <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3">
        <div className="flex items-center gap-2">
          <h2 id="alerts-strip-heading" className="text-sm font-semibold text-slate-700">
            Alerts
          </h2>
          {totalUnseen > 0 ? (
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-700">
              {totalUnseen} new
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
        {overflowAck ? (
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onDismissAll}
              className="rounded border border-amber-300 bg-amber-50 px-2 py-1 text-xs font-medium text-amber-800 hover:bg-amber-100"
            >
              Dismiss all ({totalUnseen}) as acknowledged
            </button>
            <Link
              to="/recommendations"
              className="text-xs text-slate-500 underline hover:text-slate-700"
            >
              Triage at /recommendations
            </Link>
          </div>
        ) : null}
      </header>
      <div
        tabIndex={0}
        role="list"
        aria-labelledby="alerts-strip-heading"
        className="max-h-96 overflow-y-auto divide-y divide-slate-100"
      >
        {merged.map((row) => (
          <RowView key={`${row.kind}-${rowId(row)}`} row={row} cursors={cursors} />
        ))}
      </div>
    </section>
  );
}

function rowId(row: AlertRow): number {
  switch (row.kind) {
    case "guard":    return row.row.decision_id;
    case "position": return row.row.alert_id;
    case "coverage": return row.row.event_id;
  }
}
```

- [ ] **Step 2: Typecheck — expect pass**

Run: `pnpm --dir frontend typecheck`
Expected: PASS.

- [ ] **Step 3: Run the AlertsStrip tests — expect all pass**

Run: `pnpm --dir frontend test -- AlertsStrip`
Expected: all PASS.

- [ ] **Step 4: If any test fails, read the failure, fix the relevant code region, re-run.**

Common pitfalls:
- Timestamp-sort: if a test fails on ordering, confirm the generated timestamps are actually in the order the test expects. Check that `Date.parse` handles the ISO string from `new Date(...).toISOString()`.
- Dismiss-all-skips-errored-feeds: confirm the `if (guard.status === "ok")` gate is present. An `err` feed must NOT have its POST issued.
- Mark-all-read MAX: if a feed's merged slice is empty, `Math.max(0, ...[])` returns 0 and the POST is skipped. Check the `if (guardMax > 0)` gate.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/dashboard/AlertsStrip.tsx
git commit -m "feat(#399): unified AlertsStrip — guard + position + coverage feeds"
```

---

## Task 5: Pre-push gates + Codex checkpoint 2 + push + PR

**Files:** (none; gate + review + publish)

- [ ] **Step 1: Run all backend gates**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass. If any fail, fix + re-stage + new commit. Do NOT amend.

- [ ] **Step 2: Run frontend gates**

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

Both must pass.

- [ ] **Step 3: Smoke gate — app still boots**

Run: `uv run pytest tests/smoke/test_app_boots.py -v`
Expected: PASS. (Frontend-only PR but smoke check costs nothing.)

- [ ] **Step 4: Manual browser check (if dev stack running)**

If the dev stack is running via VS Code tasks (per memory `feedback_keep_stack_running.md`): open `http://localhost:5173/` (or the configured dev port), navigate to dashboard, confirm strip renders three feeds without console errors. Do not start or restart stack automatically.

If stack is not running, skip — document in PR body as "component tests verify behaviour; manual browser check deferred to reviewer".

- [ ] **Step 5: Codex checkpoint 2 — diff review before push**

```bash
git diff main...HEAD > /tmp/pr399_diff.txt
codex.cmd exec "Checkpoint 2 diff review for PR #399 (unified AlertsStrip). Diff at /tmp/pr399_diff.txt. Spec at d:/Repos/eBull/docs/superpowers/specs/2026-04-22-alerts-strip-unified.md. Plan at d:/Repos/eBull/docs/superpowers/plans/2026-04-22-alerts-strip-unified.md.

Focus: per-feed overflow math correctness, dismiss-all skipping errored feeds, click-through route matches /instruments/:instrumentId (not symbol), discriminated-union exhaustiveness, Promise.allSettled partial-failure handling, test coverage gaps. Reply terse."
```

Fix any real findings before pushing.

- [ ] **Step 6: Push + open PR**

```bash
git push -u origin feature/399-alerts-strip-unified
gh pr create --title "feat(#399): unified AlertsStrip — guard + position + coverage" --body "$(cat <<'EOF'
## What

- `AlertsStrip` renders guard rejections + position alerts + coverage status drops in one timestamp-sorted DESC list.
- Three parallel GETs to existing `/alerts/*` endpoints; client-side merge into discriminated-union rows with per-kind pill badges.
- Unified header (`Alerts`, total-new badge) with fan-out `Promise.allSettled` POSTs for mark-all-read and dismiss-all.
- Per-feed overflow math (each backend feed caps at 500 independently).
- Dismiss-all skips POSTs to errored feeds — never ack invisible alerts.
- New API client fns + types for position + coverage.

## Why

Closes #399. Backend pieces shipped in #394 (guard), #401 (position), #402 (coverage) — this wires them into the dashboard.

## Test plan

- Vitest suite covers: merge ordering, per-kind rendering, click-through via instrument_id, hide policy (loading / all-err / all-empty / partial-err), unseen totals, per-feed overflow triggering dismiss-all, mark-all-read fan-out with MAX ids + partial-failure, dismiss-all skipping errored feeds + confirm-cancel.
- Backend gates + smoke gate green.

## Called out

- Partial-failure is silent — matches existing silent-on-error pattern. Cursors are monotonic GREATEST so subsequent action retries are safe.
- Click-through uses `/instruments/:instrumentId` (existing legacy-id shim route) — same as current guard row pattern.
- Timestamp sort is presentation only — each feed's 500-row cap is backend-ordered by PK DESC (race-safe). Client merge preserves cursor math per-kind.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7: Start polling review + CI immediately**

Per memory `feedback_post_push_cycle.md`: the loop is not optional. Start polling without asking:

```bash
gh pr checks --watch
gh pr view --comments
```

Resolve every comment as FIXED / DEFERRED / REBUTTED per the review comment resolution contract. Re-run Task 5 Steps 1–3 before every follow-up push. Merge only after APPROVE on the most recent commit + CI green (or Codex-agreed rebuttal-only round per `feedback_merge_rules`).

---

## Self-review

**1. Spec coverage:**

- Types (PositionAlert, PositionAlertsResponse, CoverageStatusDrop, CoverageStatusDropsResponse) → Task 1.
- API client fns (6) → Task 2.
- Component refactor: parallel loader, discriminated union, merge, per-kind pills, per-feed overflow, fan-out actions, dismiss-all skips errored feeds, click-through via instrument_id → Task 4.
- Test coverage for merge, per-kind, click-through, hide policy, unseen totals, overflow, mark-all-read fan-out, dismiss-all errored-feed skip → Task 3.
- Pre-push gates + Codex ckpt 2 + PR → Task 5.

All spec sections covered.

**2. Placeholder scan:** no "TBD", "TODO", "implement later". Every code step shows the full diff.

**3. Type consistency:**

- `FeedState<T>` shape stable across Task 3 + Task 4.
- `AlertRow` discriminator `kind: "guard" | "position" | "coverage"` consistent in loader, renderer, action fan-out.
- Cursor property names match pydantic backend shape: `alerts_last_seen_decision_id`, `alerts_last_seen_position_alert_id`, `alerts_last_seen_coverage_event_id`.
- API fn names `markPositionAlertsSeen(seenThroughPositionAlertId)` / `markCoverageStatusDropsSeen(seenThroughEventId)` match the backend request-body field names.

**4. Known risks:**

- Test in Task 3 Step 13 asserts `mockedDismissCoverage` is called for an `ok + empty` coverage feed. This is correct — the dismiss endpoint is safe to POST against an empty window (backend `m.max_id IS NOT NULL` guard no-ops). Clarified in the existing backend specs.
- `userEvent` v14 handles the async click + state transition — no extra `act()` wrapping needed.
