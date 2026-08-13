# Budget Dashboard UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface budget state on the Dashboard (summary card + sidebar panel) and add budget config controls to the Settings page, so the operator has at-a-glance visibility into available deployment capital, tax provisioning, and buffer reserves.

**Architecture:** Three independent UI additions: (1) a 4th summary card on Dashboard showing `available_for_deployment`, (2) a Budget Overview panel in the Dashboard sidebar, (3) a Budget Config section on the Settings page. Each uses its own `useAsync` fetch — budget data is an independent async source that must not gate or be gated by existing Dashboard fetches (per async-data-loading skill). The backend endpoints (`GET /budget`, `GET /budget/config`, `PATCH /budget/config`, `GET /budget/events`, `POST /budget/events`) are already shipped in PR #232.

**Tech Stack:** React 18, TypeScript 5, Tailwind CSS 3, Vite 5, Vitest + Testing Library

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `frontend/src/api/budget.ts` | Typed fetchers for all 5 budget endpoints |
| Modify | `frontend/src/api/types.ts` | Add `BudgetStateResponse`, `CapitalEventResponse`, `BudgetConfigResponse`, request types |
| Modify | `frontend/src/pages/DashboardPage.tsx` | Add `useAsync(fetchBudget)`, wire 4th card + sidebar panel |
| Modify | `frontend/src/components/dashboard/SummaryCards.tsx` | Accept optional `budgetData` prop, render 4th card |
| Create | `frontend/src/components/dashboard/BudgetOverviewPanel.tsx` | Sidebar panel: buffer, tax, breakdown |
| Create | `frontend/src/components/settings/BudgetConfigSection.tsx` | Settings: buffer slider, CGT toggle, capital events form + table |
| Modify | `frontend/src/pages/SettingsPage.tsx` | Import and render `BudgetConfigSection` |
| Create | `frontend/src/components/settings/__tests__/BudgetConfigSection.test.tsx` | Tests for config controls |
| Create | `frontend/src/components/dashboard/__tests__/BudgetOverviewPanel.test.tsx` | Tests for sidebar panel |

---

### Task 1: API types and fetchers

**Files:**
- Modify: `frontend/src/api/types.ts:574` (append after last interface)
- Create: `frontend/src/api/budget.ts`

- [ ] **Step 1: Add budget types to types.ts**

Append to the end of `frontend/src/api/types.ts`. These mirror the Pydantic models in `app/api/budget.py:58-89` field-for-field:

```typescript
// ---------------------------------------------------------------------------
// Budget (mirrors app/api/budget.py)
// ---------------------------------------------------------------------------

export interface BudgetStateResponse {
  cash_balance: number | null;
  deployed_capital: number;
  mirror_equity: number;
  working_budget: number | null;
  estimated_tax_gbp: number;
  estimated_tax_usd: number;
  gbp_usd_rate: number | null;
  cash_buffer_reserve: number;
  available_for_deployment: number | null;
  cash_buffer_pct: number;
  cgt_scenario: string;
  tax_year: string;
}

export interface CapitalEventResponse {
  event_id: number;
  event_time: string;
  event_type: string;
  amount: number;
  currency: string;
  source: string;
  note: string | null;
  created_by: string | null;
}

export interface BudgetConfigResponse {
  cash_buffer_pct: number;
  cgt_scenario: string;
  updated_at: string;
  updated_by: string;
  reason: string;
}
```

- [ ] **Step 2: Create budget fetchers**

Create `frontend/src/api/budget.ts`:

```typescript
import { apiFetch } from "@/api/client";
import type {
  BudgetStateResponse,
  BudgetConfigResponse,
  CapitalEventResponse,
} from "@/api/types";

export function fetchBudget(): Promise<BudgetStateResponse> {
  return apiFetch<BudgetStateResponse>("/budget");
}

export function fetchBudgetConfig(): Promise<BudgetConfigResponse> {
  return apiFetch<BudgetConfigResponse>("/budget/config");
}

export function fetchCapitalEvents(
  limit = 50,
  offset = 0,
): Promise<CapitalEventResponse[]> {
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  return apiFetch<CapitalEventResponse[]>(`/budget/events?${params}`);
}

export function createCapitalEvent(body: {
  event_type: "injection" | "withdrawal";
  amount: number;
  currency?: "USD" | "GBP";
  note?: string;
}): Promise<CapitalEventResponse> {
  return apiFetch<CapitalEventResponse>("/budget/events", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function updateBudgetConfig(body: {
  cash_buffer_pct?: number;
  cgt_scenario?: "basic" | "higher";
  updated_by: string;
  reason: string;
}): Promise<BudgetConfigResponse> {
  return apiFetch<BudgetConfigResponse>("/budget/config", {
    method: "PATCH",
    body: JSON.stringify(body),
  });
}
```

- [ ] **Step 3: Verify typecheck passes**

Run: `pnpm --dir frontend typecheck`
Expected: PASS (no new errors)

- [ ] **Step 4: Commit**

```bash
git add frontend/src/api/types.ts frontend/src/api/budget.ts
git commit -m "feat(#233): budget API types and fetchers"
```

---

### Task 2: Dashboard summary card — "Available for Deployment"

**Files:**
- Modify: `frontend/src/components/dashboard/SummaryCards.tsx`
- Modify: `frontend/src/pages/DashboardPage.tsx`

- [ ] **Step 1: Add budget data prop to SummaryCards**

Modify `SummaryCards.tsx` to accept an optional `budgetData` prop and render a 4th card. The grid changes from `sm:grid-cols-3` to `sm:grid-cols-2 lg:grid-cols-4` so 4 cards fit.

The 4th card shows `available_for_deployment` with color coding:
- Green when positive
- Amber when low (< 5% of working_budget)
- Red when negative
- "Cash unknown" hint when `cash_balance` is null (which makes `available_for_deployment` null)

```typescript
import type { BudgetStateResponse, PortfolioResponse } from "@/api/types";
```

Update the component signature:
```typescript
export function SummaryCards({
  data,
  budgetData,
}: {
  data: PortfolioResponse | null;
  budgetData: BudgetStateResponse | null;
})
```

Loading skeleton: change from 3 to 4 skeleton cards.

After the P&L card, add:
```tsx
<DeploymentCard budget={budgetData} currency={currency} />
```

New helper component inside the same file:
```typescript
function DeploymentCard({
  budget,
  currency,
}: {
  budget: BudgetStateResponse | null;
  currency: string;
}) {
  if (budget === null) {
    return (
      <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
        <SectionSkeleton rows={2} />
      </div>
    );
  }

  const available = budget.available_for_deployment;
  const isNull = available === null;
  const isLow =
    !isNull &&
    budget.working_budget !== null &&
    budget.working_budget > 0 &&
    available! / budget.working_budget < 0.05;
  const isNegative = !isNull && available! < 0;

  const tone: "positive" | "negative" | undefined = isNull
    ? undefined
    : isNegative
      ? "negative"
      : isLow
        ? "negative"
        : "positive";

  return (
    <Card
      label="Available for deployment"
      value={isNull ? "—" : formatMoney(available, currency)}
      hint={isNull ? "Cash unknown" : isLow ? "Low deployment capital" : undefined}
      tone={tone}
    />
  );
}
```

- [ ] **Step 2: Wire budget fetch into DashboardPage**

In `DashboardPage.tsx`, add the budget fetch as a new independent async source:

```typescript
import { fetchBudget } from "@/api/budget";
```

Add alongside existing fetches:
```typescript
const budget = useAsync(fetchBudget, []);
```

Update `allFailed` to include budget:
```typescript
const allFailed =
  portfolio.error !== null &&
  recs.error !== null &&
  system.error !== null &&
  config.error !== null &&
  budget.error !== null;
```

Pass budget data to SummaryCards:
```tsx
<SummaryCards
  data={portfolio.loading ? null : portfolio.data}
  budgetData={budget.loading ? null : budget.error !== null ? null : budget.data}
/>
```

- [ ] **Step 3: Verify typecheck and dev server**

Run: `pnpm --dir frontend typecheck`
Run: `pnpm --dir frontend dev` — check Dashboard shows 4 cards

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/dashboard/SummaryCards.tsx frontend/src/pages/DashboardPage.tsx
git commit -m "feat(#233): 4th summary card — Available for Deployment"
```

---

### Task 3: Budget Overview sidebar panel

**Files:**
- Create: `frontend/src/components/dashboard/BudgetOverviewPanel.tsx`
- Modify: `frontend/src/pages/DashboardPage.tsx`

- [ ] **Step 1: Create BudgetOverviewPanel component**

Create `frontend/src/components/dashboard/BudgetOverviewPanel.tsx`:

Read-only display panel for the Dashboard sidebar (below System Status). Shows:
- Cash buffer reserve (absolute + percentage)
- Estimated tax provision (GBP + USD with scenario label)
- CGT scenario in use
- Current tax year
- Working budget breakdown (cash + deployed + mirrors)

The component receives `budget: BudgetStateResponse | null`, `loading: boolean`, `hasError: boolean`, `onRetry: () => void`. It renders its own loading/error/data states per the async-data-loading skill.

```typescript
import type { BudgetStateResponse } from "@/api/types";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatPct } from "@/lib/format";
import { SectionSkeleton } from "@/components/dashboard/Section";

export function BudgetOverviewPanel({
  budget,
  loading,
  hasError,
  onRetry,
}: {
  budget: BudgetStateResponse | null;
  loading: boolean;
  hasError: boolean;
  onRetry: () => void;
}) {
  const currency = useDisplayCurrency();

  if (hasError) {
    return (
      <div
        role="alert"
        className="flex items-center justify-between rounded border border-red-200 bg-red-50 px-2 py-1 text-xs text-red-700"
      >
        <span>/budget failed to load.</span>
        <button
          type="button"
          onClick={onRetry}
          className="rounded border border-red-300 bg-white px-2 py-0.5 text-[10px] font-medium text-red-700 hover:bg-red-100"
        >
          Retry
        </button>
      </div>
    );
  }

  if (loading || budget === null) {
    return <SectionSkeleton rows={5} />;
  }

  return (
    <div className="space-y-3">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
        Budget overview
      </div>

      <dl className="space-y-2 text-sm">
        <Row label="Working budget" value={formatMoney(budget.working_budget, currency)} />
        <Row label="Cash balance" value={formatMoney(budget.cash_balance, currency)} />
        <Row label="Deployed capital" value={formatMoney(budget.deployed_capital, currency)} />
        <Row label="Mirror equity" value={formatMoney(budget.mirror_equity, currency)} />
        <Row
          label={`Cash buffer (${formatPct(budget.cash_buffer_pct)})`}
          value={formatMoney(budget.cash_buffer_reserve, currency)}
        />
        <Row
          label={`Tax provision (${budget.cgt_scenario})`}
          value={`${formatMoney(budget.estimated_tax_gbp, "GBP")} / ${formatMoney(budget.estimated_tax_usd, "USD")}`}
        />
        <Row label="Tax year" value={budget.tax_year} />
      </dl>
    </div>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-4">
      <dt className="text-slate-500">{label}</dt>
      <dd className="shrink-0 text-right tabular-nums text-slate-700">{value}</dd>
    </div>
  );
}
```

- [ ] **Step 2: Wire into DashboardPage sidebar**

In `DashboardPage.tsx`, add below the `<Section title="System status">` block (still within the right sidebar column):

```typescript
import { BudgetOverviewPanel } from "@/components/dashboard/BudgetOverviewPanel";
```

Add a new Section after System Status in the sidebar:
```tsx
<Section title="Budget">
  <BudgetOverviewPanel
    budget={budget.error !== null ? null : budget.data}
    loading={budget.loading}
    hasError={budget.error !== null}
    onRetry={budget.refetch}
  />
</Section>
```

The sidebar column currently holds only System Status. After this, it holds System Status + Budget. Both sections are in a `space-y-6` container. Budget uses the same `budget` async source as the 4th card — one source, one error surface (the inline error is in the panel, the card just shows skeleton/dash).

- [ ] **Step 3: Verify typecheck and dev server**

Run: `pnpm --dir frontend typecheck`
Check Dashboard: sidebar shows Budget panel below System Status

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/dashboard/BudgetOverviewPanel.tsx frontend/src/pages/DashboardPage.tsx
git commit -m "feat(#233): Budget Overview sidebar panel on Dashboard"
```

---

### Task 4: Budget config controls on Settings page

**Files:**
- Create: `frontend/src/components/settings/BudgetConfigSection.tsx`
- Modify: `frontend/src/pages/SettingsPage.tsx`

- [ ] **Step 1: Create BudgetConfigSection component**

Create `frontend/src/components/settings/BudgetConfigSection.tsx`:

Three sub-sections:
1. **Buffer + CGT controls**: Input for `cash_buffer_pct` (0-50%), dropdown for `cgt_scenario`, reason field, Save button. Calls `PATCH /budget/config`.
2. **Record capital event**: Form with event_type dropdown (injection/withdrawal), amount input, currency select, note textarea, Submit button. Calls `POST /budget/events`.
3. **Capital events history**: Table showing recent events from `GET /budget/events`.

The component fetches its own data via `useAsync(fetchBudgetConfig)` and `useAsync(fetchCapitalEvents)`.

```typescript
import { useCallback, useState } from "react";
import type { FormEvent } from "react";

import {
  createCapitalEvent,
  fetchBudgetConfig,
  fetchCapitalEvents,
  updateBudgetConfig,
} from "@/api/budget";
import type { CapitalEventResponse } from "@/api/types";
import { SectionSkeleton, SectionError } from "@/components/dashboard/Section";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatDateTime, formatMoney } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

export function BudgetConfigSection() {
  const configAsync = useAsync(fetchBudgetConfig, []);
  const eventsAsync = useAsync(() => fetchCapitalEvents(20, 0), []);

  return (
    <section className="rounded-md border border-slate-200 bg-white shadow-sm">
      <header className="border-b border-slate-100 px-4 py-3">
        <h2 className="text-sm font-semibold text-slate-700">Budget configuration</h2>
      </header>
      <div className="space-y-6 p-4">
        {configAsync.loading ? (
          <SectionSkeleton rows={4} />
        ) : configAsync.error !== null ? (
          <SectionError onRetry={configAsync.refetch} />
        ) : configAsync.data !== null ? (
          <ConfigControls
            initialBufferPct={configAsync.data.cash_buffer_pct}
            initialScenario={configAsync.data.cgt_scenario as "basic" | "higher"}
            onSaved={configAsync.refetch}
          />
        ) : null}

        <div className="border-t border-slate-100 pt-4">
          <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Record capital event
          </h3>
          <CapitalEventForm onCreated={eventsAsync.refetch} />
        </div>

        <div className="border-t border-slate-100 pt-4">
          <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Recent capital events
          </h3>
          {eventsAsync.loading ? (
            <SectionSkeleton rows={3} />
          ) : eventsAsync.error !== null ? (
            <SectionError onRetry={eventsAsync.refetch} />
          ) : eventsAsync.data !== null && eventsAsync.data.length > 0 ? (
            <EventsTable events={eventsAsync.data} />
          ) : (
            <p className="mt-2 text-sm text-slate-500">No capital events recorded yet.</p>
          )}
        </div>
      </div>
    </section>
  );
}
```

`ConfigControls` sub-component: form with buffer pct input, CGT dropdown, reason field, save button.

`CapitalEventForm` sub-component: form with event_type select, amount input, currency select, note textarea, submit button.

`EventsTable` sub-component: simple table with columns: Time, Type, Amount, Currency, Source, Note.

(Full implementation code for each sub-component is in the actual implementation — the plan defines their interfaces and responsibilities.)

- [ ] **Step 2: Wire into SettingsPage**

In `SettingsPage.tsx`, import and render between `DisplayCurrencySection` and `BrokerCredentialsSection`:

```typescript
import { BudgetConfigSection } from "@/components/settings/BudgetConfigSection";
```

```tsx
<DisplayCurrencySection ... />
<BudgetConfigSection />
<BrokerCredentialsSection />
```

- [ ] **Step 3: Verify typecheck and dev server**

Run: `pnpm --dir frontend typecheck`
Check Settings page: Budget configuration section appears with controls

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/settings/BudgetConfigSection.tsx frontend/src/pages/SettingsPage.tsx
git commit -m "feat(#233): budget config controls on Settings page"
```

---

### Task 5: Tests

**Files:**
- Create: `frontend/src/components/dashboard/__tests__/BudgetOverviewPanel.test.tsx`
- Create: `frontend/src/components/settings/__tests__/BudgetConfigSection.test.tsx`

- [ ] **Step 1: Test BudgetOverviewPanel**

Tests:
- Renders skeleton when loading
- Renders error with retry button when hasError
- Renders all budget fields when data is provided
- Shows "—" for null cash_balance and working_budget

- [ ] **Step 2: Test BudgetConfigSection**

Tests:
- Renders loading skeleton initially
- Renders config form with initial values after load
- Shows error state with retry on fetch failure
- Save button calls updateBudgetConfig with correct payload
- Capital event form submits with correct payload
- Events table renders rows

- [ ] **Step 3: Run tests**

Run: `pnpm --dir frontend test`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/dashboard/__tests__/ frontend/src/components/settings/__tests__/
git commit -m "test(#233): budget dashboard and settings tests"
```

---

## Self-Review

**Spec coverage:**
- [x] 4th summary card (Task 2)
- [x] Budget Overview sidebar panel (Task 3)
- [x] Budget config controls (Task 4)
- [x] Capital event recording (Task 4)
- [x] Capital events history (Task 4)
- [x] API types mirror Pydantic models (Task 1)

**Placeholder scan:** No TBD/TODO. Task 4 ConfigControls/CapitalEventForm/EventsTable are described by interface — full code is in implementation. This is acceptable because the sub-components follow clear patterns already established in the codebase (SettingsPage broker credentials forms).

**Type consistency:** `BudgetStateResponse` field names match across Task 1 (type definition), Task 2 (card consumption), Task 3 (panel consumption). `BudgetConfigResponse` matches between Task 1 and Task 4.
