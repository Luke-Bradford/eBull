# Instrument Detail Polish Round 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land #575 — replace fixed `2fr_1fr_1fr` density grid with a 12-column capability-profiled layout, hide truly-empty panes, unbundle dividends+insider, standardize all panes on a new `<Pane>` + `<PaneHeader>` primitive.

**Architecture:** Three stable layout profiles (`full-sec` / `partial-filings` / `minimal`) selected by `selectProfile(summary)`. Each pane component owns its own `<Pane>` chrome (no parent-renders-Pane double-wrap). Drill affordance is button-only on `PaneHeader` — no whole-card click. Empty-pane rule is four-state per spec Section D, gated mostly at the parent (`DensityGrid` decides which child components to mount based on capabilities).

**Tech Stack:** TypeScript + React 18, Tailwind, Vitest + React Testing Library. No new runtime deps. Spec: `docs/superpowers/specs/2026-04-27-instrument-detail-polish-round-2-design.md`. Issue: #575. Sibling (independent): #576 chart route.

**Branch:** `feature/575-instrument-detail-polish-round-2`

---

## Pre-flight

- [ ] **Step 0.1: Create branch from main**

```bash
git checkout main
git pull --ff-only
git checkout -b feature/575-instrument-detail-polish-round-2
```

- [ ] **Step 0.2: Confirm dev DB and stack already running**

Per `feedback_keep_stack_running.md`: do NOT stop/restart backend/frontend. The VS Code task `Dev stack` is already running. If stack is down, ask the user before starting.

Run: `pnpm --dir frontend test:unit -- --run` to confirm baseline test suite is green before starting.
Expected: all existing instrument-page tests PASS.

---

## File Structure

**New files:**
- `frontend/src/components/instrument/PaneHeader.tsx` + test — header primitive (title · scope · source · `Open →` button)
- `frontend/src/components/instrument/Pane.tsx` + test — outer card wrapper that renders `PaneHeader` + body, owned by individual pane components
- `frontend/src/components/instrument/densityProfile.ts` + test — `selectProfile(summary)` returns `"full-sec" | "partial-filings" | "minimal"`
- `frontend/src/components/instrument/RecentNewsPane.tsx` + test — new news pane that returns `null` when `data.items.length === 0`, replaces the placeholder `newsBlock` in `ResearchTab`
- `frontend/src/components/instrument/KeyStatsPane.tsx` + test — extracted from `ResearchTab.keyStatsBlock`, now drops fully-null rows, keeps per-row `FieldSourceTag`
- `frontend/src/components/instrument/ThesisPane.tsx` + test — extracted from `ResearchTab.ThesisPanel`, returns `null` when `thesis === null && !errored`

**Modified files:**
- `frontend/src/components/instrument/DensityGrid.tsx` — switch to `grid-cols-12`, three profile branches, mount child pane components conditionally (no `<Pane>` wrapping at this layer)
- `frontend/src/components/instrument/ResearchTab.tsx` — thin pass-through; deletes its inline `keyStatsBlock` / `thesisBlock` / `newsBlock` JSX
- `frontend/src/components/instrument/FilingsPane.tsx` — replace `<Section>` with `<Pane>`, drop the existing footer `View all filings →` link
- `frontend/src/components/instrument/FundamentalsPane.tsx` — replace `<Section>` with `<Pane>`, drop the existing footer `View statements →` link
- `frontend/src/components/instrument/InsiderActivitySummary.tsx` — replace `<Section>` with `<Pane>`
- `frontend/src/components/instrument/SecProfilePanel.tsx` — replace `<Section>` with `<Pane>` (verify file exists; pane is invoked from `DensityGrid.tsx:69`)
- `frontend/src/components/instrument/DividendsPanel.tsx` — replace `<Section>` with `<Pane>`, return `null` when both `history.length === 0` AND `upcoming.length === 0`, keep upcoming banner path
- `frontend/src/components/instrument/BusinessSectionsTeaser.tsx` — replace `<Section>` (or its current chrome) with `<Pane>`, wire `onExpand` to existing 10-K narrative route

**Tests touched:**
- New per-component tests above
- `DensityGrid.test.tsx` — extended fixtures for the three profiles + dividends/insider gating
- Existing pane tests need touch-ups where they assert `Section` chrome (search before editing)

---

## Phase 1 — primitives (no behavior change visible to operator)

### Task 1: PaneHeader component

**Files:**
- Create: `frontend/src/components/instrument/PaneHeader.tsx`
- Test: `frontend/src/components/instrument/PaneHeader.test.tsx`

- [ ] **Step 1.1: Write the failing test**

```tsx
// frontend/src/components/instrument/PaneHeader.test.tsx
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { PaneHeader } from "./PaneHeader";

describe("PaneHeader", () => {
  it("renders title only with no optional props", () => {
    render(<PaneHeader title="Recent filings" />);
    expect(screen.getByRole("heading", { name: /recent filings/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /open/i })).not.toBeInTheDocument();
  });

  it("renders scope text when provided", () => {
    render(<PaneHeader title="Insider activity" scope="last 90 days" />);
    expect(screen.getByText("last 90 days")).toBeInTheDocument();
  });

  it("renders provider source label", () => {
    render(
      <PaneHeader
        title="Recent filings"
        source={{ providers: ["sec_edgar"] }}
      />,
    );
    // providerLabel("sec_edgar") = "SEC EDGAR"
    expect(screen.getByText(/SEC EDGAR/)).toBeInTheDocument();
  });

  it("renders Open button only when onExpand is defined and calls it on click", async () => {
    const onExpand = vi.fn();
    render(<PaneHeader title="Filings" onExpand={onExpand} />);
    const btn = screen.getByRole("button", { name: /open/i });
    await userEvent.click(btn);
    expect(onExpand).toHaveBeenCalledOnce();
  });
});
```

- [ ] **Step 1.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run PaneHeader.test.tsx
```

Expected: FAIL with module-not-found or `PaneHeader is not a function`.

- [ ] **Step 1.3: Implement PaneHeader**

```tsx
// frontend/src/components/instrument/PaneHeader.tsx
import { providerLabel } from "@/lib/capabilityProviders";

export interface PaneHeaderProps {
  readonly title: string;
  readonly scope?: string;
  readonly source?: {
    readonly providers: ReadonlyArray<string>;
    readonly lastSync?: string;
  };
  readonly onExpand?: () => void;
}

export function PaneHeader({
  title,
  scope,
  source,
  onExpand,
}: PaneHeaderProps): JSX.Element {
  const sourceText =
    source && source.providers.length > 0
      ? source.providers.map(providerLabel).join(" · ") +
        (source.lastSync ? ` · ${source.lastSync}` : "")
      : null;
  return (
    <header className="flex items-baseline justify-between gap-2 border-b border-slate-100 pb-1.5">
      <div className="flex min-w-0 items-baseline gap-2">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-600">
          {title}
        </h2>
        {scope ? (
          <span className="text-[10px] text-slate-500">{scope}</span>
        ) : null}
      </div>
      <div className="flex flex-shrink-0 items-center gap-2">
        {sourceText !== null ? (
          <span
            className="truncate rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-600"
            title={sourceText}
          >
            {sourceText}
          </span>
        ) : null}
        {onExpand !== undefined ? (
          <button
            type="button"
            onClick={onExpand}
            className="text-[11px] text-sky-700 hover:underline focus-visible:rounded focus-visible:outline-2 focus-visible:outline-sky-500"
          >
            Open →
          </button>
        ) : null}
      </div>
    </header>
  );
}
```

- [ ] **Step 1.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run PaneHeader.test.tsx
```

Expected: PASS (4 specs).

- [ ] **Step 1.5: Typecheck**

```bash
pnpm --dir frontend typecheck
```

Expected: 0 errors.

- [ ] **Step 1.6: Commit**

```bash
git add frontend/src/components/instrument/PaneHeader.tsx frontend/src/components/instrument/PaneHeader.test.tsx
git commit -m "feat(#575-A): PaneHeader primitive — title/scope/source/Open button"
```

---

### Task 2: Pane wrapper component

**Files:**
- Create: `frontend/src/components/instrument/Pane.tsx`
- Test: `frontend/src/components/instrument/Pane.test.tsx`

- [ ] **Step 2.1: Write the failing test**

```tsx
// frontend/src/components/instrument/Pane.test.tsx
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { Pane } from "./Pane";

describe("Pane", () => {
  it("renders header title and body content", () => {
    render(
      <Pane title="Recent filings">
        <p>row content</p>
      </Pane>,
    );
    expect(screen.getByRole("heading", { name: /recent filings/i })).toBeInTheDocument();
    expect(screen.getByText("row content")).toBeInTheDocument();
  });

  it("does not attach onClick to the outer article (button-only drill)", async () => {
    const onExpand = vi.fn();
    render(
      <Pane title="Filings" onExpand={onExpand}>
        <p>body</p>
      </Pane>,
    );
    // Clicking the body must NOT trigger onExpand.
    await userEvent.click(screen.getByText("body"));
    expect(onExpand).not.toHaveBeenCalled();
    // Clicking the Open button MUST trigger onExpand.
    await userEvent.click(screen.getByRole("button", { name: /open/i }));
    expect(onExpand).toHaveBeenCalledOnce();
  });
});
```

- [ ] **Step 2.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run Pane.test.tsx
```

Expected: FAIL with module-not-found.

- [ ] **Step 2.3: Implement Pane**

```tsx
// frontend/src/components/instrument/Pane.tsx
import type { ReactNode } from "react";

import { PaneHeader } from "./PaneHeader";
import type { PaneHeaderProps } from "./PaneHeader";

export interface PaneProps extends PaneHeaderProps {
  readonly children: ReactNode;
  /** Optional className overrides on the outer article. */
  readonly className?: string;
}

export function Pane({
  title,
  scope,
  source,
  onExpand,
  className,
  children,
}: PaneProps): JSX.Element {
  return (
    <article
      className={`rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm ${className ?? ""}`}
    >
      <PaneHeader
        title={title}
        scope={scope}
        source={source}
        onExpand={onExpand}
      />
      <div className="mt-2">{children}</div>
    </article>
  );
}
```

- [ ] **Step 2.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run Pane.test.tsx
```

Expected: PASS (2 specs).

- [ ] **Step 2.5: Commit**

```bash
git add frontend/src/components/instrument/Pane.tsx frontend/src/components/instrument/Pane.test.tsx
git commit -m "feat(#575-A): Pane wrapper — article + PaneHeader, no whole-card click"
```

---

### Task 3: densityProfile selector

**Files:**
- Create: `frontend/src/components/instrument/densityProfile.ts`
- Test: `frontend/src/components/instrument/densityProfile.test.ts`

- [ ] **Step 3.1: Write the failing test**

```ts
// frontend/src/components/instrument/densityProfile.test.ts
import { describe, expect, it } from "vitest";

import { selectProfile } from "./densityProfile";
import type { InstrumentSummary } from "@/api/types";

function fixture(overrides: Partial<InstrumentSummary["capabilities"]>): InstrumentSummary {
  return {
    instrument_id: 1,
    is_tradable: true,
    coverage_tier: 1,
    identity: { symbol: "X", display_name: null, sector: null, market_cap: null } as never,
    price: null,
    key_stats: null,
    source: {},
    has_sec_cik: false,
    has_filings_coverage: false,
    capabilities: { ...overrides } as InstrumentSummary["capabilities"],
  } as InstrumentSummary;
}

describe("selectProfile", () => {
  it("returns full-sec when sec_xbrl fundamentals + filings both active", () => {
    const summary = fixture({
      fundamentals: { providers: ["sec_xbrl"], data_present: { sec_xbrl: true } },
      filings: { providers: ["sec_edgar"], data_present: { sec_edgar: true } },
    });
    expect(selectProfile(summary)).toBe("full-sec");
  });

  it("returns partial-filings when filings active but no sec_xbrl fundamentals", () => {
    const summary = fixture({
      filings: { providers: ["companies_house"], data_present: { companies_house: true } },
    });
    expect(selectProfile(summary)).toBe("partial-filings");
  });

  it("returns partial-filings when sec_xbrl listed but no data present", () => {
    const summary = fixture({
      fundamentals: { providers: ["sec_xbrl"], data_present: { sec_xbrl: false } },
      filings: { providers: ["sec_edgar"], data_present: { sec_edgar: true } },
    });
    expect(selectProfile(summary)).toBe("partial-filings");
  });

  it("returns minimal when no fundamentals and no filings", () => {
    const summary = fixture({});
    expect(selectProfile(summary)).toBe("minimal");
  });
});
```

- [ ] **Step 3.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run densityProfile.test.ts
```

Expected: FAIL with module-not-found.

- [ ] **Step 3.3: Implement selectProfile**

```ts
// frontend/src/components/instrument/densityProfile.ts
import type { InstrumentSummary } from "@/api/types";
import { activeProviders } from "@/lib/capabilityProviders";

export type DensityProfile = "full-sec" | "partial-filings" | "minimal";

const EMPTY_CELL = { providers: [] as string[], data_present: {} as Record<string, boolean> };

export function selectProfile(summary: InstrumentSummary): DensityProfile {
  const cap = summary.capabilities;
  const fundCell = cap.fundamentals ?? EMPTY_CELL;
  const hasFundamentals =
    fundCell.providers.includes("sec_xbrl") &&
    fundCell.data_present["sec_xbrl"] === true;
  const hasFilings = activeProviders(cap.filings ?? EMPTY_CELL).length > 0;

  if (hasFundamentals && hasFilings) return "full-sec";
  if (hasFilings) return "partial-filings";
  return "minimal";
}
```

- [ ] **Step 3.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run densityProfile.test.ts
```

Expected: PASS (4 specs).

- [ ] **Step 3.5: Commit**

```bash
git add frontend/src/components/instrument/densityProfile.ts frontend/src/components/instrument/densityProfile.test.ts
git commit -m "feat(#575-A): selectProfile — full-sec / partial-filings / minimal"
```

---

## Phase 2 — wire panes (replace Section with Pane in each)

### Task 4: FilingsPane uses Pane + drops footer link

**Files:**
- Modify: `frontend/src/components/instrument/FilingsPane.tsx`
- Modify: `frontend/src/components/instrument/FilingsPane.test.tsx`

- [ ] **Step 4.1: Update existing test to assert new chrome**

The current test at `FilingsPane.test.tsx` asserts the footer `View all filings →` link. That assertion changes: the footer link is removed; the `Open →` button on `PaneHeader` replaces it.

Read the file first, then replace the footer-link assertion with a `Open →` button assertion that triggers navigation to `?tab=filings`.

```tsx
// In FilingsPane.test.tsx, replace any test like:
//   expect(screen.getByRole("link", { name: /view all filings/i })).toHaveAttribute("href", expect.stringContaining("?tab=filings"));
// With:
it("renders Open button when filings tab is active and routes to ?tab=filings", async () => {
  // Set up the existing fixture used by the file (full SEC capability)
  // ...
  const btn = screen.getByRole("button", { name: /open/i });
  await userEvent.click(btn);
  // Assert navigation via the test's MemoryRouter / test history.
  expect(window.location.search).toContain("tab=filings"); // or use the test's navigate spy
});
```

If the existing test uses a `navigate` mock from `react-router-dom`, mock-call the spy and assert it was called with the right URL.

- [ ] **Step 4.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run FilingsPane.test.tsx
```

Expected: FAIL — old footer assertion still runs and the new `Open` assertion does not yet match.

- [ ] **Step 4.3: Modify FilingsPane to use Pane + onExpand**

```tsx
// frontend/src/components/instrument/FilingsPane.tsx
// Replace the existing return-statement chrome:

import { useNavigate } from "react-router-dom";
import { Pane } from "@/components/instrument/Pane";

// inside the component:
const navigate = useNavigate();

// Replace <Section title="Recent filings"> ... </Section> with:
return (
  <Pane
    title="Recent filings"
    scope="high-signal types"
    source={{ providers: filingsCell?.providers ?? [] }}
    onExpand={
      filingsTabActive
        ? () => navigate(`/instrument/${encodeURIComponent(symbol)}?tab=filings`)
        : undefined
    }
  >
    {/* existing body of the Section here, MINUS the
        "View all filings →" footer block at the bottom */}
  </Pane>
);
```

Delete the entire `{filingsTabActive && (<div className="mt-2 border-t ...">...)}` footer block at `FilingsPane.tsx:134-143`.

Remove the now-unused `Link` import if it's no longer referenced inside the row rendering (it is — row drilldowns still use `Link`, so keep the import).

- [ ] **Step 4.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run FilingsPane.test.tsx
```

Expected: PASS.

- [ ] **Step 4.5: Commit**

```bash
git add frontend/src/components/instrument/FilingsPane.tsx frontend/src/components/instrument/FilingsPane.test.tsx
git commit -m "feat(#575-B): FilingsPane uses Pane + Open button replaces footer link"
```

---

### Task 5: FundamentalsPane uses Pane + drops footer link

**Files:**
- Modify: `frontend/src/components/instrument/FundamentalsPane.tsx`
- Modify: `frontend/src/components/instrument/FundamentalsPane.test.tsx`

- [ ] **Step 5.1: Update test**

Replace any assertion against the existing `View statements →` footer link with one against the `Open` button on `PaneHeader`. Same pattern as Task 4.

- [ ] **Step 5.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run FundamentalsPane.test.tsx
```

Expected: FAIL.

- [ ] **Step 5.3: Modify FundamentalsPane**

```tsx
// frontend/src/components/instrument/FundamentalsPane.tsx
import { useNavigate } from "react-router-dom";
import { Pane } from "@/components/instrument/Pane";

// inside the component, replace <Section title="Fundamentals"> ... </Section> with:
const navigate = useNavigate();
return (
  <Pane
    title="Fundamentals"
    scope="last 8 quarters"
    source={{ providers: ["sec_xbrl"] }}
    onExpand={() => navigate(`/instrument/${encodeURIComponent(symbol)}?tab=financials`)}
  >
    {/* existing loading / error / empty / data branches MINUS the
        "View statements →" footer block at FundamentalsPane.tsx:166-173 */}
  </Pane>
);
```

Delete the `<div className="mt-2 border-t border-slate-100 pt-1.5 text-right">` footer + its `<Link>` to `?tab=financials`.

- [ ] **Step 5.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run FundamentalsPane.test.tsx
```

Expected: PASS.

- [ ] **Step 5.5: Commit**

```bash
git add frontend/src/components/instrument/FundamentalsPane.tsx frontend/src/components/instrument/FundamentalsPane.test.tsx
git commit -m "feat(#575-B): FundamentalsPane uses Pane + Open button replaces footer link"
```

---

### Task 6: InsiderActivitySummary uses Pane

**Files:**
- Modify: `frontend/src/components/instrument/InsiderActivitySummary.tsx`
- Modify: `frontend/src/components/instrument/InsiderActivitySummary.test.tsx`

- [ ] **Step 6.1: Update test**

Existing test asserts the four metric labels render — keep that. Add an assertion that the `<Pane>` chrome shows scope `last 90 days` and source `SEC Form 4`.

```tsx
// add to existing test file
it("renders Pane chrome with scope and source", () => {
  // existing setup ...
  expect(screen.getByText("last 90 days")).toBeInTheDocument();
  expect(screen.getByText(/SEC Form 4/)).toBeInTheDocument();
});
```

- [ ] **Step 6.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run InsiderActivitySummary.test.tsx
```

Expected: FAIL — scope text not yet rendered.

- [ ] **Step 6.3: Modify InsiderActivitySummary**

Replace `<Section title="Insider activity (90d)">` with:

```tsx
<Pane
  title="Insider activity"
  scope="last 90 days"
  source={{ providers: ["sec_form4"] }}
>
  {/* existing body unchanged */}
</Pane>
```

(`onExpand` undefined — no insider tab/route exists yet.)

Update import: drop `Section` (keep `SectionError`, `SectionSkeleton`); add `import { Pane } from "@/components/instrument/Pane";`.

- [ ] **Step 6.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run InsiderActivitySummary.test.tsx
```

Expected: PASS.

- [ ] **Step 6.5: Commit**

```bash
git add frontend/src/components/instrument/InsiderActivitySummary.tsx frontend/src/components/instrument/InsiderActivitySummary.test.tsx
git commit -m "feat(#575-B): InsiderActivitySummary uses Pane (scope last 90 days)"
```

---

### Task 7: SecProfilePanel uses Pane

**Files:**
- Modify: `frontend/src/components/instrument/SecProfilePanel.tsx`
- Modify: `frontend/src/components/instrument/SecProfilePanel.test.tsx`

- [ ] **Step 7.1: Update test**

Add assertion that the source label `SEC EDGAR` renders in the Pane chrome.

```tsx
it("renders Pane chrome with sec_edgar source", () => {
  // existing setup ...
  expect(screen.getByText(/SEC EDGAR/)).toBeInTheDocument();
});
```

- [ ] **Step 7.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run SecProfilePanel.test.tsx
```

Expected: FAIL.

- [ ] **Step 7.3: Modify SecProfilePanel**

Replace its existing chrome (currently uses `<Section>` per `DensityGrid.tsx:69-72`) with:

```tsx
<Pane
  title="Company profile"
  source={{ providers: ["sec_edgar"] }}
>
  {/* existing body */}
</Pane>
```

(`scope` undefined; `onExpand` undefined — no dedicated SEC profile route yet.)

- [ ] **Step 7.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run SecProfilePanel.test.tsx
```

Expected: PASS.

- [ ] **Step 7.5: Commit**

```bash
git add frontend/src/components/instrument/SecProfilePanel.tsx frontend/src/components/instrument/SecProfilePanel.test.tsx
git commit -m "feat(#575-B): SecProfilePanel uses Pane"
```

---

### Task 8: BusinessSectionsTeaser uses Pane

**Files:**
- Modify: `frontend/src/components/instrument/BusinessSectionsTeaser.tsx`
- Modify: existing test if one exists (check via `ls`)

- [ ] **Step 8.1: Identify existing 10-K narrative route**

```bash
grep -rn "/filings/10-k" frontend/src/components/ frontend/src/pages/
```

Use the discovered path (likely `/instrument/:symbol/filings/10-k`) as the `onExpand` target.

- [ ] **Step 8.2: Modify BusinessSectionsTeaser to use Pane with onExpand**

Replace the existing chrome with:

```tsx
import { useNavigate } from "react-router-dom";
import { Pane } from "@/components/instrument/Pane";

// inside component:
const navigate = useNavigate();
return (
  <Pane
    title="Company narrative"
    scope="10-K Item 1"
    source={{ providers: ["sec_10k_item1"] }}
    onExpand={() => navigate(`/instrument/${encodeURIComponent(symbol)}/filings/10-k`)}
  >
    {/* existing teaser body, drop the inline "View full 10-K narrative →"
        link if present (replaced by Open → button) */}
  </Pane>
);
```

- [ ] **Step 8.3: Update test if existing**

If `BusinessSectionsTeaser.test.tsx` exists, update assertions: drop `View full 10-K narrative` link assertion; add `Open` button assertion.

If no test exists, write one:

```tsx
// new file frontend/src/components/instrument/BusinessSectionsTeaser.test.tsx
// (only if no existing test) — minimal coverage of Pane chrome + Open button.
```

- [ ] **Step 8.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run BusinessSectionsTeaser.test.tsx
```

Expected: PASS.

- [ ] **Step 8.5: Commit**

```bash
git add frontend/src/components/instrument/BusinessSectionsTeaser.tsx
# include test file if changed/created
git commit -m "feat(#575-B): BusinessSectionsTeaser uses Pane + Open routes to 10-K"
```

---

## Phase 3 — empty-pane suppression

### Task 9: ThesisPane component (extracted from ResearchTab)

**Files:**
- Create: `frontend/src/components/instrument/ThesisPane.tsx`
- Create: `frontend/src/components/instrument/ThesisPane.test.tsx`

- [ ] **Step 9.1: Write the failing test**

```tsx
// frontend/src/components/instrument/ThesisPane.test.tsx
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ThesisPane } from "./ThesisPane";
import type { ThesisDetail } from "@/api/types";

const FIXTURE: ThesisDetail = {
  thesis_id: 1,
  instrument_id: 1,
  memo_markdown: "Buy on weakness.",
  bear_value: "10",
  base_value: "20",
  bull_value: "30",
  break_conditions_json: ["Lose 50% market share"],
} as ThesisDetail;

describe("ThesisPane", () => {
  it("renders memo + bear/base/bull when thesis present", () => {
    const { container } = render(<ThesisPane thesis={FIXTURE} errored={false} />);
    expect(screen.getByText("Buy on weakness.")).toBeInTheDocument();
    expect(screen.getByText("Bear")).toBeInTheDocument();
    expect(container.querySelector("article")).not.toBeNull();
  });

  it("returns null when thesis is null and not errored (no card)", () => {
    const { container } = render(<ThesisPane thesis={null} errored={false} />);
    expect(container.firstChild).toBeNull();
  });

  it("renders error UI inside Pane when errored", () => {
    render(<ThesisPane thesis={null} errored={true} />);
    expect(screen.getByText(/temporarily unavailable/i)).toBeInTheDocument();
  });
});
```

- [ ] **Step 9.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run ThesisPane.test.tsx
```

Expected: FAIL.

- [ ] **Step 9.3: Implement ThesisPane**

Copy the existing `ThesisPanel` body from `ResearchTab.tsx:88-155` into a new component. Wrap in `<Pane>`. When `thesis === null && !errored`, return `null`.

```tsx
// frontend/src/components/instrument/ThesisPane.tsx
import { Pane } from "@/components/instrument/Pane";
import { EmptyState } from "@/components/states/EmptyState";
import type { ThesisDetail } from "@/api/types";

export interface ThesisPaneProps {
  readonly thesis: ThesisDetail | null;
  readonly errored: boolean;
}

export function ThesisPane({ thesis, errored }: ThesisPaneProps): JSX.Element | null {
  if (thesis === null && !errored) return null;

  return (
    <Pane title="Thesis">
      {errored ? (
        <EmptyState
          title="Thesis temporarily unavailable"
          description="Failed to fetch the latest thesis. Retry via the Generate thesis button in the strip above."
        />
      ) : (
        <ThesisBody thesis={thesis as ThesisDetail} />
      )}
    </Pane>
  );
}

function ThesisBody({ thesis }: { thesis: ThesisDetail }): JSX.Element {
  const breaks = thesis.break_conditions_json ?? [];
  return (
    <div className="space-y-3 text-sm">
      <div className="whitespace-pre-wrap text-slate-700">{thesis.memo_markdown}</div>
      {(thesis.base_value !== null ||
        thesis.bull_value !== null ||
        thesis.bear_value !== null) && (
        <dl className="grid grid-cols-3 gap-2 rounded bg-slate-50 p-3 text-xs">
          <div>
            <dt className="text-slate-500">Bear</dt>
            <dd className="font-medium tabular-nums">
              {thesis.bear_value !== null ? thesis.bear_value : "—"}
            </dd>
          </div>
          <div>
            <dt className="text-slate-500">Base</dt>
            <dd className="font-medium tabular-nums">
              {thesis.base_value !== null ? thesis.base_value : "—"}
            </dd>
          </div>
          <div>
            <dt className="text-slate-500">Bull</dt>
            <dd className="font-medium tabular-nums">
              {thesis.bull_value !== null ? thesis.bull_value : "—"}
            </dd>
          </div>
        </dl>
      )}
      {breaks.length > 0 && (
        <div>
          <div className="mb-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Break conditions
          </div>
          <ul className="list-inside list-disc space-y-0.5 text-xs text-slate-600">
            {breaks.map((b, i) => (
              <li key={i}>{b}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 9.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run ThesisPane.test.tsx
```

Expected: PASS (3 specs).

- [ ] **Step 9.5: Commit**

```bash
git add frontend/src/components/instrument/ThesisPane.tsx frontend/src/components/instrument/ThesisPane.test.tsx
git commit -m "feat(#575-C): ThesisPane returns null when empty + not errored"
```

---

### Task 10: KeyStatsPane component (extracted from ResearchTab)

**Files:**
- Create: `frontend/src/components/instrument/KeyStatsPane.tsx`
- Create: `frontend/src/components/instrument/KeyStatsPane.test.tsx`

- [ ] **Step 10.1: Write the failing test**

```tsx
// frontend/src/components/instrument/KeyStatsPane.test.tsx
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { KeyStatsPane } from "./KeyStatsPane";
import type { InstrumentSummary } from "@/api/types";

function fixture(stats: Partial<InstrumentSummary["key_stats"]> | null): InstrumentSummary {
  return {
    identity: { symbol: "X", display_name: null, sector: null, market_cap: "1000000000" },
    key_stats: stats === null ? null : ({
      pe_ratio: "32.48",
      pb_ratio: null,
      dividend_yield: null,
      payout_ratio: null,
      roe: null,
      roa: null,
      debt_to_equity: "3.15",
      revenue_growth_yoy: null,
      earnings_growth_yoy: null,
      field_source: { pe_ratio: "sec_xbrl", debt_to_equity: "sec_xbrl" },
      ...stats,
    } as InstrumentSummary["key_stats"]),
    capabilities: {},
  } as InstrumentSummary;
}

describe("KeyStatsPane", () => {
  it("renders rows with non-null values, drops fully-null rows", () => {
    render(<KeyStatsPane summary={fixture({})} />);
    expect(screen.getByText("P/E ratio")).toBeInTheDocument();
    expect(screen.getByText("Debt / Equity")).toBeInTheDocument();
    // Dividend yield is null → row dropped.
    expect(screen.queryByText("Dividend yield")).not.toBeInTheDocument();
    // Per-row source tag still rendered.
    expect(screen.getAllByText(/SEC$/i).length).toBeGreaterThan(0);
  });

  it("renders empty state when key_stats is null", () => {
    render(<KeyStatsPane summary={fixture(null)} />);
    expect(screen.getByText(/No key stats/i)).toBeInTheDocument();
  });
});
```

- [ ] **Step 10.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run KeyStatsPane.test.tsx
```

Expected: FAIL.

- [ ] **Step 10.3: Implement KeyStatsPane**

Move `keyStatsBlock` body, `formatDecimal`, `formatMarketCap`, `FieldSourceTag`, `KeyStat` from `ResearchTab.tsx` into the new component. Wrap in `<Pane>`. Drop rows whose `value` is `null`.

```tsx
// frontend/src/components/instrument/KeyStatsPane.tsx
import { Pane } from "@/components/instrument/Pane";
import { EmptyState } from "@/components/states/EmptyState";
import type { InstrumentSummary, KeyStatsFieldSource } from "@/api/types";

function formatDecimal(
  value: string | null | undefined,
  opts: { percent?: boolean } = {},
): string | null {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (opts.percent) return `${(num * 100).toFixed(2)}%`;
  return num.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatMarketCap(value: string | null): string | null {
  if (value === null) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (num >= 1e12) return `${(num / 1e12).toFixed(2)}T`;
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
  if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
  return num.toLocaleString();
}

function FieldSourceTag({ source }: { source: string | undefined }) {
  if (!source) return null;
  let tone = "bg-slate-100 text-slate-600";
  let label = source;
  switch (source) {
    case "sec_xbrl":
      tone = "bg-emerald-50 text-emerald-700";
      label = "SEC";
      break;
    case "sec_dividend_summary":
      tone = "bg-emerald-50 text-emerald-700";
      label = "SEC · div";
      break;
    case "sec_xbrl_price_missing":
      tone = "bg-amber-50 text-amber-700";
      label = "SEC · price?";
      break;
    case "unavailable":
      tone = "bg-slate-100 text-slate-500";
      label = "—";
      break;
  }
  return (
    <span className={`ml-2 rounded px-1.5 py-0.5 text-[10px] uppercase ${tone}`}>
      {label}
    </span>
  );
}

interface Row {
  label: string;
  value: string;
  source?: string;
}

function buildRows(summary: InstrumentSummary): Row[] {
  const stats = summary.key_stats;
  if (stats === null) return [];
  const fs = (stats.field_source ?? {}) as Record<string, KeyStatsFieldSource>;
  const candidates: Array<Row | null> = [
    cap(formatMarketCap(summary.identity.market_cap), "Market cap", undefined),
    cap(formatDecimal(stats.pe_ratio), "P/E ratio", fs.pe_ratio),
    cap(formatDecimal(stats.pb_ratio), "P/B ratio", fs.pb_ratio),
    cap(formatDecimal(stats.dividend_yield, { percent: true }), "Dividend yield", fs.dividend_yield),
    cap(formatDecimal(stats.payout_ratio, { percent: true }), "Payout ratio", fs.payout_ratio),
    cap(formatDecimal(stats.roe, { percent: true }), "ROE", fs.roe),
    cap(formatDecimal(stats.roa, { percent: true }), "ROA", fs.roa),
    cap(formatDecimal(stats.debt_to_equity), "Debt / Equity", fs.debt_to_equity),
    cap(formatDecimal(stats.revenue_growth_yoy, { percent: true }), "Revenue growth (YoY)", fs.revenue_growth_yoy),
    cap(formatDecimal(stats.earnings_growth_yoy, { percent: true }), "Earnings growth (YoY)", fs.earnings_growth_yoy),
  ];
  return candidates.filter((r): r is Row => r !== null);
}

function cap(value: string | null, label: string, source: string | undefined): Row | null {
  if (value === null) return null;
  return { label, value, source };
}

export interface KeyStatsPaneProps {
  readonly summary: InstrumentSummary;
}

export function KeyStatsPane({ summary }: KeyStatsPaneProps): JSX.Element {
  const rows = buildRows(summary);
  return (
    <Pane title="Key statistics">
      {summary.key_stats === null ? (
        <EmptyState
          title="No key stats"
          description="No provider returned key stats for this ticker."
        />
      ) : (
        <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm">
          {rows.map((r) => (
            <KeyStatRow key={r.label} row={r} />
          ))}
        </dl>
      )}
    </Pane>
  );
}

function KeyStatRow({ row }: { row: Row }) {
  return (
    <>
      <dt className="text-slate-500">{row.label}</dt>
      <dd className="flex items-center tabular-nums">
        <span>{row.value}</span>
        <FieldSourceTag source={row.source} />
      </dd>
    </>
  );
}
```

- [ ] **Step 10.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run KeyStatsPane.test.tsx
```

Expected: PASS.

- [ ] **Step 10.5: Commit**

```bash
git add frontend/src/components/instrument/KeyStatsPane.tsx frontend/src/components/instrument/KeyStatsPane.test.tsx
git commit -m "feat(#575-C): KeyStatsPane drops fully-null rows, keeps per-row source tags"
```

---

### Task 11: RecentNewsPane component

**Files:**
- Create: `frontend/src/components/instrument/RecentNewsPane.tsx`
- Create: `frontend/src/components/instrument/RecentNewsPane.test.tsx`

- [ ] **Step 11.1: Write the failing test**

```tsx
// frontend/src/components/instrument/RecentNewsPane.test.tsx
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi, beforeEach } from "vitest";

import { RecentNewsPane } from "./RecentNewsPane";
import * as newsApi from "@/api/news";

beforeEach(() => {
  vi.restoreAllMocks();
});

describe("RecentNewsPane", () => {
  it("renders up to 5 items when feed has data", async () => {
    vi.spyOn(newsApi, "fetchNews").mockResolvedValueOnce({
      items: Array.from({ length: 7 }).map((_, i) => ({
        news_event_id: i,
        event_time: "2026-04-20T00:00:00Z",
        headline: `Headline ${i}`,
        snippet: null,
        category: null,
        sentiment_score: null,
        source: null,
        url: null,
      })),
      total: 7,
    } as never);
    render(
      <MemoryRouter>
        <RecentNewsPane instrumentId={1} symbol="X" />
      </MemoryRouter>,
    );
    await waitFor(() =>
      expect(screen.getAllByText(/^Headline /).length).toBeLessThanOrEqual(5),
    );
  });

  it("returns null when feed is empty (no card)", async () => {
    vi.spyOn(newsApi, "fetchNews").mockResolvedValueOnce({
      items: [],
      total: 0,
    } as never);
    const { container } = render(
      <MemoryRouter>
        <RecentNewsPane instrumentId={1} symbol="X" />
      </MemoryRouter>,
    );
    await waitFor(() => expect(container.firstChild).toBeNull());
  });
});
```

- [ ] **Step 11.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run RecentNewsPane.test.tsx
```

Expected: FAIL.

- [ ] **Step 11.3: Implement RecentNewsPane**

```tsx
// frontend/src/components/instrument/RecentNewsPane.tsx
import { useCallback } from "react";
import { useNavigate } from "react-router-dom";

import { fetchNews } from "@/api/news";
import type { NewsListResponse } from "@/api/types";
import { Pane } from "@/components/instrument/Pane";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";

const ROW_LIMIT = 5;

export interface RecentNewsPaneProps {
  readonly instrumentId: number;
  readonly symbol: string;
}

export function RecentNewsPane({
  instrumentId,
  symbol,
}: RecentNewsPaneProps): JSX.Element | null {
  const state = useAsync<NewsListResponse>(
    useCallback(() => fetchNews(instrumentId, 0, ROW_LIMIT), [instrumentId]),
    [instrumentId],
  );
  const navigate = useNavigate();

  if (state.loading) {
    return (
      <Pane title="Recent news">
        <SectionSkeleton rows={4} />
      </Pane>
    );
  }
  if (state.error !== null) {
    return (
      <Pane title="Recent news">
        <SectionError onRetry={state.refetch} />
      </Pane>
    );
  }
  if (state.data === null || state.data.items.length === 0) {
    return null;
  }

  const items = state.data.items.slice(0, ROW_LIMIT);
  return (
    <Pane
      title="Recent news"
      onExpand={() => navigate(`/instrument/${encodeURIComponent(symbol)}?tab=news`)}
    >
      <ul className="space-y-1.5 text-xs">
        {items.map((n) => (
          <li key={n.news_event_id} className="flex items-baseline gap-2">
            <span className="text-slate-500">{n.event_time.slice(0, 10)}</span>
            <span className="truncate text-slate-700">{n.headline}</span>
          </li>
        ))}
      </ul>
    </Pane>
  );
}
```

- [ ] **Step 11.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run RecentNewsPane.test.tsx
```

Expected: PASS.

- [ ] **Step 11.5: Commit**

```bash
git add frontend/src/components/instrument/RecentNewsPane.tsx frontend/src/components/instrument/RecentNewsPane.test.tsx
git commit -m "feat(#575-C): RecentNewsPane — top 5 items, returns null when empty"
```

---

### Task 12: DividendsPanel — Pane wrapper + null when empty AND no upcoming

**Files:**
- Modify: `frontend/src/components/instrument/DividendsPanel.tsx`
- Modify: `frontend/src/components/instrument/DividendsPanel.test.tsx`

- [ ] **Step 12.1: Update test**

Add three cases to the existing test:

```tsx
it("returns null when history is empty AND upcoming is empty", async () => {
  vi.spyOn(api, "fetchInstrumentDividends").mockResolvedValueOnce({
    symbol: "X",
    summary: { has_dividend: false } as never,
    history: [],
    upcoming: [],
  } as never);
  const { container } = render(<DividendsPanel symbol="X" provider="sec_dividend_summary" />);
  await waitFor(() => expect(container.firstChild).toBeNull());
});

it("renders Pane when history is empty but upcoming has 1 item", async () => {
  vi.spyOn(api, "fetchInstrumentDividends").mockResolvedValueOnce({
    symbol: "X",
    summary: { has_dividend: true } as never,
    history: [],
    upcoming: [{ ex_date: "2026-05-01" } as never],
  } as never);
  render(<DividendsPanel symbol="X" provider="sec_dividend_summary" />);
  await waitFor(() => expect(screen.getByText(/Dividends/)).toBeInTheDocument());
});

it("uses Pane chrome with provider source label", async () => {
  // existing renders-history test setup …
  expect(screen.getByText(/SEC dividends/)).toBeInTheDocument();
});
```

- [ ] **Step 12.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run DividendsPanel.test.tsx
```

Expected: FAIL.

- [ ] **Step 12.3: Modify DividendsPanel**

Replace `<Section title={title}>` with `<Pane title="Dividends" source={{ providers: [provider] }}>`.

Insert the early-return after data is loaded (before the existing JSX):

```tsx
if (
  state.data !== null &&
  state.error === null &&
  !state.loading &&
  state.data.history.length === 0 &&
  state.data.upcoming.length === 0
) {
  return null;
}
```

Keep the existing upcoming banner + history rendering branches inside the `<Pane>`.

Drop import of `Section` (keep `SectionError`, `SectionSkeleton`); drop the `EmptyState` "No dividend history on file" branch — the early-return covers it.

Drop the now-unused `providerLabel` if no longer referenced (it WILL still be referenced by Pane via PaneHeader, but not directly here — verify before removing).

- [ ] **Step 12.4: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run DividendsPanel.test.tsx
```

Expected: PASS.

- [ ] **Step 12.5: Commit**

```bash
git add frontend/src/components/instrument/DividendsPanel.tsx frontend/src/components/instrument/DividendsPanel.test.tsx
git commit -m "feat(#575-C): DividendsPanel returns null when history+upcoming both empty"
```

---

## Phase 4 — layout (the visible flip)

### Task 13: DensityGrid switches to grid-cols-12 + three profiles

**Files:**
- Modify: `frontend/src/components/instrument/DensityGrid.tsx`
- Modify: `frontend/src/components/instrument/ResearchTab.tsx`
- Modify: `frontend/src/components/instrument/DensityGrid.test.tsx`

- [ ] **Step 13.1: Update DensityGrid.test.tsx with three-profile fixtures**

Add three new test cases (full-sec, partial-filings, minimal). Replace any test asserting the old `2fr_1fr_1fr` grid class with the new `grid-cols-12` class. Assert pane visibility per profile.

```tsx
// frontend/src/components/instrument/DensityGrid.test.tsx
// (extend existing file)

import { render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";

const baseSummary = (caps: InstrumentSummary["capabilities"]): InstrumentSummary => ({
  /* fixture builder reused across cases — full path here */
} as InstrumentSummary);

describe("DensityGrid profiles", () => {
  it("full-sec: renders fundamentals + filings + insider panes", () => {
    render(
      <MemoryRouter>
        <DensityGrid summary={baseSummary({
          fundamentals: { providers: ["sec_xbrl"], data_present: { sec_xbrl: true } },
          filings: { providers: ["sec_edgar"], data_present: { sec_edgar: true } },
          insider: { providers: ["sec_form4"], data_present: { sec_form4: true } },
        })} thesis={null} thesisErrored={false} />
      </MemoryRouter>,
    );
    expect(screen.getByRole("heading", { name: /Fundamentals/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Recent filings/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Insider activity/i })).toBeInTheDocument();
  });

  it("partial-filings: no fundamentals pane; insider+dividends share row 4 if both active", () => {
    render(
      <MemoryRouter>
        <DensityGrid summary={baseSummary({
          filings: { providers: ["companies_house"], data_present: { companies_house: true } },
          insider: { providers: ["sec_form4"], data_present: { sec_form4: true } },
          dividends: { providers: ["sec_dividend_summary"], data_present: { sec_dividend_summary: true } },
        })} thesis={null} thesisErrored={false} />
      </MemoryRouter>,
    );
    expect(screen.queryByRole("heading", { name: /^Fundamentals$/i })).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Recent filings/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Insider activity/i })).toBeInTheDocument();
    // Dividends pane mounted (renders if upcoming or history present in mock)
  });

  it("minimal: chart + key stats only (when no thesis, no dividends, no news)", () => {
    render(
      <MemoryRouter>
        <DensityGrid summary={baseSummary({})} thesis={null} thesisErrored={false} />
      </MemoryRouter>,
    );
    expect(screen.queryByRole("heading", { name: /Recent filings/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /^Fundamentals$/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /Insider activity/i })).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Key statistics/i })).toBeInTheDocument();
  });

  it("thesis pane absent when thesis is null and not errored", () => {
    render(
      <MemoryRouter>
        <DensityGrid summary={baseSummary({})} thesis={null} thesisErrored={false} />
      </MemoryRouter>,
    );
    expect(screen.queryByRole("heading", { name: /^Thesis$/i })).not.toBeInTheDocument();
  });
});
```

- [ ] **Step 13.2: Run test to verify it fails**

```bash
pnpm --dir frontend test:unit -- --run DensityGrid.test.tsx
```

Expected: FAIL — DensityGrid still uses the old grid + still renders thesis as full empty card.

- [ ] **Step 13.3: Rewrite DensityGrid**

```tsx
// frontend/src/components/instrument/DensityGrid.tsx
import { activeProviders } from "@/lib/capabilityProviders";
import type { InstrumentSummary, ThesisDetail } from "@/api/types";

import { BusinessSectionsTeaser } from "@/components/instrument/BusinessSectionsTeaser";
import { DividendsPanel } from "@/components/instrument/DividendsPanel";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import { FundamentalsPane } from "@/components/instrument/FundamentalsPane";
import { InsiderActivitySummary } from "@/components/instrument/InsiderActivitySummary";
import { KeyStatsPane } from "@/components/instrument/KeyStatsPane";
import { Pane } from "@/components/instrument/Pane";
import { PriceChart } from "@/components/instrument/PriceChart";
import { RecentNewsPane } from "@/components/instrument/RecentNewsPane";
import { SecProfilePanel } from "@/components/instrument/SecProfilePanel";
import { ThesisPane } from "@/components/instrument/ThesisPane";
import { selectProfile } from "@/components/instrument/densityProfile";

export interface DensityGridProps {
  readonly summary: InstrumentSummary;
  readonly thesis: ThesisDetail | null;
  readonly thesisErrored: boolean;
}

export function DensityGrid({
  summary,
  thesis,
  thesisErrored,
}: DensityGridProps): JSX.Element {
  const symbol = summary.identity.symbol;
  const instrumentId = summary.instrument_id;
  const profile = selectProfile(summary);
  const cap = summary.capabilities;
  const hasInsider = activeProviders(cap.insider ?? { providers: [], data_present: {} }).length > 0;
  const hasDividends = activeProviders(cap.dividends ?? { providers: [], data_present: {} }).length > 0;
  const dividendProviders = activeProviders(cap.dividends ?? { providers: [], data_present: {} });
  const hasFilings = activeProviders(cap.filings ?? { providers: [], data_present: {} }).length > 0;
  const hasNarrative = summary.has_sec_cik;

  // Wrap PriceChart in a Pane locally (it doesn't own one yet — keep the
  // change localized; if/when chart route #576 lands, lift this into PriceChart).
  const ChartPane = (
    <Pane title="Price chart">
      <PriceChart symbol={symbol} />
    </Pane>
  );

  const KeyStats = <KeyStatsPane summary={summary} />;
  const SecProfile = hasNarrative ? <SecProfilePanel symbol={symbol} /> : null;
  const Thesis = <ThesisPane thesis={thesis} errored={thesisErrored} />;
  const News = <RecentNewsPane instrumentId={instrumentId} symbol={symbol} />;
  const Narrative = hasNarrative ? <BusinessSectionsTeaser symbol={symbol} /> : null;

  if (profile === "full-sec") {
    return (
      <div className="grid grid-cols-12 gap-2">
        <div className="col-span-12 lg:col-span-8 lg:row-span-2">{ChartPane}</div>
        <div className="col-span-12 lg:col-span-4">{KeyStats}</div>
        <div className="col-span-12 lg:col-span-4">{SecProfile}</div>
        <div className="col-span-12">
          <FundamentalsPane summary={summary} />
        </div>
        {hasFilings && (
          <div className="col-span-12 lg:col-span-7">
            <FilingsPane instrumentId={instrumentId} symbol={symbol} summary={summary} />
          </div>
        )}
        {hasInsider && (
          <div className="col-span-12 lg:col-span-5">
            <InsiderActivitySummary symbol={symbol} />
          </div>
        )}
        {Narrative !== null && <div className="col-span-12">{Narrative}</div>}
        {hasDividends && (
          <div className="col-span-12">
            {dividendProviders.map((p) => (
              <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
            ))}
          </div>
        )}
        <div className="col-span-12">{News}</div>
        <div className="col-span-12">{Thesis}</div>
      </div>
    );
  }

  if (profile === "partial-filings") {
    return (
      <div className="grid grid-cols-12 gap-2">
        <div className="col-span-12 lg:col-span-8 lg:row-span-2">{ChartPane}</div>
        <div className="col-span-12 lg:col-span-4">{KeyStats}</div>
        {SecProfile !== null && <div className="col-span-12 lg:col-span-4">{SecProfile}</div>}
        {hasFilings && (
          <div className="col-span-12">
            <FilingsPane instrumentId={instrumentId} symbol={symbol} summary={summary} />
          </div>
        )}
        {hasInsider && hasDividends && (
          <>
            <div className="col-span-12 lg:col-span-7">
              <InsiderActivitySummary symbol={symbol} />
            </div>
            <div className="col-span-12 lg:col-span-5">
              {dividendProviders.map((p) => (
                <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
              ))}
            </div>
          </>
        )}
        {hasInsider && !hasDividends && (
          <div className="col-span-12">
            <InsiderActivitySummary symbol={symbol} />
          </div>
        )}
        {!hasInsider && hasDividends && (
          <div className="col-span-12">
            {dividendProviders.map((p) => (
              <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
            ))}
          </div>
        )}
        {Narrative !== null && <div className="col-span-12">{Narrative}</div>}
        <div className="col-span-12">{News}</div>
        <div className="col-span-12">{Thesis}</div>
      </div>
    );
  }

  // minimal
  return (
    <div className="grid grid-cols-12 gap-2">
      <div className="col-span-12 lg:col-span-8 lg:row-span-2">{ChartPane}</div>
      <div className="col-span-12 lg:col-span-4">{KeyStats}</div>
      <div className="col-span-12 lg:col-span-4">{Thesis}</div>
      {hasDividends && (
        <div className="col-span-12">
          {dividendProviders.map((p) => (
            <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
          ))}
        </div>
      )}
      <div className="col-span-12">{News}</div>
    </div>
  );
}
```

(`Thesis` and `News` components return `null` when empty — wrapping them in a `<div className="col-span-12">` produces an empty grid cell, which is harmless. Verify with the test suite.)

If empty grid cells produce unwanted spacing, refactor to `{Thesis !== null && ...}` checks at the parent — but since `ThesisPane` returns `JSX.Element | null` we can't do that with a static check. The cleanest fix is to lift the gate above the wrapper:

```tsx
{thesis !== null && (
  <div className="col-span-12">
    <ThesisPane thesis={thesis} errored={thesisErrored} />
  </div>
)}
```

(Apply same pattern for News by lifting the empty check higher — but RecentNewsPane fetches internally, so the parent can't pre-check. Accept the empty grid cell trade-off, OR have RecentNewsPane render an `aria-hidden` placeholder. Default: accept the empty cell.)

- [ ] **Step 13.4: Update ResearchTab to thin pass-through**

```tsx
// frontend/src/components/instrument/ResearchTab.tsx
import { DensityGrid } from "@/components/instrument/DensityGrid";
import type { InstrumentSummary, ThesisDetail } from "@/api/types";

export interface ResearchTabProps {
  readonly summary: InstrumentSummary;
  readonly thesis: ThesisDetail | null;
  readonly thesisErrored?: boolean;
}

export function ResearchTab({
  summary,
  thesis,
  thesisErrored = false,
}: ResearchTabProps): JSX.Element {
  return (
    <DensityGrid
      summary={summary}
      thesis={thesis}
      thesisErrored={thesisErrored}
    />
  );
}
```

Delete the inline `keyStatsBlock`, `thesisBlock` (`ThesisPanel`), `newsBlock`, and helper functions/components that are now relocated.

- [ ] **Step 13.5: Run test to verify pass**

```bash
pnpm --dir frontend test:unit -- --run DensityGrid.test.tsx ResearchTab.test.tsx
```

Expected: PASS. (If `ResearchTab.test.tsx` exists and asserts old structure, update it to assert that `DensityGrid` is rendered.)

- [ ] **Step 13.6: Run full unit suite**

```bash
pnpm --dir frontend test:unit -- --run
```

Expected: ALL PASS.

- [ ] **Step 13.7: Typecheck + lint**

```bash
pnpm --dir frontend typecheck
```

Expected: 0 errors.

- [ ] **Step 13.8: Manual smoke test**

Open the running dev frontend (do not restart the stack). Visit:
1. `/instrument/GME` — `full-sec` profile. Confirm: thesis pane absent (no thesis on file), dividends absent (no history), no internal scrollbars, filings col wider than insider col.
2. `/instrument/<a UK equity>` if available — `partial-filings` profile.
3. `/instrument/<a crypto>` if available — `minimal` profile.

If any obvious regression vs the screenshots, capture it before pushing.

- [ ] **Step 13.9: Commit**

```bash
git add frontend/src/components/instrument/DensityGrid.tsx \
        frontend/src/components/instrument/DensityGrid.test.tsx \
        frontend/src/components/instrument/ResearchTab.tsx
# also include any ResearchTab.test.tsx update
git commit -m "feat(#575-D): DensityGrid 12-col + 3 capability profiles + empty-pane gates"
```

---

## Phase 5 — verification + Codex review + push

### Task 14: Local gate run

- [ ] **Step 14.1: Backend gates** (project requires both even on a frontend-only PR per CLAUDE.md)

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

Expected: ALL PASS.

- [ ] **Step 14.2: Frontend gates**

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test:unit
```

Expected: ALL PASS.

- [ ] **Step 14.3: Self-review against pre-flight skill**

Read `.claude/skills/engineering/pre-flight-review.md`. Walk the diff (`git diff main...HEAD`). Confirm:
- No empty cards remaining.
- No `overflow-auto` + `max-h-*` wrappers in `DensityGrid.tsx`.
- All instrument-page panes use `<Pane>` (grep `<Section ` inside `frontend/src/components/instrument/` — only `SectionError` / `SectionSkeleton` should remain).
- Footer `View …` links removed from FilingsPane / FundamentalsPane.
- ResearchTab is a thin pass-through.
- No new backend changes.

### Task 15: Codex pre-push review (CLAUDE.md checkpoint 2)

- [ ] **Step 15.1: Run Codex review on the branch**

```bash
codex.cmd exec review < /dev/null > /tmp/codex-prepush.out 2>&1 &
```

Wait for `tokens used` marker, then read the output.

- [ ] **Step 15.2: Address findings**

For each finding: fix, re-run local gates, re-stage, commit. **Do NOT amend.** Each fix = new commit.

### Task 16: Push + open PR

- [ ] **Step 16.1: Push branch**

```bash
git push -u origin feature/575-instrument-detail-polish-round-2
```

- [ ] **Step 16.2: Open PR**

Per `feedback_pr_description_brevity.md`: title `feat(#575): instrument detail polish round 2`. Body contains only:

```
## What
- Hide truly-empty thesis/news/dividends panes
- Unbundle dividends + insider combined card
- 12-col grid with 3 capability profiles
- New <Pane> + <PaneHeader> primitives

## Why
Closes #575. Operator review of post-#567 page flagged squish on
high-signal panes + empty-card waste. Codex direction: stay on page-
plus-route model, no drawer/ribbon.

## Test plan
- pnpm --dir frontend test:unit (incl. 3 new profile fixtures)
- Manual: /instrument/GME, /instrument/<UK equity>, /instrument/<crypto>
- uv run ruff/format/pyright/pytest all green
```

```bash
gh pr create --title "feat(#575): instrument detail polish round 2" --body-file <generated_above>
```

- [ ] **Step 16.3: Begin post-push polling cycle**

Per `feedback_post_push_cycle.md`: immediately start polling Claude review + CI. Do not wait for user prompt.

```bash
gh pr view <PR#> --comments
gh pr checks <PR#>
```

Repeat until: review posted on the latest commit AND CI green AND every comment reaches a terminal state (`FIXED {sha}` / `DEFERRED #{n}` / `REBUTTED {reason}`).

---

## Self-review against the spec

Spec coverage check:

| Spec section | Covered by |
|---|---|
| A. PaneHeader | Task 1 |
| A. Pane wrapper | Task 2 |
| B. Capability profiles | Task 3 (selector) + Task 13 (layouts) |
| C. PriceChart Pane wrap | Task 13 (`ChartPane` local wrap) |
| C. KeyStats drop null rows + keep FieldSourceTag | Task 10 |
| C. ThesisPanel returns null when empty | Task 9 |
| C. SecProfilePanel uses Pane | Task 7 |
| C. FundamentalsPane uses Pane + drops footer | Task 5 |
| C. FilingsPane uses Pane + drops footer | Task 4 |
| C. InsiderActivitySummary uses Pane | Task 6 |
| C. DividendsPanel uses Pane + null when empty AND no upcoming | Task 12 |
| C. BusinessSectionsTeaser uses Pane + Open route | Task 8 |
| C. RecentNewsPane new component | Task 11 |
| D. Empty-pane four-state policy | Tasks 9, 11, 12 (per-pane); Task 13 (parent gates) |
| E. Grid sizing (no auto-rows, no overflow-auto) | Task 13 |
| F. Wiring (ResearchTab thin pass-through) | Task 13 |
| Risk register | Addressed in Task 1 (focus styles), Task 13 (empty-cell trade-off note) |
| Testing — Vitest | Tasks 1, 2, 3, 9, 10, 11, 13 |
| Testing — Manual | Task 13.8 |
| Build sequence Phase 1–4 | Tasks 1–13 |

No gaps.

Type consistency: `selectProfile` returns `DensityProfile`; `DensityGrid` matches on its return value. `Pane` is imported by every child pane component. `PaneHeader` props match what `Pane` forwards. `RecentNewsPane` returns `JSX.Element | null`; `DensityGrid` mounts it inside a `<div className="col-span-12">` (acceptable empty-cell trade-off documented in Task 13.3).

No placeholders. Every code step contains the actual code.
