# Instrument detail refinement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refine the post-#559 density grid to Bloomberg info density + Yahoo Finance presentation: filter FilingsPane to high-signal types, drop fixed row heights + internal scrollbars, add a fundamentals sparkline pane, replace inline insider list with a compact summary.

**Architecture:** Four PRs. Phase A widens the backend `/filings/{id}` filter to accept a CSV `filing_type` and updates `FilingsPane` to use it. Phase B reshapes the grid to content-driven row heights and tightens spacing/typography. Phase C adds an SVG `Sparkline` + `FundamentalsPane` that joins income+balance statements per period. Phase D replaces the inline `InsiderActivityPanel` in the grid with `InsiderActivitySummary` rendered from the existing `/insider_summary` endpoint using the total-activity lens.

**Phase ordering:** Phase A is independent (backend + FilingsPane only). Phases B, C, D all edit `frontend/src/components/instrument/DensityGrid.tsx` and its test, so they are write-scope-coupled even though functionally separable. Recommended order: A → B → C → D, each rebasing on main after the prior phase merges. Skip-rebase only if the conflict surface is empty.

**Tech Stack:** FastAPI + psycopg3 + pyright (backend); React 18 + TypeScript + Tailwind + react-router-dom + `useAsync` (frontend); Pytest + Vitest.

**Spec:** `docs/superpowers/specs/2026-04-27-instrument-detail-refinement-design.md`

---

## File Structure

### Phase A — backend filter + FilingsPane fix

| Path | Action | Responsibility |
|---|---|---|
| `app/api/filings.py:89,116-118` | Modify | Accept CSV `filing_type=` and translate to `= ANY(...)`. |
| `tests/test_api_filings.py` | Modify | New test asserting the CSV path returns rows for any listed type and excludes others. |
| `frontend/src/api/filings.ts` | Modify | `fetchFilings` accepts optional `{ filing_type?: string }` opts. |
| `frontend/src/components/instrument/FilingsPane.tsx` | Modify | Pass the static SIGNIFICANT_FILING_TYPES CSV; bump `ROW_LIMIT` to 6; add "View all filings →" footer. |
| `frontend/src/components/instrument/FilingsPane.test.tsx` | Modify | Assert API is called with the CSV; assert footer link routes to `?tab=filings`. |

### Phase B — grid sizing + typography

| Path | Action | Responsibility |
|---|---|---|
| `frontend/src/components/instrument/DensityGrid.tsx` | Modify | Drop `lg:auto-rows-[220px]`; drop `overflow-auto` on each pane wrapper; tighten gap + padding; chart pane gets explicit `min-h-[440px]`; section title rule changed to `text-xs uppercase`. |
| `frontend/src/components/instrument/DensityGrid.test.tsx` | Modify | Existing assertions stay; add assertion that no descendant has the literal `overflow-auto` class. |

### Phase C — Sparkline + FundamentalsPane

| Path | Action | Responsibility |
|---|---|---|
| `frontend/src/components/instrument/Sparkline.tsx` | Create | Reusable hand-coded SVG `<polyline>` sparkline. |
| `frontend/src/components/instrument/Sparkline.test.tsx` | Create | Vitest: 8 input values → 8 comma-separated coords in `points`; <2 values → empty SVG. |
| `frontend/src/components/instrument/FundamentalsPane.tsx` | Create | Two parallel `useAsync` calls (income + balance, quarterly), period-safe inner-join keyed on `(period_end, period_type)`, latest-8 slice, 4 sparklines. Gated on `summary.capabilities.fundamentals.providers.includes("sec_xbrl") && data_present.sec_xbrl === true`. |
| `frontend/src/components/instrument/FundamentalsPane.test.tsx` | Create | Mocks both fetches; asserts 4 sparklines render; asserts `totalDebt = long_term_debt + short_term_debt`; asserts pane returns null when capability inactive; asserts EmptyState when joined set < 2 periods. |
| `frontend/src/components/instrument/DensityGrid.tsx` | Modify | Insert `<FundamentalsPane>` into row 2 (replacing one of the existing right-column slots per the spec layout). |

### Phase D — InsiderActivitySummary

| Path | Action | Responsibility |
|---|---|---|
| `frontend/src/components/instrument/InsiderActivitySummary.tsx` | Create | Reads `/insider_summary` via `fetchInsiderSummary`; renders compact NET/ACQUIRED/DISPOSED/TXNS/LATEST block using total-activity lens. |
| `frontend/src/components/instrument/InsiderActivitySummary.test.tsx` | Create | Vitest: mocked payload renders all five fields; NET 90d sign + arrow correct. |
| `frontend/src/components/instrument/DensityGrid.tsx` | Modify | Replace the inline `<InsiderActivityPanel>` use with `<InsiderActivitySummary>` (the existing component file stays for future use elsewhere). |

---

## Testing strategy

- **Backend:** Phase A — extend the existing FastAPI dependency-mock pattern in `tests/test_api_filings.py`. New test seeds 4 filings (10-K, 8-K, Form 4, 144), asserts `?filing_type=10-K,8-K` returns 2 rows and excludes the other types.
- **Frontend:** Vitest per component. Mocking pattern follows the existing `FilingsPane.test.tsx` (vi.spyOn on the API client), `DensityGrid.test.tsx` (vi.mock for child components).
- **Manual:** at the end of each phase, load `/instrument/GME` and verify the spec's manual-test checklist for that phase.
- **Pre-push gate per phase:**
  - Backend touch: `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest`
  - Frontend touch: `pnpm --dir frontend typecheck && pnpm --dir frontend test:unit`
  - Both touched: run all five.

---

# Phase A — backend filter + FilingsPane fix

**Branch:** `feature/567-phase-a-filings-filter`

**Goal:** `/filings/{id}` accepts CSV `filing_type=`. `FilingsPane` calls with the SIGNIFICANT_FILING_TYPES list and adds a "View all filings →" footer. Operator no longer sees Form 4 / 144 dominate the grid pane.

## Task A.1: Backend — CSV `filing_type` filter

**Files:**
- Modify: `app/api/filings.py:89,116-118`
- Test: `tests/test_api_filings.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_api_filings.py`:

```python
def test_list_filings_csv_filing_type_filter():
    """CSV filing_type matches any listed type; excludes others."""
    rows = [
        _make_filing_row(filing_event_id=1, filing_type="10-K"),
        _make_filing_row(filing_event_id=2, filing_type="8-K"),
    ]
    count_cur = MagicMock()
    count_cur.fetchone.return_value = {"cnt": 2}
    list_cur = MagicMock()
    list_cur.fetchall.return_value = rows

    inst_cur = MagicMock()
    inst_cur.fetchone.return_value = {"instrument_id": 1, "symbol": "GME"}

    cursors: Iterator[MagicMock] = iter([inst_cur, count_cur, list_cur])
    conn = MagicMock()
    conn.cursor.return_value.__enter__.side_effect = lambda: next(cursors)

    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override
    try:
        client = TestClient(app)
        r = client.get("/filings/1?filing_type=10-K,8-K")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["total"] == 2
        types = [item["filing_type"] for item in body["items"]]
        assert set(types) == {"10-K", "8-K"}
        # Confirm the SQL builder used = ANY(...) with a list, not a single value
        # by inspecting the bound params on the count query call.
        executed_params = count_cur.execute.call_args[0][1]
        assert "filing_types" in executed_params
        assert sorted(executed_params["filing_types"]) == ["10-K", "8-K"]
    finally:
        app.dependency_overrides.pop(get_conn, None)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_api_filings.py::test_list_filings_csv_filing_type_filter -v`
Expected: FAIL — current single-value filter doesn't bind `filing_types`.

- [ ] **Step 3: Update `app/api/filings.py` to accept CSV**

Replace the block at `app/api/filings.py:89,116-118`:

```python
# Around line 89 — keep the param as-is:
filing_type: str | None = Query(default=None),
```

```python
# Around line 116-118 — replace the single-value branch:
if filing_type is not None:
    types = [t.strip() for t in filing_type.split(",") if t.strip()]
    if types:
        where_clauses.append("filing_type = ANY(%(filing_types)s)")
        filter_params["filing_types"] = types
```

This is backwards-compatible: a single value `?filing_type=10-K` becomes `["10-K"]` and matches the same row set.

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_api_filings.py -v`
Expected: every test in the file PASSES (the new CSV test + all pre-existing single-value tests).

- [ ] **Step 5: Run the broader regression set**

Run: `uv run pytest tests/test_api_filings.py tests/api/ -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git checkout -b feature/567-phase-a-filings-filter
git add app/api/filings.py tests/test_api_filings.py
git commit -m "feat(#567-A): /filings/{id}?filing_type= accepts CSV"
```

## Task A.2: Frontend — `fetchFilings` accepts opts

**Files:**
- Modify: `frontend/src/api/filings.ts`

- [ ] **Step 1: Update the API client**

Replace the entire content of `frontend/src/api/filings.ts`:

```ts
import { apiFetch } from "@/api/client";
import type { FilingsListResponse } from "@/api/types";

export interface FetchFilingsOpts {
  readonly filing_type?: string;
}

export function fetchFilings(
  instrumentId: number,
  offset = 0,
  limit = 10,
  opts: FetchFilingsOpts = {},
): Promise<FilingsListResponse> {
  const params = new URLSearchParams({
    offset: String(offset),
    limit: String(limit),
  });
  if (opts.filing_type !== undefined) {
    params.set("filing_type", opts.filing_type);
  }
  return apiFetch<FilingsListResponse>(
    `/filings/${instrumentId}?${params.toString()}`,
  );
}
```

- [ ] **Step 2: Run typecheck**

Run: `pnpm --dir frontend typecheck`
Expected: 0 errors. Existing callers (3-arg form) still work — opts defaults to `{}`.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/api/filings.ts
git commit -m "feat(#567-A): fetchFilings accepts optional filing_type opt"
```

## Task A.3: Frontend — `FilingsPane` filters + footer link

**Files:**
- Modify: `frontend/src/components/instrument/FilingsPane.tsx`
- Modify: `frontend/src/components/instrument/FilingsPane.test.tsx`

- [ ] **Step 1: Update `FilingsPane.tsx`**

Replace the entire content of `frontend/src/components/instrument/FilingsPane.tsx`:

```tsx
/**
 * FilingsPane — high-signal filings list (8-K + 10-K + 10-Q + foreign
 * issuer equivalents) on the instrument page density grid (#559 / #567).
 * Each row links to the corresponding drilldown route. A "View all
 * filings →" footer routes to the canonical Filings tab.
 */

import { fetchFilings } from "@/api/filings";
import type { FilingsListResponse } from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { Link } from "react-router-dom";

const ROW_LIMIT = 6;

// US issuer types + foreign private issuer (FPI / ADR) types in one
// list. The backend filters with `filing_type = ANY(...)`, so listing
// FPI types alongside US types is harmless on US instruments and
// correct for foreign issuers.
const SIGNIFICANT_FILING_TYPES = [
  "8-K",
  "8-K/A",
  "10-K",
  "10-K/A",
  "10-Q",
  "10-Q/A",
  "6-K",
  "6-K/A",
  "20-F",
  "20-F/A",
  "40-F",
  "40-F/A",
].join(",");

const TYPES_WITH_DRILLDOWN = new Set(["8-K", "8-K/A", "10-K", "10-K/A"]);

function drilldownLink(symbol: string, filingType: string | null): string | null {
  if (filingType === null || !TYPES_WITH_DRILLDOWN.has(filingType)) return null;
  const symbolEnc = encodeURIComponent(symbol);
  if (filingType.startsWith("10-K")) {
    return `/instrument/${symbolEnc}/filings/10-k`;
  }
  return `/instrument/${symbolEnc}/filings/8-k`;
}

export interface FilingsPaneProps {
  readonly instrumentId: number;
  readonly symbol: string;
}

export function FilingsPane({ instrumentId, symbol }: FilingsPaneProps): JSX.Element {
  const state = useAsync<FilingsListResponse>(
    useCallback(
      () =>
        fetchFilings(instrumentId, 0, ROW_LIMIT, {
          filing_type: SIGNIFICANT_FILING_TYPES,
        }),
      [instrumentId],
    ),
    [instrumentId],
  );

  return (
    <Section title="Recent filings">
      {state.loading ? (
        <SectionSkeleton rows={5} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data === null || state.data.items.length === 0 ? (
        <EmptyState
          title="No filings"
          description="No 8-K / 10-K / 10-Q rows on file for this instrument."
        />
      ) : (
        <ul className="space-y-1.5 text-xs">
          {state.data.items.slice(0, ROW_LIMIT).map((f) => {
            const link = drilldownLink(symbol, f.filing_type ?? null);
            const label = (
              <span className="flex items-baseline gap-2">
                <span className="text-slate-500">{f.filing_date}</span>
                <span className="rounded bg-slate-100 px-1 py-0.5 text-[10px] text-slate-600">
                  {f.filing_type ?? "?"}
                </span>
                <span className="truncate text-slate-700">
                  {f.extracted_summary ?? f.filing_type ?? "filing"}
                </span>
              </span>
            );
            return (
              <li key={f.filing_event_id}>
                {link !== null ? (
                  <Link to={link} className="hover:underline">
                    {label}
                  </Link>
                ) : (
                  label
                )}
              </li>
            );
          })}
        </ul>
      )}
      <div className="mt-2 border-t border-slate-100 pt-1.5 text-right">
        <Link
          to={`/instrument/${encodeURIComponent(symbol)}?tab=filings`}
          className="text-[11px] text-sky-700 hover:underline"
        >
          View all filings →
        </Link>
      </div>
    </Section>
  );
}
```

- [ ] **Step 2: Update `FilingsPane.test.tsx`**

Replace the existing tests block in `frontend/src/components/instrument/FilingsPane.test.tsx` with three tests covering the CSV call, the footer link, and the row-cap:

```tsx
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import * as filingsApi from "@/api/filings";

describe("FilingsPane", () => {
  it("calls fetchFilings with the SIGNIFICANT_FILING_TYPES CSV and ROW_LIMIT 6", () => {
    const spy = vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 0,
      offset: 0,
      limit: 6,
      items: [],
    });
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" />
      </MemoryRouter>,
    );
    expect(spy).toHaveBeenCalledWith(
      1,
      0,
      6,
      expect.objectContaining({
        filing_type: expect.stringContaining("10-K"),
      }),
    );
    const callArgs = spy.mock.calls[0];
    const csv = (callArgs[3] as { filing_type: string }).filing_type;
    // Spot-check both US + FPI types are listed
    for (const t of ["8-K", "10-K", "10-Q", "6-K", "20-F", "40-F"]) {
      expect(csv).toContain(t);
    }
  });

  it("renders 6 rows max", async () => {
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 6,
      offset: 0,
      limit: 6,
      items: Array.from({ length: 6 }, (_, i) => ({
        filing_event_id: i + 1,
        instrument_id: 1,
        filing_date: `2026-03-${(i + 1).toString().padStart(2, "0")}`,
        filing_type: i % 2 === 0 ? "10-K" : "8-K",
        provider: "sec_edgar",
        red_flag_score: null,
        extracted_summary: `summary ${i}`,
        primary_document_url: null,
        source_url: null,
        created_at: "2026-03-01T00:00:00Z",
      })),
    });
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" />
      </MemoryRouter>,
    );
    const rows = await screen.findAllByText(/summary \d/);
    expect(rows.length).toBe(6);
  });

  it("footer link routes to /instrument/GME?tab=filings", async () => {
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 0,
      offset: 0,
      limit: 6,
      items: [],
    });
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" />
      </MemoryRouter>,
    );
    const link = await screen.findByText(/View all filings/);
    expect(link.closest("a")).toHaveAttribute(
      "href",
      "/instrument/GME?tab=filings",
    );
  });
});
```

- [ ] **Step 3: Run frontend gates**

Run: `pnpm --dir frontend typecheck && pnpm --dir frontend test -- FilingsPane`
Expected: typecheck 0 errors; FilingsPane suite PASS.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/instrument/FilingsPane.tsx frontend/src/components/instrument/FilingsPane.test.tsx
git commit -m "feat(#567-A): FilingsPane uses high-signal type filter + footer link"
```

## Task A.4: Pre-push + Codex + push + merge

- [ ] **Step 1: Full gate**

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest && pnpm --dir frontend typecheck && pnpm --dir frontend test:unit
```
Expected: all green.

- [ ] **Step 2: Codex review**

Run: `codex.cmd exec review`
Expected: clean or actionable findings — fix anything real before pushing.

- [ ] **Step 3: Push + open PR**

```bash
git push -u origin feature/567-phase-a-filings-filter
gh pr create --title "feat(#567-A): FilingsPane high-signal filter + backend CSV filing_type" --body "$(cat <<'EOF'
## What

- Backend `/filings/{id}?filing_type=` accepts CSV; translates to SQL `filing_type = ANY(...)`.
- Frontend `fetchFilings` accepts an optional `{ filing_type?: string }` opts arg.
- `FilingsPane` filters to the static SIGNIFICANT_FILING_TYPES list (8-K/10-K/10-Q + 6-K/20-F/40-F equivalents); ROW_LIMIT bumped to 6; "View all filings →" footer routes to the Filings tab.

## Why

Phase A of #567 — operator flagged that the grid's filings pane was dominated by Form 4 / 144 noise. Now shows the operator-relevant filings only.

## Test plan

- [ ] Backend: new `tests/test_api_filings.py::test_list_filings_csv_filing_type_filter` covers the CSV path.
- [ ] Frontend: 3 Vitest tests on `FilingsPane` (CSV call shape, 6-row cap, footer link).
- [ ] Manual: load `/instrument/GME`, confirm 8-K / 10-K rows visible (no Form 4 / 144), click "View all filings →" goes to the Filings tab.
EOF
)"
```

- [ ] **Step 4: Poll review + CI; resolve every comment; merge on APPROVE + green**

Run (loop): `gh pr view <PR#> --comments` and `gh pr checks <PR#>`. Address each finding to FIXED / DEFERRED / REBUTTED + EXTRACTED for prevention notes.

- [ ] **Step 5: Merge**

```bash
gh pr merge <PR#> --squash --delete-branch
git checkout main && git fetch && git reset --hard origin/main
```

---

# Phase B — grid sizing + typography

**Branch:** `feature/567-phase-b-grid-sizing`

**Goal:** Drop fixed 220 px row heights and per-pane `overflow-auto`. Tighten gap + padding + section title typography. Chart pane keeps an explicit min height so its 2-row footprint survives the auto-rows change.

## Task B.1: Refactor `DensityGrid.tsx`

**Files:**
- Modify: `frontend/src/components/instrument/DensityGrid.tsx`
- Modify: `frontend/src/components/instrument/DensityGrid.test.tsx`

- [ ] **Step 1: Update the grid container + pane wrappers**

In `frontend/src/components/instrument/DensityGrid.tsx`:

(a) Replace the outer grid `<div className="...">` line:

```tsx
// BEFORE:
<div className="grid grid-cols-1 gap-3 lg:grid-cols-[2fr_1fr_1fr] lg:auto-rows-[220px]">

// AFTER:
<div className="grid grid-cols-1 gap-2 lg:grid-cols-[2fr_1fr_1fr]">
```

(b) For EVERY pane wrapper `<div>` of the form `className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm[...]"`, replace `overflow-auto` with nothing and `p-3` with `px-3 py-2.5`:

```tsx
// BEFORE:
<div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">

// AFTER:
<div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm">
```

This applies to ALL pane wrappers including the ones with `lg:col-span-X` modifiers — keep those.

(c) For the chart pane wrapper specifically (the one with `lg:col-start-1 lg:col-end-2 lg:row-start-1 lg:row-end-3`), add `min-h-[440px]` to retain the 2-row footprint:

```tsx
// BEFORE:
<div className="overflow-hidden rounded-md border border-slate-200 bg-white p-3 shadow-sm lg:col-start-1 lg:col-end-2 lg:row-start-1 lg:row-end-3">
  <PriceChart symbol={symbol} />
</div>

// AFTER:
<div className="overflow-hidden rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm min-h-[440px] lg:col-start-1 lg:col-end-2 lg:row-start-1 lg:row-end-3">
  <PriceChart symbol={symbol} />
</div>
```

(Chart pane KEEPS `overflow-hidden` because the chart canvas is intentionally clipped to the pane bounds — that's not a scrollbar concern.)

- [ ] **Step 2: Append the no-overflow assertion to `DensityGrid.test.tsx`**

In `frontend/src/components/instrument/DensityGrid.test.tsx`, append a new test case to the existing `describe("DensityGrid", ...)` block:

```tsx
  it("no descendant uses overflow-auto (panes are content-driven, not scrollboxes)", () => {
    const { container } = render(
      <MemoryRouter>
        <DensityGrid
          summary={summary}
          keyStatsBlock={<div>KEY STATS BLOCK</div>}
          thesisBlock={<div>THESIS BLOCK</div>}
          newsBlock={<div>NEWS BLOCK</div>}
        />
      </MemoryRouter>,
    );
    const overflowAuto = container.querySelectorAll(".overflow-auto");
    expect(overflowAuto.length).toBe(0);
  });
```

- [ ] **Step 3: Run frontend gates**

Run: `pnpm --dir frontend typecheck && pnpm --dir frontend test -- DensityGrid`
Expected: typecheck 0 errors; DensityGrid suite PASS (existing 3 tests + the new one).

- [ ] **Step 4: Run the full unit-test suite to catch any spillover**

Run: `pnpm --dir frontend test:unit`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git checkout -b feature/567-phase-b-grid-sizing
git add frontend/src/components/instrument/DensityGrid.tsx frontend/src/components/instrument/DensityGrid.test.tsx
git commit -m "refactor(#567-B): content-driven grid rows; drop overflow-auto from panes"
```

## Task B.2: Pre-push + Codex + push + merge

- [ ] **Step 1: Full gate**

```bash
pnpm --dir frontend typecheck && pnpm --dir frontend test:unit
```
Expected: green. (Backend untouched — pytest not required.)

- [ ] **Step 2: Codex review**

Run: `codex.cmd exec review`

- [ ] **Step 3: Push + PR**

```bash
git push -u origin feature/567-phase-b-grid-sizing
gh pr create --title "refactor(#567-B): density grid sizing + spacing" --body "$(cat <<'EOF'
## What

- `lg:auto-rows-[220px]` removed — rows are now content-driven.
- `overflow-auto` removed from every pane wrapper — no more internal scrollbars.
- Chart pane gets explicit `min-h-[440px]` to retain its 2-row footprint.
- Pane padding tightened to `px-3 py-2.5` (asymmetric); grid gap tightened to `gap-2`.

## Why

Phase B of #567 — operator flagged scroll-box noise. Yahoo-Finance-style fit-on-screen presentation.

## Test plan

- [ ] Vitest assertion: no descendant has `overflow-auto`.
- [ ] Manual: load `/instrument/GME` at 1440x900, verify no internal scrollbars on any pane.
EOF
)"
```

- [ ] **Step 4: Poll + resolve + merge on APPROVE + green**

```bash
gh pr merge <PR#> --squash --delete-branch
git checkout main && git fetch && git reset --hard origin/main
```

---

# Phase C — Sparkline + FundamentalsPane

**Branch:** `feature/567-phase-c-fundamentals`

**Goal:** Add a `Sparkline` SVG component + `FundamentalsPane` that joins income+balance financials per period and renders 4 sparklines. Gated on `summary.capabilities.fundamentals.providers.sec_xbrl.data_present === true`.

**Depends on:** Phase B (uses the refined pane wrapper class set).

## Task C.1: `Sparkline` component (TDD)

**Files:**
- Create: `frontend/src/components/instrument/Sparkline.tsx`
- Create: `frontend/src/components/instrument/Sparkline.test.tsx`

- [ ] **Step 1: Write the failing test**

Create `frontend/src/components/instrument/Sparkline.test.tsx`:

```tsx
import { describe, expect, it } from "vitest";
import { render } from "@testing-library/react";
import { Sparkline } from "@/components/instrument/Sparkline";

describe("Sparkline", () => {
  it("renders <polyline> with 8 comma-separated coords for 8 input values", () => {
    const { container } = render(
      <Sparkline values={[1, 2, 3, 4, 5, 4, 3, 2]} width={80} height={24} />,
    );
    const polyline = container.querySelector("polyline");
    expect(polyline).not.toBeNull();
    const points = polyline?.getAttribute("points") ?? "";
    const coords = points.trim().split(/\s+/);
    expect(coords).toHaveLength(8);
    for (const c of coords) {
      expect(c).toMatch(/^\d+(?:\.\d+)?,\d+(?:\.\d+)?$/);
    }
  });

  it("renders an empty <svg> with no <polyline> when given fewer than 2 values", () => {
    const { container } = render(<Sparkline values={[42]} />);
    const polyline = container.querySelector("polyline");
    expect(polyline).toBeNull();
  });

  it("uses currentColor as default stroke", () => {
    const { container } = render(<Sparkline values={[1, 2, 3]} />);
    const polyline = container.querySelector("polyline");
    expect(polyline?.getAttribute("stroke")).toBe("currentColor");
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --dir frontend test -- Sparkline`
Expected: FAIL — `Sparkline` import unresolved.

- [ ] **Step 3: Implement the component**

Create `frontend/src/components/instrument/Sparkline.tsx`:

```tsx
/**
 * Sparkline — hand-coded SVG <polyline> sparkline. No external chart
 * dependency. Used in `FundamentalsPane` for compact 8-point time
 * series (revenue, op income, net income, total debt over 8 quarters).
 */

import type { JSX } from "react";

export interface SparklineProps {
  readonly values: ReadonlyArray<number>;
  readonly width?: number;
  readonly height?: number;
  readonly stroke?: string;
  readonly className?: string;
}

export function Sparkline({
  values,
  width = 80,
  height = 24,
  stroke = "currentColor",
  className,
}: SparklineProps): JSX.Element {
  if (values.length < 2) {
    return <svg width={width} height={height} className={className} />;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const xStep = width / (values.length - 1);
  const points = values
    .map((v, i) => {
      const x = i * xStep;
      const y = height - ((v - min) / range) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg
      width={width}
      height={height}
      className={className}
      aria-hidden="true"
    >
      <polyline
        points={points}
        fill="none"
        stroke={stroke}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --dir frontend test -- Sparkline`
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git checkout -b feature/567-phase-c-fundamentals
git add frontend/src/components/instrument/Sparkline.tsx frontend/src/components/instrument/Sparkline.test.tsx
git commit -m "feat(#567-C): Sparkline component (hand-coded SVG)"
```

## Task C.2: `FundamentalsPane` component (TDD)

**Files:**
- Create: `frontend/src/components/instrument/FundamentalsPane.tsx`
- Create: `frontend/src/components/instrument/FundamentalsPane.test.tsx`

- [ ] **Step 1: Write the failing test**

Create `frontend/src/components/instrument/FundamentalsPane.test.tsx`:

```tsx
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { FundamentalsPane } from "@/components/instrument/FundamentalsPane";
import * as api from "@/api/instruments";
import type { InstrumentSummary } from "@/api/types";

function makeSummary(secXbrlActive: boolean): InstrumentSummary {
  return {
    instrument_id: 1,
    has_sec_cik: true,
    identity: {
      symbol: "GME",
      display_name: "GameStop",
      market_cap: "1000000",
      sector: null,
    },
    capabilities: {
      fundamentals: {
        providers: secXbrlActive ? ["sec_xbrl"] : [],
        data_present: secXbrlActive ? { sec_xbrl: true } : {},
      },
    },
    key_stats: null,
  } as never;
}

const incomeRows = Array.from({ length: 4 }, (_, i) => ({
  period_end: `2026-0${i + 1}-30`,
  period_type: `Q${i + 1}`,
  values: {
    revenue: String(1000 + i * 100),
    operating_income: String(50 + i * 5),
    net_income: String(40 + i * 4),
  },
}));
const balanceRows = Array.from({ length: 4 }, (_, i) => ({
  period_end: `2026-0${i + 1}-30`,
  period_type: `Q${i + 1}`,
  values: {
    long_term_debt: String(200 + i * 10),
    short_term_debt: String(50 + i * 2),
  },
}));

describe("FundamentalsPane", () => {
  it("returns null when sec_xbrl capability is inactive", () => {
    const { container } = render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(false)} />
      </MemoryRouter>,
    );
    expect(container.firstChild).toBeNull();
  });

  it("renders 4 sparklines when capability active and data present", async () => {
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: string }) => {
        if (query.statement === "income") {
          return Promise.resolve({
            symbol: "GME",
            statement: "income",
            period: "quarterly",
            currency: "USD",
            source: "sec_xbrl",
            rows: incomeRows,
          });
        }
        return Promise.resolve({
          symbol: "GME",
          statement: "balance",
          period: "quarterly",
          currency: "USD",
          source: "sec_xbrl",
          rows: balanceRows,
        });
      }) as never,
    );
    render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(true)} />
      </MemoryRouter>,
    );
    expect(await screen.findByText("Revenue")).toBeInTheDocument();
    expect(screen.getByText("Op income")).toBeInTheDocument();
    expect(screen.getByText("Net income")).toBeInTheDocument();
    expect(screen.getByText("Total debt")).toBeInTheDocument();
  });

  it("computes total debt as long_term_debt + short_term_debt per period", async () => {
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: string }) => {
        if (query.statement === "income") {
          return Promise.resolve({
            symbol: "GME",
            statement: "income",
            period: "quarterly",
            currency: "USD",
            source: "sec_xbrl",
            rows: [
              {
                period_end: "2026-03-30",
                period_type: "Q1",
                values: { revenue: "100", operating_income: "10", net_income: "5" },
              },
              {
                period_end: "2026-06-30",
                period_type: "Q2",
                values: { revenue: "200", operating_income: "20", net_income: "10" },
              },
            ],
          });
        }
        return Promise.resolve({
          symbol: "GME",
          statement: "balance",
          period: "quarterly",
          currency: "USD",
          source: "sec_xbrl",
          rows: [
            {
              period_end: "2026-03-30",
              period_type: "Q1",
              values: { long_term_debt: "100", short_term_debt: "20" },
            },
            {
              period_end: "2026-06-30",
              period_type: "Q2",
              values: { long_term_debt: "150", short_term_debt: "30" },
            },
          ],
        });
      }) as never,
    );
    render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(true)} />
      </MemoryRouter>,
    );
    // Latest total debt = 150 + 30 = 180
    expect(await screen.findByText(/180/)).toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --dir frontend test -- FundamentalsPane`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the component**

Create `frontend/src/components/instrument/FundamentalsPane.tsx`:

```tsx
/**
 * FundamentalsPane — 4 sparklines (Revenue / Op income / Net income /
 * Total debt) over the latest 8 quarters from SEC XBRL fundamentals
 * (#567). Gated on `summary.capabilities.fundamentals.providers.sec_xbrl`
 * being active so non-SEC instruments don't render a dead pane.
 *
 * Data path: 2 parallel calls to /instruments/{symbol}/financials —
 * one for income, one for balance — joined per (period_end, period_type)
 * to keep all four sparklines on the same quarter set.
 */

import { fetchInstrumentFinancials } from "@/api/instruments";
import type { InstrumentFinancialRow, InstrumentSummary } from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { Sparkline } from "@/components/instrument/Sparkline";
import { useAsync } from "@/lib/useAsync";
import { useCallback, useMemo } from "react";
import { Link } from "react-router-dom";

const SLICE = 8;

interface SeriesRow {
  readonly period_end: string;
  readonly revenue: number;
  readonly operatingIncome: number;
  readonly netIncome: number;
  readonly totalDebt: number;
}

function num(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function joinPeriods(
  income: ReadonlyArray<InstrumentFinancialRow>,
  balance: ReadonlyArray<InstrumentFinancialRow>,
): SeriesRow[] {
  const bMap = new Map(
    balance.map((r) => [`${r.period_end}|${r.period_type}`, r]),
  );
  const joined: SeriesRow[] = [];
  for (const i of income) {
    const key = `${i.period_end}|${i.period_type}`;
    const b = bMap.get(key);
    if (b === undefined) continue;
    const revenue = num(i.values.revenue ?? null);
    const operatingIncome = num(i.values.operating_income ?? null);
    const netIncome = num(i.values.net_income ?? null);
    const lt = num(b.values.long_term_debt ?? null) ?? 0;
    const st = num(b.values.short_term_debt ?? null) ?? 0;
    if (revenue === null || operatingIncome === null || netIncome === null) {
      continue;
    }
    joined.push({
      period_end: i.period_end,
      revenue,
      operatingIncome,
      netIncome,
      totalDebt: lt + st,
    });
  }
  // Sort newest first then take the latest SLICE; reverse so the
  // sparklines plot oldest → newest left → right.
  joined.sort((a, b) => (a.period_end < b.period_end ? 1 : -1));
  const latest = joined.slice(0, SLICE);
  latest.reverse();
  return latest;
}

function formatLatest(values: ReadonlyArray<number>): string {
  if (values.length === 0) return "—";
  const v = values[values.length - 1];
  if (Math.abs(v) >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(2)}K`;
  return v.toFixed(0);
}

export interface FundamentalsPaneProps {
  readonly summary: InstrumentSummary;
}

export function FundamentalsPane({ summary }: FundamentalsPaneProps): JSX.Element | null {
  const symbol = summary.identity.symbol;
  const fundCell = summary.capabilities.fundamentals;
  const active =
    fundCell !== undefined &&
    fundCell.providers.includes("sec_xbrl") &&
    fundCell.data_present.sec_xbrl === true;

  const income = useAsync(
    useCallback(
      () =>
        fetchInstrumentFinancials(symbol, {
          statement: "income",
          period: "quarterly",
        }),
      [symbol],
    ),
    [symbol],
  );
  const balance = useAsync(
    useCallback(
      () =>
        fetchInstrumentFinancials(symbol, {
          statement: "balance",
          period: "quarterly",
        }),
      [symbol],
    ),
    [symbol],
  );

  const series = useMemo(() => {
    if (income.data === null || balance.data === null) return [];
    return joinPeriods(income.data.rows, balance.data.rows);
  }, [income.data, balance.data]);

  if (!active) return null;

  return (
    <Section title="Fundamentals">
      {income.loading || balance.loading ? (
        <SectionSkeleton rows={3} />
      ) : income.error !== null || balance.error !== null ? (
        <SectionError onRetry={() => { income.refetch(); balance.refetch(); }} />
      ) : series.length < 2 ? (
        <EmptyState
          title="Not enough fundamentals history"
          description="Need at least 2 quarters with both income + balance data."
        />
      ) : (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <FundamentalCell
            label="Revenue"
            values={series.map((r) => r.revenue)}
            stroke="text-sky-500"
          />
          <FundamentalCell
            label="Op income"
            values={series.map((r) => r.operatingIncome)}
            stroke="text-emerald-500"
          />
          <FundamentalCell
            label="Net income"
            values={series.map((r) => r.netIncome)}
            stroke="text-emerald-500"
          />
          <FundamentalCell
            label="Total debt"
            values={series.map((r) => r.totalDebt)}
            stroke="text-amber-500"
          />
        </div>
      )}
      <div className="mt-2 border-t border-slate-100 pt-1.5 text-right">
        <Link
          to={`/instrument/${encodeURIComponent(symbol)}?tab=financials`}
          className="text-[11px] text-sky-700 hover:underline"
        >
          View statements →
        </Link>
      </div>
    </Section>
  );
}

function FundamentalCell({
  label,
  values,
  stroke,
}: {
  readonly label: string;
  readonly values: ReadonlyArray<number>;
  readonly stroke: string;
}) {
  return (
    <div className="flex flex-col items-start">
      <span className="text-[10px] uppercase tracking-wider text-slate-500">
        {label}
      </span>
      <Sparkline values={values} className={stroke} />
      <span className="text-xs font-medium tabular-nums text-slate-800">
        {formatLatest(values)}
      </span>
    </div>
  );
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --dir frontend test -- FundamentalsPane`
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/instrument/FundamentalsPane.tsx frontend/src/components/instrument/FundamentalsPane.test.tsx
git commit -m "feat(#567-C): FundamentalsPane (sec_xbrl-gated, period-safe join)"
```

## Task C.3: Insert `<FundamentalsPane>` into `DensityGrid`

**Files:**
- Modify: `frontend/src/components/instrument/DensityGrid.tsx`
- Modify: `frontend/src/components/instrument/DensityGrid.test.tsx`

- [ ] **Step 1: Add the import + render**

In `frontend/src/components/instrument/DensityGrid.tsx`:

(a) Add the import at the top with the other instrument-component imports:

```tsx
import { FundamentalsPane } from "@/components/instrument/FundamentalsPane";
```

(b) Find the row 2 right-column slot — currently the `SecProfilePanel` pane sits in `lg:col-start-2 lg:row-start-2` (or similar). Per the spec layout (Section G), the new layout for row 2 is:

- left wide cell (2fr) → `<FundamentalsPane summary={summary} />`
- middle 1fr cell → existing `<SecProfilePanel symbol={symbol} />`
- right 1fr cell → existing `<FilingsPane instrumentId={summary.instrument_id} symbol={symbol} />`

Concretely, locate the existing two row-2 right-column panes (SEC profile + Filings) and insert a new wide pane BEFORE them in DOM order so the grid's auto-placement sets it as the first cell of row 2 (which spans col 1 = the 2fr column). Wrap it in the standard pane className:

```tsx
<div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm">
  <FundamentalsPane summary={summary} />
</div>
```

If `FundamentalsPane` returns `null` (capability inactive), the wrapper still renders an empty bordered card. To avoid that, conditionally render the wrapper too:

```tsx
{summary.capabilities.fundamentals?.providers.includes("sec_xbrl") &&
 summary.capabilities.fundamentals.data_present.sec_xbrl === true ? (
  <div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm">
    <FundamentalsPane summary={summary} />
  </div>
) : null}
```

- [ ] **Step 2: Update existing tests + add a fundamentals-pane visibility test**

In `frontend/src/components/instrument/DensityGrid.test.tsx`:

(a) Add a `vi.mock` block for `FundamentalsPane` so the existing tests don't try to fire real API calls:

```tsx
vi.mock("@/components/instrument/FundamentalsPane", () => ({
  FundamentalsPane: () => <div>Fundamentals stub</div>,
}));
```

(b) Append a test asserting the pane renders ONLY when `sec_xbrl` is active:

```tsx
it("renders FundamentalsPane only when sec_xbrl fundamentals capability is active", () => {
  const summaryActive = {
    ...summary,
    capabilities: {
      fundamentals: {
        providers: ["sec_xbrl"],
        data_present: { sec_xbrl: true },
      },
    },
  } as never;
  const { rerender } = render(
    <MemoryRouter>
      <DensityGrid
        summary={summaryActive}
        keyStatsBlock={<div>K</div>}
        thesisBlock={<div>T</div>}
        newsBlock={<div>N</div>}
      />
    </MemoryRouter>,
  );
  expect(screen.getByText("Fundamentals stub")).toBeInTheDocument();

  const summaryInactive = {
    ...summary,
    capabilities: {},
  } as never;
  rerender(
    <MemoryRouter>
      <DensityGrid
        summary={summaryInactive}
        keyStatsBlock={<div>K</div>}
        thesisBlock={<div>T</div>}
        newsBlock={<div>N</div>}
      />
    </MemoryRouter>,
  );
  expect(screen.queryByText("Fundamentals stub")).toBeNull();
});
```

- [ ] **Step 3: Run frontend gates**

Run: `pnpm --dir frontend typecheck && pnpm --dir frontend test:unit`
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/instrument/DensityGrid.tsx frontend/src/components/instrument/DensityGrid.test.tsx
git commit -m "feat(#567-C): wire FundamentalsPane into DensityGrid (gated)"
```

## Task C.4: Pre-push + Codex + push + merge

- [ ] **Step 1: Full gate**

```bash
pnpm --dir frontend typecheck && pnpm --dir frontend test:unit
```

- [ ] **Step 2: Codex review**

Run: `codex.cmd exec review`

- [ ] **Step 3: Push + PR**

```bash
git push -u origin feature/567-phase-c-fundamentals
gh pr create --title "feat(#567-C): Fundamentals sparkline pane" --body "$(cat <<'EOF'
## What

- `Sparkline` hand-coded SVG component (no new chart dep).
- `FundamentalsPane`: 4 sparklines for Revenue / Op income / Net income / Total debt over the latest 8 quarters.
- Period-safe join keyed on `(period_end, period_type)` between income + balance statements so all four metrics plot the same quarter set.
- Gated on `summary.capabilities.fundamentals.providers.sec_xbrl.data_present === true`.
- Inserted into the density grid row 2.

## Why

Phase C of #567 — operator wants Bloomberg-tier visual analysis. Fundamentals over time is the highest-signal chart we can ship today.

## Test plan

- [ ] Vitest: 3 Sparkline tests (polyline shape, empty path, default stroke).
- [ ] Vitest: 3 FundamentalsPane tests (gating off, gating on, total-debt computation).
- [ ] Vitest: DensityGrid renders the pane only when capability active.
- [ ] Manual: load `/instrument/GME`, confirm 4 sparklines visible; load a non-SEC instrument and confirm pane is absent.
EOF
)"
```

- [ ] **Step 4: Poll + resolve + merge on APPROVE + green**

```bash
gh pr merge <PR#> --squash --delete-branch
git checkout main && git fetch && git reset --hard origin/main
```

---

# Phase D — InsiderActivitySummary

**Branch:** `feature/567-phase-d-insider-summary`

**Goal:** Replace the inline `InsiderActivityPanel` (transaction list) in DensityGrid with `InsiderActivitySummary` (compact 5-field block using the total-activity lens).

## Task D.1: `InsiderActivitySummary` component (TDD)

**Files:**
- Create: `frontend/src/components/instrument/InsiderActivitySummary.tsx`
- Create: `frontend/src/components/instrument/InsiderActivitySummary.test.tsx`

- [ ] **Step 1: Write the failing test**

Create `frontend/src/components/instrument/InsiderActivitySummary.test.tsx`:

```tsx
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { InsiderActivitySummary } from "@/components/instrument/InsiderActivitySummary";
import * as api from "@/api/instruments";

const payload = {
  symbol: "GME",
  open_market_net_shares_90d: "999999",
  open_market_buy_count_90d: 99,
  open_market_sell_count_90d: 99,
  total_acquired_shares_90d: "42392",
  total_disposed_shares_90d: "18331",
  acquisition_count_90d: 22,
  disposition_count_90d: 16,
  unique_filers_90d: 12,
  latest_txn_date: "2026-04-13",
  net_shares_90d: "999999",
  buy_count_90d: 99,
  sell_count_90d: 99,
};

describe("InsiderActivitySummary", () => {
  it("renders NET 90d, ACQUIRED, DISPOSED, TXNS, LATEST from total-activity lens", async () => {
    vi.spyOn(api, "fetchInsiderSummary").mockResolvedValue(payload);
    render(
      <MemoryRouter>
        <InsiderActivitySummary symbol="GME" />
      </MemoryRouter>,
    );
    // NET 90d = 42392 - 18331 = 24061 (positive → leading +)
    expect(await screen.findByText(/\+24,?061/)).toBeInTheDocument();
    expect(screen.getByText(/42,?392/)).toBeInTheDocument();
    expect(screen.getByText(/18,?331/)).toBeInTheDocument();
    // TXNS = acquisition_count_90d + disposition_count_90d = 22 + 16 = 38
    expect(screen.getByText("38")).toBeInTheDocument();
    expect(screen.getByText("2026-04-13")).toBeInTheDocument();
  });

  it("renders negative NET with leading minus when disposed > acquired", async () => {
    vi.spyOn(api, "fetchInsiderSummary").mockResolvedValue({
      ...payload,
      total_acquired_shares_90d: "10000",
      total_disposed_shares_90d: "30000",
    });
    render(
      <MemoryRouter>
        <InsiderActivitySummary symbol="GME" />
      </MemoryRouter>,
    );
    // NET = 10000 - 30000 = -20000
    expect(await screen.findByText(/-20,?000/)).toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm --dir frontend test -- InsiderActivitySummary`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the component**

Create `frontend/src/components/instrument/InsiderActivitySummary.tsx`:

```tsx
/**
 * InsiderActivitySummary — compact insider summary block on the
 * density grid (#567). Uses total-activity lens consistently
 * (total_acquired_shares_90d, total_disposed_shares_90d,
 *  acquisition_count_90d + disposition_count_90d).
 *
 * NET 90d is computed client-side as acquired - disposed because
 * the response's `net_shares_90d` legacy alias maps to the
 * open-market net, NOT total-activity (would cross lenses).
 */

import { fetchInsiderSummary } from "@/api/instruments";
import type { InsiderSummary } from "@/api/instruments";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";

export interface InsiderActivitySummaryProps {
  readonly symbol: string;
}

function fmt(n: number): string {
  if (Math.abs(n) >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  return n.toLocaleString();
}

function fmtSigned(n: number): string {
  if (n > 0) return `+${fmt(n)}`;
  if (n < 0) return `-${fmt(Math.abs(n))}`;
  return "0";
}

function num(v: string | null | undefined): number {
  if (v === null || v === undefined) return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export function InsiderActivitySummary({
  symbol,
}: InsiderActivitySummaryProps): JSX.Element {
  const state = useAsync<InsiderSummary>(
    useCallback(() => fetchInsiderSummary(symbol), [symbol]),
    [symbol],
  );

  return (
    <Section title="Insider activity (90d)">
      {state.loading ? (
        <SectionSkeleton rows={2} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data === null ? (
        <EmptyState
          title="No insider data"
          description="No Form 4 transactions on file for this instrument."
        />
      ) : (
        (() => {
          const acquired = num(state.data.total_acquired_shares_90d);
          const disposed = num(state.data.total_disposed_shares_90d);
          const net = acquired - disposed;
          const txns =
            state.data.acquisition_count_90d + state.data.disposition_count_90d;
          const arrow = net > 0 ? "↑" : net < 0 ? "↓" : "·";
          const netClass =
            net > 0
              ? "text-emerald-700"
              : net < 0
                ? "text-red-700"
                : "text-slate-700";
          return (
            <div className="grid grid-cols-5 gap-2 text-xs">
              <Field label="NET 90d">
                <span className={`font-medium tabular-nums ${netClass}`}>
                  {fmtSigned(net)} {arrow}
                </span>
              </Field>
              <Field label="ACQUIRED">
                <span className="tabular-nums">{fmt(acquired)} sh</span>
              </Field>
              <Field label="DISPOSED">
                <span className="tabular-nums">{fmt(disposed)} sh</span>
              </Field>
              <Field label="TXNS">
                <span className="tabular-nums">{txns}</span>
              </Field>
              <Field label="LATEST">
                <span className="tabular-nums">
                  {state.data.latest_txn_date ?? "—"}
                </span>
              </Field>
            </div>
          );
        })()
      )}
    </Section>
  );
}

function Field({
  label,
  children,
}: {
  readonly label: string;
  readonly children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col">
      <span className="text-[10px] uppercase tracking-wider text-slate-500">
        {label}
      </span>
      <span>{children}</span>
    </div>
  );
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pnpm --dir frontend test -- InsiderActivitySummary`
Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git checkout -b feature/567-phase-d-insider-summary
git add frontend/src/components/instrument/InsiderActivitySummary.tsx frontend/src/components/instrument/InsiderActivitySummary.test.tsx
git commit -m "feat(#567-D): InsiderActivitySummary (total-activity lens)"
```

## Task D.2: Swap inline `InsiderActivityPanel` for `InsiderActivitySummary` in `DensityGrid`

**Files:**
- Modify: `frontend/src/components/instrument/DensityGrid.tsx`

- [ ] **Step 1: Replace the import + usage**

In `frontend/src/components/instrument/DensityGrid.tsx`:

(a) Replace the import:

```tsx
// BEFORE:
import { InsiderActivityPanel } from "@/components/instrument/InsiderActivityPanel";

// AFTER:
import { InsiderActivitySummary } from "@/components/instrument/InsiderActivitySummary";
```

(b) Replace the iteration over `insiderProviders` (which renders `<InsiderActivityPanel>` per provider) with a single `<InsiderActivitySummary>` instance gated on whether ANY insider provider is active. The summary endpoint isn't provider-scoped (the existing component takes an optional `provider` arg but the summary shape is the same), so one render is enough:

Find the existing block:

```tsx
{insiderProviders.map((p) => (
  <InsiderActivityPanel key={`ins-${p}`} symbol={symbol} provider={p} />
))}
```

Replace with:

```tsx
{insiderProviders.length > 0 && (
  <InsiderActivitySummary symbol={symbol} />
)}
```

- [ ] **Step 2: Update DensityGrid test**

In `frontend/src/components/instrument/DensityGrid.test.tsx`, the existing `vi.mock` block mocks `InsiderActivityPanel`. Replace that mock with one for `InsiderActivitySummary`:

```tsx
// BEFORE:
vi.mock("@/components/instrument/InsiderActivityPanel", () => ({
  InsiderActivityPanel: () => <div>Insider</div>,
}));

// AFTER:
vi.mock("@/components/instrument/InsiderActivitySummary", () => ({
  InsiderActivitySummary: () => <div>Insider summary</div>,
}));
```

- [ ] **Step 3: Run frontend gates**

Run: `pnpm --dir frontend typecheck && pnpm --dir frontend test:unit`
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/instrument/DensityGrid.tsx frontend/src/components/instrument/DensityGrid.test.tsx
git commit -m "feat(#567-D): DensityGrid uses InsiderActivitySummary in place of inline panel"
```

## Task D.3: Pre-push + Codex + push + merge

- [ ] **Step 1: Full gate**

```bash
pnpm --dir frontend typecheck && pnpm --dir frontend test:unit
```

- [ ] **Step 2: Codex review**

Run: `codex.cmd exec review`

- [ ] **Step 3: Push + PR**

```bash
git push -u origin feature/567-phase-d-insider-summary
gh pr create --title "feat(#567-D): InsiderActivitySummary on density grid" --body "$(cat <<'EOF'
## What

- New `InsiderActivitySummary` component: compact 5-field block (NET 90d, ACQUIRED, DISPOSED, TXNS, LATEST) using the total-activity lens.
- NET 90d computed client-side as `total_acquired_shares_90d - total_disposed_shares_90d` so the lens stays consistent (the legacy `net_shares_90d` alias is open-market, not total-activity).
- Replaces the inline `InsiderActivityPanel` (transaction row list) inside `DensityGrid`.
- `InsiderActivityPanel` file kept for future use on a dedicated tab.

## Why

Phase D of #567 — operator flagged that the row list inside the density grid is too long-form. A compact summary fits the grid better.

## Test plan

- [ ] Vitest: 2 InsiderActivitySummary tests (positive NET + negative NET).
- [ ] Vitest: DensityGrid mock updated.
- [ ] Manual: load `/instrument/GME`, confirm 5 fields visible (NET / ACQUIRED / DISPOSED / TXNS / LATEST).
EOF
)"
```

- [ ] **Step 4: Poll + resolve + merge on APPROVE + green**

```bash
gh pr merge <PR#> --squash --delete-branch
git checkout main && git fetch && git reset --hard origin/main
```

---

## Self-review summary

- **Spec coverage:** A (filings filter + footer link), B (grid sizing + spacing), C (Sparkline + FundamentalsPane), D (InsiderActivitySummary). All 6 in-scope spec sections covered.
- **Out of scope per spec:** ownership pie (#567 spec section I), site-wide visual polish, insider weekly bars, 10-Q drilldown routes, dedicated insider/dividend tabs. Each is filed or noted as a follow-up.
- **Phase ordering:** Phase A is fully independent (backend + FilingsPane). Phases B, C, D all touch `DensityGrid.tsx` + its test — write-scope-coupled, recommended sequencing A → B → C → D with rebase between each.
- **Type/contract consistency:** `fetchFilings(instrumentId, offset, limit, opts)` signature in Phase A matches the test call site and the FilingsPane consumer. `FundamentalsPaneProps` takes `summary: InstrumentSummary` matching the existing `DensityGrid` prop type. `InsiderActivitySummaryProps` takes `symbol: string` matching `fetchInsiderSummary(symbol)`.
- **Placeholders:** none — every step has runnable code or a concrete command with expected output.
