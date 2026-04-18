# P0-2 Portfolio workstation — design spec

**Issue:** [#314](https://github.com/Luke-Bradford/eBull/issues/314)
**Plan:** `docs/superpowers/plans/2026-04-18-product-visibility-pivot.md`
**Size:** M (~2 days, frontend only)
**Depends on:** #313 (shipped in #319) — reuses `OrderEntryModal`, `ClosePositionModal`, `DemoLivePill`.
**Blocks:** #316 (instrument terminal reuses the detail panel's thesis/filing/score components).

---

## 1. Goal

Turn `PortfolioPage` from a report (click row → navigate away → back) into a workbench: one click selects a row, the detail panel populates inline with position data + thesis + filings + score + action buttons, and keyboard navigation lets the operator move through the list without the mouse.

Vision-check: yes — operator "manages their fund from this screen" without ever leaving it for the golden paths (review, Add, Close).

## 2. Backend contract (all endpoints already exist)

No backend changes. Detail-panel sources:

- `GET /portfolio` — `fetchPortfolio()` already loaded on the page. Provides `PositionItem.trades: BrokerPositionItem[]` in DISPLAY currency. The detail panel's broker-position table reuses these rows verbatim (no extra fetch from the panel).
- `GET /theses/{id}` — `fetchLatestThesis(instrumentId)`. Returns `ThesisDetail` with valuation fields in NATIVE currency (no currency field on the response).
- `GET /filings/{id}` — `fetchFilings(instrumentId, offset, limit)`. `FilingItem` fields: `filing_date`, `filing_type`, `provider`, `extracted_summary`, `red_flag_score`, URLs. No `title` field.
- `GET /rankings/history/{id}` — `fetchScoreHistory(instrumentId, limit)`.

The detail panel deliberately does NOT call `GET /portfolio/instruments/{id}` — the modals from #313 already fetch that on open for native-currency preview, and having the panel duplicate the fetch on every selection change would halve the cache hit rate and confuse the spec's "panel uses display currency, modal uses native" boundary.

Each call 404s if the instrument has no data. We treat 404 as an empty state, not an error (mirrors `InstrumentDetailPage.tsx:160`).

### Currency contract (pinned)

- Position snapshot, broker-position rows: rendered in **display currency** from `PositionItem` / `BrokerPositionItem`.
- Score total + sub-scores: dimensionless numbers from `ScoreHistoryItem`. Rendered via `formatNumber`, never via `formatMoney`.
- Thesis valuation fields (buy-zone low/high, bull/base/bear values): rendered as **bare numbers with no currency symbol**, with a small caption `"(in instrument's native currency)"` under the block. Rationale: `ThesisDetail` does not carry a currency field and we are not fetching `/portfolio/instruments/{id}` in the panel (see above). The full `InstrumentDetailPage` already formats these with native currency; the detail panel is a summary, the full page is the source of truth for formatting.

## 3. Non-goals (drop aggressively)

- **No URL-driven selection.** Selected row lives in in-memory page state for v1. Deep-linking (`?selected=42`) is a nice-to-have that adds router complexity and race conditions; deferred to a follow-up if an operator asks for it.
- **No dual-pane for mirror rows.** Clicking a mirror row still navigates to `/copy-trading/{id}` — copy-trading is a separate execution surface with different data needs. The workstation is for directly-held positions.
- **No inline thesis editing, no inline order-type switching, no drag-resize panels.** Thesis is read-only in the panel; for anything else the operator clicks "View Research" and drops into the full `InstrumentDetailPage`.
- **No keyboard-shortcut help overlay.** Shortcuts are documented in the spec and a small label bar at the bottom of the detail panel. A modal cheat-sheet is deferred.
- **No virtualized table.** Pagination handles >50 rows. Virtualization is premature until we have thousands of holdings.
- **No column show/hide, no per-operator layout persistence.** Later PR.
- **No news panel.** Present on `InstrumentDetailPage` but the workstation trades panel real estate for thesis + filings + score — the three signals that drive an exit decision. News stays on the drill-through page.
- **No "close all" for aggregated positions.** If `trades.length > 1`, the detail panel now exposes per-broker-position rows each with their own `Close` button — that covers the scope that was deferred in #313. A "close every broker position for this instrument" macro button would be an unsafe one-click bulk action in a product whose `EXIT` semantics are still being hardened; skipped.

## 4. Layout

Split-pane on `≥lg` breakpoints (≥1024px wide), stacked on smaller screens.

```
┌───────────────────────────────────┬──────────────────────────┐
│ SummaryBar (AUM / P&L / …)        │                          │
├───────────────────────────────────┤                          │
│ Search + shortcut hint            │  DetailPanel             │
├───────────────────────────────────┤   (selectedInstrumentId) │
│ PortfolioTable (selectable rows)  │                          │
│                                   │                          │
│ [Pagination controls]             │                          │
└───────────────────────────────────┴──────────────────────────┘
```

- Left pane: 60% width at ≥lg. Full width below.
- Right pane: 40% width at ≥lg. Stacks below the table on narrow screens.
- DetailPanel renders a placeholder when no row is selected: `"Select a position to see its detail."`.

## 5. File plan

### New files

- `frontend/src/components/portfolio/DetailPanel.tsx` — right-pane container, drives thesis/filings/score sections.
- `frontend/src/components/portfolio/PositionSummary.tsx` — left-of-detail-panel, the held-position snapshot.
- `frontend/src/components/portfolio/BrokerPositionsTable.tsx` — per-broker-position rows with individual Close buttons.
- `frontend/src/components/portfolio/ThesisBlock.tsx` — condensed thesis block (stance, confidence, buy-zone, memo preview).
- `frontend/src/components/portfolio/ScoreBlock.tsx` — latest score card with sub-scores.
- `frontend/src/components/portfolio/FilingsBlock.tsx` — latest 3 filings.
- `frontend/src/components/portfolio/DetailPanel.test.tsx`
- `frontend/src/components/portfolio/BrokerPositionsTable.test.tsx`
- `frontend/src/pages/PortfolioPage.test.tsx` — first test file for this page; covers table interaction, selection state, keyboard nav, pagination, modal wiring.

### Modified files

- `frontend/src/pages/PortfolioPage.tsx` — adopt split layout, selected-row state, keyboard handler, pagination. Drop the row-click `useNavigate` for position rows (replace with select). Mirror rows keep their navigate behaviour.

### Unchanged files

- `frontend/src/api/*.ts` — no new fetchers.
- `frontend/src/api/types.ts` — no new types.
- All existing `OrderEntryModal` / `ClosePositionModal` / `DemoLivePill` code from #313.

Estimated ~9 files new + 1 modified, ~900 LoC including tests.

## 6. Selected-row state + derived slices

Page state:

```ts
const [selectedId, setSelectedId] = useState<number | null>(null);
const [focusedIdx, setFocusedIdx] = useState<number>(0);
const [search, setSearch] = useState<string>("");
const [page, setPage] = useState<number>(1);
const PAGE_SIZE = 50;
```

Derived, in render:

```ts
// allRows: positions + mirrors, sorted by value (unchanged from today).
// visible: allRows filtered by search. Used for pagination + keyboard nav.
// pageRows: visible slice for the current page.
// totalPages: Math.max(1, Math.ceil(visible.length / PAGE_SIZE)).
// selectedPosition: portfolio.data.positions.find(p => p.instrument_id === selectedId) ?? null
//   — pulled from the UNFILTERED positions list so the detail panel keeps rendering
//   when the operator narrows the search.
```

Clamp invariants enforced in effects:

```ts
// When `visible.length` changes (search, pagination), clamp focusedIdx.
useEffect(() => {
  if (pageRows.length === 0) return;           // keep focusedIdx unchanged; `j`/`k` are no-ops (§8)
  setFocusedIdx((i) => Math.min(Math.max(i, 0), pageRows.length - 1));
}, [pageRows.length]);

// When the current page exceeds totalPages (e.g. search shrinks results
// below the current page), clamp page back into range.
useEffect(() => {
  if (page > totalPages) setPage(totalPages);
}, [page, totalPages]);
```

Edge cases pinned:

- **Zero rows visible** (`pageRows.length === 0`): detail panel still renders for the last `selectedId`; the table renders a `"No positions match your search."` empty state; `j`/`k`/`Enter` are no-ops until rows return.
- **Selected row filtered out**: `selectedPosition` is derived from `portfolio.data.positions` (unfiltered), so the detail panel keeps the operator's last choice visible. Clearing search brings the row back into the left pane.
- **Page clamp**: shrinking `visible` below the current page resets the page to the last valid page; never leaves the UI showing an empty page while rows exist.
- **Stale selection after `/portfolio` refetch**: if the operator fully closes a position, `portfolio.refetch()` (fired from `handleFilled`) drops that instrument from `portfolio.data.positions`. `selectedPosition` is derived via `find` and becomes `null`. We clear the stale `selectedId` in a `useEffect([portfolio.data])` so the detail panel collapses back to the placeholder rather than silently hiding behind `selectedId !== null` gates:

```ts
useEffect(() => {
  if (selectedId === null) return;
  if (portfolio.data === null) return; // still loading — do nothing
  const stillExists = portfolio.data.positions.some(
    (p) => p.instrument_id === selectedId,
  );
  if (!stillExists) setSelectedId(null);
}, [portfolio.data, selectedId]);
```

`b` and `c` are additionally gated on `selectedPosition !== null` at the handler level so that a stale `selectedId` between a `/portfolio` refetch and the clamp effect's flush cannot open a modal with a ghost position. `Enter` is unaffected — it promotes `focusedIdx` to `selectedId` based on the currently-visible `pageRows[focusedIdx]`, which is always a real row.

Mirror rows remain row-click-to-navigate (§3). They are never selectable via `selectedId` because the detail panel is for held-instrument data, not for a copy-trading summary.

## 7. DetailPanel

### Props

```ts
interface DetailPanelProps {
  readonly selectedPosition: PositionItem | null;
  readonly currency: string; // display currency for the header stats
  readonly onAdd: (p: PositionItem) => void;
  readonly onCloseTrade: (t: CloseTarget) => void;
  readonly onViewResearch: (instrumentId: number) => void;
}
```

No `isOpen` / `onRequestClose` — it's always visible in the right pane, the data inside swaps when `selectedPosition` changes.

### Composition

`DetailPanel` fetches via `useAsync` keyed on `selectedPosition?.instrument_id`:

- `thesis = useAsync(() => fetchLatestThesis(id), [id])`
- `filings = useAsync(() => fetchFilings(id, 0, 3), [id])`
- `scores = useAsync(() => fetchScoreHistory(id, 5), [id])`

Each block renders its own loading / error / empty state (per `async-data-loading.md` — one error surface, one retry button per source). 404s are treated as "no data yet" empty states, not errors.

When `selectedPosition === null`, the panel renders only the placeholder `"Select a position to see its detail."` and skips all fetches (conditional rendering of the fetch-driving components, per the mount-on-open pattern we used in #313 modals).

### Sections (top → bottom)

1. **Header**: `{symbol} · {company_name}` + `Add` button + `View Research` link.
2. **Position snapshot** (`PositionSummary`): units, avg cost, market value, P&L — in display currency, pulled from `selectedPosition` (no fetch needed).
3. **Broker positions** (`BrokerPositionsTable`): always rendered; shows 1-N rows from `selectedPosition.trades` (display currency — same as the rest of the portfolio page). Each row has a `Close` button that fires `onCloseTrade`. This covers the aggregated-position close gap flagged during #313.
4. **Thesis** (`ThesisBlock`): stance, confidence, buy-zone low/high, bull/base/bear values, first ~300 chars of `memo_markdown`, and a `Read full thesis →` link to `/instruments/{id}` (the research page — NOT `/portfolio/{id}`, which is the native-currency drill-through). Valuation numbers render as plain numbers with a small caption `"(in instrument's native currency)"` since `ThesisDetail` does not carry a currency field and the panel does not fetch native context (§2).
5. **Latest score** (`ScoreBlock`): total + 5 sub-scores (quality, value, turnaround, momentum, sentiment) from the newest `ScoreHistoryItem`.
6. **Filings** (`FilingsBlock`): latest 3 filings showing `filing_date`, `filing_type`, and a one-line excerpt from `extracted_summary` (truncated to ~80 chars with ellipsis). `FilingItem` has no title field — `extracted_summary` is the human-readable line. If `extracted_summary` is null, render `"(no summary — open filing for details)"` with a link to `source_url` or `primary_document_url`.

Bottom label bar shows keyboard hints: `/ search · j/k move · Enter select · Esc clear · b Add · c Close`.

## 8. Keyboard navigation

### Focus + handler strategy

Keyboard shortcuts must work regardless of where browser focus sits (e.g. right after the page loads, or after the operator clicks anywhere inside the table). We attach the handler to `document` via `useEffect` + `window.addEventListener("keydown", ...)`, not to the container's `onKeyDown`. Rationale: `<tr>` elements are not focusable by default and making them focusable just for keyboard routing creates a whole tab-order story we do not need for v1. A window listener is simpler, matches how `InstrumentDetailPage.test.tsx` and other existing pages dispatch keyboard events in tests, and the gate conditions below keep the handler narrowly scoped.

Tests must exercise the shortcuts via `userEvent.keyboard(...)` both before any row click and after a row click, to pin that the listener does not depend on row focus.

### Gate

The handler fires UNLESS:

1. `document.activeElement` is an `input`, `textarea`, `select`, or `[contenteditable]` — EXCEPT the `Esc` key, which is always processed (special-case) so it can blur the search box.
2. A modal is open (`addFor !== null || closeFor !== null`).
3. A modifier key is held (`Ctrl`, `Meta`, `Alt`) — keeps browser shortcuts like Ctrl+R untouched.

Shortcuts:

| Key | Action |
|---|---|
| `/` | Focus the search box; `preventDefault` so the `/` does not land in the input as its first character. |
| `j` | `focusedIdx = min(focusedIdx + 1, pageRows.length - 1)`. No-op when `pageRows.length === 0`. |
| `k` | `focusedIdx = max(focusedIdx - 1, 0)`. No-op when `pageRows.length === 0`. |
| `Enter` | `setSelectedId(pageRows[focusedIdx].instrument_id)` (positions only; no-op on mirror rows and when the page is empty). |
| `Esc` | If the search input is focused, blur it AND clear the search string. Otherwise `setSelectedId(null); setFocusedIdx(0)`. Processed regardless of the input-focused gate (special-case above). |
| `b` | If `selectedPosition !== null`, open the Add modal for it. No-op otherwise (includes the stale-selection window between a `/portfolio` refetch and the clamp effect). |
| `c` | If `selectedPosition !== null` AND `selectedPosition.trades.length === 1`, open the Close modal for that trade. If `trades.length > 1`, render a one-line hint `"Close requires a single broker position — use the detail panel."` in a non-intrusive spot (top of the table). Otherwise no-op. |

Visual focus indicator: the row at `focusedIdx` renders a 2px left border and `bg-slate-100`. The row at `selectedId` renders a stronger `bg-blue-50` and a left border in blue. Both can be present simultaneously.

### Interaction with the existing mouse path

- Clicking a position row: sets both `selectedId` and `focusedIdx` to that row.
- Clicking an Action button (`Add` / `Close`) inside a row: same as before — `stopPropagation` + open the modal, without changing selection (selection is only about the detail panel).
- Clicking outside rows does NOT clear selection (Esc does).

## 9. Pagination

Trigger: `visible.length > 50` after search filtering.

UI: simple `← Prev | Page N of M | Next →` footer under the table. `Prev` / `Next` disabled at the bounds. No jump-to-page input; no per-page size selector.

`selectedId` persists across page changes (the detail panel stays populated even if the selected row is no longer visible on the current page). `focusedIdx` resets to 0 on page change.

## 10. Integration with modals from #313

The existing `addFor` / `closeFor` page-level state and `handleFilled` logic from #313 is preserved verbatim. New callers:

- `DetailPanel.onAdd` → `setAddFor(selectedPosition)`.
- `DetailPanel.onCloseTrade` → `setCloseFor({ instrumentId, trade, valuationSource })`.
- Keyboard `b` / `c` → same setters, sourced from `selectedPosition` + (for `c`) its single `trades[0]`.

Modals continue to mount conditionally (`{addFor !== null ? <OrderEntryModal ...> : null}`). No changes to modal component code.

## 11. Test plan (`PortfolioPage.test.tsx`)

First test file for this page. Covers:

- **Split layout**: renders DetailPanel placeholder when nothing selected.
- **Row click selects**: click position row → detail panel loads with `fetchLatestThesis`, `fetchFilings`, `fetchScoreHistory` called with `instrumentId`.
- **Mirror row navigates**: click mirror row → `useNavigate` fired with `/copy-trading/{mirror_id}`; no detail-panel fetches.
- **`/ focuses search**: keydown `/` moves focus to the search input; `/` does not appear in the input.
- **`j/k` moves focus ring**: starting at `focusedIdx=0`, two `j` keydowns move the ring to index 2.
- **`Enter` selects**: keydown `Enter` promotes focused row into `selectedId`.
- **`Esc` clears**: with `selectedId=X`, Esc sets `selectedId=null` and collapses the detail panel to placeholder.
- **`b` opens Add modal**: with `selectedId=X`, `b` keydown opens `OrderEntryModal` with the correct instrument id.
- **`c` opens Close modal only on single-trade**: with `trades.length === 1`, `c` opens modal; with `trades.length > 1`, `c` is a no-op and a hint line renders `"Close requires a single broker position — use the detail panel."`.
- **Modal open suppresses shortcuts**: with `addFor !== null`, pressing `b` or `c` is a no-op (the modal owns keyboard focus).
- **Input focus suppresses shortcuts**: typing in the search box does not trigger `j/k/b/c`.
- **Pagination**: with 51 positions, page 1 shows 50 rows, page 2 shows 1; selectedId persists across pages.
- **Pagination clamp**: with 51 positions and `page=2`, if search filters results to 10 rows, `page` clamps back to 1 and rows render (no empty page-2 with rows available on page-1).
- **Selected row persists when filtered out**: select row X, type a search that excludes X. Detail panel keeps rendering X's data. Clear search → X reappears in the table, still selected.
- **Stale selection cleared after refetch**: select row X, simulate a `/portfolio` refetch whose response no longer contains X (position fully closed). `selectedId` clears, detail panel collapses to placeholder, and `b` / `c` keydowns are no-ops until a new row is selected.
- **Keyboard works before first click**: first render → no row clicked → `j` still moves the focus ring. Confirms the window listener does not depend on any element having focus.
- **Esc clears search when focused**: focus the search input, type "APPL", press Esc → input blurs AND search string is cleared.
- **Detail-panel 404**: thesis fetch throws `ApiError(404, ...)` → thesis section renders empty state, not error (matches `InstrumentDetailPage` convention).
- **Detail-panel all-errors fallback**: if thesis, filings, AND scores all fail with non-404, render each section's error independently; do NOT fire a page-level banner (per `async-data-loading.md`: top banner is reserved for "ALL sources failed", and `/portfolio` itself is still loaded).

Component-level tests for `DetailPanel`, `BrokerPositionsTable`: simpler shape tests for row rendering, action wiring, empty states.

## 12. Settled-decisions alignment

- **Close-position safety invariant** (`app/api/orders.py:14-17`): preserved — per-broker-position Close buttons route through the existing `ClosePositionModal` → `POST /portfolio/positions/{id}/close` path. Still operator-UI-only.
- **Demo-first** / **long-only**: unchanged — we are not adding any new execution path.
- **Product-visibility pivot** (`docs/settled-decisions.md:298`): this PR is P0-2, the second in the sequence.

## 13. Prevention-log alignment

| Entry | Honored by |
|---|---|
| async-data-loading: one error per source | §7 each block handles own loading/error; no page-level gate spans thesis/filings/scores |
| async-data-loading: top banner only for all-failed | `/portfolio` itself is the only source gating the page-level error; sub-section errors stay inline |
| safety-state-ui: cache | inherited from #313's `DemoLivePill`; not re-derived here |
| #135 positional/unscoped test selectors | page tests use `getByRole("row", { name: ... })` scoped selectors |
| #319 `canSubmit` loading guard | inherited from #313 modals; detail-panel has no submit buttons of its own |

## 14. Rollout + verification

Before push:

1. `pnpm --dir frontend typecheck` / `test` pass.
2. `uv run ruff check .`, `ruff format --check`, `pyright`, `pytest` — no-ops but run.

Browser verify (operator):

3. Load `/portfolio`. Click a row → detail panel populates.
4. Type `/`, type query, `Esc`, `j` / `k`, `Enter`, `b`, `c` — verify each shortcut.
5. On a multi-trade position, use the per-broker-position table's Close button.
6. Verify pagination appears when >50 positions exist (may need a dev-seed).

## 15. Open questions

None.

## 16. Reviewer cheat-sheet

- [ ] `PortfolioPage` keyboard handler is gated on `!input-focused && !modal-open && !modifier`; every test covers each gate.
- [ ] Detail panel's fetches are only wired when `selectedPosition !== null` (no idle traffic on empty state).
- [ ] Each DetailPanel source owns its own loading / error / empty state; no combined gates.
- [ ] 404s from thesis / filings / scores render empty states, not errors.
- [ ] `c` keyboard shortcut is a no-op on multi-trade positions with a visible hint.
- [ ] Pagination does NOT clear `selectedId`.
- [ ] Mirror rows are unchanged (still navigate on click, not selectable).
- [ ] No new backend endpoints; no changes to existing modals.
- [ ] Per-broker-position Close buttons in the detail panel preserve the `stopPropagation` discipline.
