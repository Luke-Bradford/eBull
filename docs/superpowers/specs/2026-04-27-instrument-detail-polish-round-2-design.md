# Instrument detail page — polish round 2 (#575)

Date: 2026-04-27
Status: design draft (post Codex round 1 second-opinion on parent direction; spec-level Codex review pending)
Parent: #567 (epic), #559 (visual polish)
Sibling: #576 (dedicated chart workspace, independent PR)

## Context

PRs #571–#574 shipped phases A–D of the prior refinement spec
(`docs/superpowers/specs/2026-04-27-instrument-detail-refinement-design.md`).
Operator review of the rendered page (screenshots 2026-04-27) flags
remaining gaps that the prior spec did not cover, plus a tightened
direction validated by Codex second-opinion: stay on the page-plus-route
model, do **not** add drawer / sheet / verdict-ribbon / universal-L3
contract, do **not** use `grid-flow-dense` for the outer layout.

This spec is a single-PR polish pass (may split into 2 if pane-header
standardization grows). It does **not** introduce new interaction layers.

## Pain points (file:line citations)

1. **Empty panes still render full empty-state cards.**
   - `frontend/src/components/instrument/ResearchTab.tsx:103-110` —
     `ThesisPanel` returns full `EmptyState` "No thesis yet" when
     `thesis === null`. Wastes ~25% of right column.
   - `frontend/src/components/instrument/DensityGrid.tsx:111-124` —
     dividends + insider combined card uses
     `overflow-auto max-h-[360px]` even when dividends are empty.
     Internal scroll wrapper on no content.
   - `frontend/src/components/instrument/ResearchTab.tsx:201-205` —
     `newsBlock` literally renders placeholder text "News tab still
     has the full feed." → vestigial pane.
2. **Combined dividends+insider card** still alive in
   `DensityGrid.tsx:111` despite #567 acceptance criterion #5
   ("drop transitional combined card span").
3. **Inverse density.** Filings (high-signal, deep data) cramped to
   1fr column (`DensityGrid.tsx:53` `grid-cols-[2fr_1fr_1fr]`,
   filings sits in the right 1fr). Insider full-width row but only
   5 stat fields.
4. **Inconsistent pane chrome.** `Section.tsx:11-29` is the only
   header primitive. Some panes display source provenance
   (`FieldSourceTag` in `ResearchTab.tsx:36-66`), most don't. Drill
   affordances live as ad-hoc footer links (`FilingsPane.tsx:135-143`,
   `FundamentalsPane.tsx:166-173`); other panes have no drill cue.
5. **Layout doesn't adapt to capability coverage.** Current
   `DensityGrid.tsx` renders the same shape regardless of capabilities.
   `summary.has_sec_cik` gates SEC profile + business sections inline,
   but other capability misses (no fundamentals, no insider, no
   filings) leave layout holes.

## Decisions (with rationale)

| # | Decision | Pick | Rationale |
|---|---|---|---|
| 1 | Empty panes | Hide entirely; do not render an empty card | Codex direction. Status moves to `SummaryStrip` for thesis (CTA already there). No vestigial UI. |
| 2 | Dividends + insider card | Unbundle into two separate panes | Each gets its own capability gate and pane chrome. Drops the combined `overflow-auto max-h-[360px]` wrapper. |
| 3 | Outer layout primitive | `grid-cols-12` with explicit `col-span-N` per pane | Codex flagged: do **not** use `grid-flow-dense` (panes jumping breaks spatial memory). Do **not** use subgrid (overkill). |
| 4 | Capability adaptation | 3 stable layout profiles selected by `DensityGrid` based on `summary.capabilities` | Codex pattern: 2-3 stable templates, map instruments into one. Tested as 3 distinct fixtures. |
| 5 | Pane header primitive | New `<PaneHeader>` component used by every instrument-page pane | Replaces ad-hoc `<Section>` + footer link patterns with one consistent header carrying title / scope / source / drill affordance. |
| 6 | Drill affordance | `onExpand?: () => void` prop on `PaneHeader` renders an `Open →` button. **Button only — not whole-card click.** | Codex spec-review flagged: nested button + outer-card click double-fires; outer card not keyboard-accessible without `role="button"` + `tabIndex` + key handlers; conflicts with existing internal interactive children in `PriceChart.tsx:160` (range buttons), `FilingsPane.tsx:121` (row links), `BusinessSectionsTeaser.tsx:77` (anchor). Button-only keeps a11y simple, no propagation tax, internal links work unchanged. |
| 7 | News pane | Hide if no items; if rendering, show ≤5 most-recent items | Drops the placeholder string. Real data only. |
| 8 | Verdict ribbon | **Out of scope** | Codex: noise duplicating SummaryStrip + key-stats + filings recency. Skip. |
| 9 | DetailSheet drawer | **Out of scope** | Codex: prove page-route model insufficient first. |
| 10 | L3 raw-data views | **Out of scope here** (scope of #576 covers chart's L3 OHLCV; other panes opt-in per pane in future tickets) | Codex: L3 is per-pane optional, not universal. |

## Architecture

### A. New `<PaneHeader>` component

`frontend/src/components/instrument/PaneHeader.tsx`:

```tsx
import { providerLabel } from "@/lib/capabilityProviders";

export interface PaneHeaderProps {
  readonly title: string;
  readonly scope?: string;       // e.g. "last 90 days", "latest quarter"
  readonly source?: {            // optional provenance
    readonly providers: ReadonlyArray<string>;
    readonly lastSync?: string;  // ISO date, optional
  };
  readonly onExpand?: () => void; // renders "Open →" button (button-only — no card-click)
}

export function PaneHeader({
  title, scope, source, onExpand,
}: PaneHeaderProps): JSX.Element {
  return (
    <header className="flex items-baseline justify-between gap-2 border-b border-slate-100 pb-1.5">
      <div className="flex items-baseline gap-2 min-w-0">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-600">
          {title}
        </h2>
        {scope ? (
          <span className="text-[10px] text-slate-500">{scope}</span>
        ) : null}
      </div>
      <div className="flex items-center gap-2 flex-shrink-0">
        {source && source.providers.length > 0 ? (
          <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-600">
            {source.providers.map(providerLabel).join(" · ")}
            {source.lastSync ? ` · ${source.lastSync}` : ""}
          </span>
        ) : null}
        {onExpand ? (
          <button
            type="button"
            onClick={onExpand}
            className="text-[11px] text-sky-700 hover:underline"
          >
            Open →
          </button>
        ) : null}
      </div>
    </header>
  );
}
```

Pane wrapper (used by every instrument pane in the grid) becomes:

```tsx
<article className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm">
  <PaneHeader title={...} scope={...} source={...} onExpand={onExpand} />
  <div className="mt-2">{children}</div>
</article>
```

No outer-card `onClick`; only the `Open →` button in the header
triggers `onExpand`. Internal interactive children (chart range
buttons, filings row `<Link>`s, anchor tags inside narrative teaser)
operate independently. Hover state is on the `Open →` button itself
(blue underline), not the whole card. Keeps a11y simple — focus order
is button-by-button, no `role="button"` shim on the article.

A small wrapper component `<Pane>` encapsulates the article + header
so individual pane components don't repeat the boilerplate.

**Wrapper ownership rule:** `<Pane>` is owned and rendered by the
**child pane component** (`FilingsPane`, `FundamentalsPane`,
`InsiderActivitySummary`, etc.) — **not** by `DensityGrid`.
`DensityGrid` only decides whether to mount the child component at
all (capability gate). When a child component decides it has nothing
to show (its narrow empty rule per Section D), it returns `null` and
no card appears. This avoids the "parent renders empty wrapper / child
renders null inside" double-chrome trap Codex flagged.

Existing `<Section>` from `Section.tsx` stays in place for
non-instrument-page consumers (Dashboard, Portfolio, etc.) — this is a
new primitive scoped to instrument page panes.

### B. Capability profiles

`frontend/src/components/instrument/densityProfile.ts`:

```ts
import type { InstrumentSummary } from "@/api/types";
import { activeProviders } from "@/lib/capabilityProviders";

export type DensityProfile = "full-sec" | "partial-filings" | "minimal";

export function selectProfile(summary: InstrumentSummary): DensityProfile {
  const cap = summary.capabilities;
  const hasFundamentals =
    cap.fundamentals?.providers.includes("sec_xbrl") &&
    cap.fundamentals.data_present.sec_xbrl === true;
  const hasFilings = activeProviders(cap.filings ?? { providers: [], data_present: {} }).length > 0;

  if (hasFundamentals && hasFilings) return "full-sec";
  if (hasFilings) return "partial-filings";
  return "minimal";
}
```

`DensityGrid.tsx` accepts an additional `profile` derived from
`selectProfile(summary)` and renders one of three stable layouts:

Codex spec-review flagged that `DividendsPanel` is provider-agnostic
(`DividendsPanel.tsx:2` — applies to UK and other non-SEC regions),
so every profile that has any active dividends capability must allocate
a dividends slot. Each profile below now gates dividends on
`activeProviders(cap.dividends).length > 0` independent of SEC status.

#### Profile: `full-sec` (e.g. GME, AAPL)

```
12-col grid, gap-2:
Row 1: chart (col-span-8, row-span-2) | keyStats (col-span-4)
Row 2:                                | secProfile (col-span-4)
Row 3: fundamentals (col-span-12)
Row 4: filings (col-span-7)            | insider (col-span-5)
Row 5: narrative (col-span-12) [if hasNarrative]
Row 6: dividends (col-span-12) [if activeProviders(cap.dividends).length > 0]
Row 7: news (col-span-12) [if newsItems.length > 0]
Row 8: thesis (col-span-12) [if thesis !== null]
```

#### Profile: `partial-filings` (e.g. UK Companies House, no SEC XBRL)

```
12-col grid, gap-2:
Row 1: chart (col-span-8, row-span-2) | keyStats (col-span-4)
Row 2:                                | secProfile (col-span-4) [if hasSec]
Row 3: filings (col-span-12)
Row 4: insider (col-span-7) [if activeProviders(cap.insider).length > 0]
       | dividends (col-span-5) [if activeProviders(cap.dividends).length > 0]
Row 5: narrative (col-span-12) [if hasNarrative]
Row 6: news (col-span-12) [if newsItems.length > 0]
Row 7: thesis (col-span-12) [if thesis !== null]
```

(Row 4 collapses if only one of insider/dividends is active — the
present pane spans full 12 cols.)

#### Profile: `minimal` (e.g. crypto, forex — no filings, no fundamentals)

```
12-col grid, gap-2:
Row 1: chart (col-span-8, row-span-2) | keyStats (col-span-4)
Row 2:                                | thesis (col-span-4) [if thesis !== null]
Row 3: dividends (col-span-12) [if activeProviders(cap.dividends).length > 0]
Row 4: news (col-span-12) [if newsItems.length > 0]
```

Pane order within each profile is stable; capability misses **remove**
panes without reflowing the others. Conditional panes use
`{condition && <ChildPaneComponent ... />}` in `DensityGrid`'s JSX,
where the child component owns its own `<Pane>` wrapper internally
(per Section A wrapper-ownership rule). `DensityGrid` does **not**
render `<Pane>` directly; it only decides which child components to
mount. No `grid-flow-dense`.

### C. Pane changes (per pane)

| Pane | Change |
|---|---|
| `PriceChart` (existing) | Wrap in `<Pane>` with `onExpand` routing to `/instrument/:symbol/chart` (route ships in #576; until then `onExpand` is `undefined` and no `Open →` button renders). `PaneHeader` shows title "Price chart" only — **no `scope` prop**. Codex spec-review flagged: range state lives inside `PriceChart` via URL search params (`PriceChart.tsx:108`); a parent `PaneHeader` cannot read it without lifting state up or duplicating the search-param read at the wrapper. Range stays visible inside the chart UI itself. |
| `KeyStats` (`ResearchTab.tsx:171-193`) | Replace `<Section title="Key statistics">` with `<Pane title="Key statistics">`. **No pane-level `source` prop and no `scope` prop** — Codex spec-review flagged: rows mix live / TTM / YoY / latest-quarter values (`ResearchTab.tsx:180`), and current per-row `FieldSourceTag` provenance (`ResearchTab.tsx:36`) is per-field. A pane-level scope of `"latest quarter"` would mislabel the live-price-derived rows; a pane-level source badge would be less accurate than per-row tags. **Keep `FieldSourceTag` on each row unchanged.** Drop fully-null rows (e.g. drop "Dividend yield" row when value is `null`); only render rows whose value is not null. |
| `ThesisPanel` | When `thesis === null`, **render `null`** (parent decides whether to render the pane at all). When errored, render the error inside the pane. When present, render as today. Parent component (`ResearchTab` → `DensityGrid`) gates the pane on `thesis !== null` and passes `onExpand` only if thesis history page exists (it does not yet — leave undefined). |
| `SecProfilePanel` | Wrap in `<Pane>` with title "Company profile", source `["sec_edgar"]`. `onExpand` undefined for now. |
| `FundamentalsPane` | Wrap in `<Pane>` with title "Fundamentals", scope = "last 8 quarters", source `["sec_xbrl"]`, `onExpand` routing to `?tab=financials` (existing). Drop the existing footer "View statements →" link (`FundamentalsPane.tsx:166-173`) — replaced by `PaneHeader` "Open →". |
| `FilingsPane` | Wrap in `<Pane>` with title "Recent filings", scope = "high-signal types", source = active filings providers (e.g. `["sec_edgar"]`), `onExpand` routing to `?tab=filings` (existing). Drop the existing footer "View all filings →" link (`FilingsPane.tsx:135-143`) — replaced by `PaneHeader` "Open →". |
| `InsiderActivitySummary` | Wrap in `<Pane>` with title "Insider activity", scope = "last 90 days", source `["sec_form4"]`, `onExpand` undefined for now (no insider tab/route exists). |
| `DividendsPanel` (existing, used elsewhere) | When used inside `DensityGrid`, wrap in `<Pane>` with title "Dividend history", source from active dividend providers. Drop the combined-card `overflow-auto max-h-[360px]` wrapper. Pane only rendered when `activeProviders(dividends).length > 0`. |
| `BusinessSectionsTeaser` | Wrap in `<Pane>` with title "Company narrative", scope = "10-K Item 1", source `["sec_10k_item1"]`, `onExpand` routing to existing 10-K narrative route. |
| News pane | New shared component `<RecentNewsPane>`. Renders `<Pane>` with title "Recent news" + up to 5 most-recent items. Hidden entirely when no items. `onExpand` routes to `?tab=news`. |

### D. Empty-pane policy

Codex spec-review flagged that "render nothing on empty" is too broad.
Several panes already distinguish `no-capability` from
`active-but-no-current-data` and rely on that distinction
(`DividendsPanel.tsx:97` shows upcoming dividends even with empty
history; `FilingsPane.tsx:96` distinguishes "active filings capability
but no high-signal rows" from "no capability"; `InsiderActivitySummary.tsx:55`
has its own "no Form 4 data" empty state when the provider is active).

Refined four-state rule:

| State | Definition | UI |
|---|---|---|
| **Capability inactive** | `activeProviders(cap.X).length === 0` | Parent does NOT mount the pane component. No card. |
| **Capability active, future signal only** | Provider active AND no historical data BUT pane has forward-looking content (e.g. upcoming ex-date, upcoming earnings) | Pane renders with the forward-looking content. Card visible. |
| **Capability active, no current and no forward signal** | Provider active AND no rows AND no forward content | Pane returns `null`. No card. |
| **Capability active, endpoint returned null/error** | Provider active AND data fetch failed or returned `null` body | Pane renders an explicit error/empty state inside its `<Pane>` chrome (existing `SectionError` / inline empty). Card visible. |

Per-pane mapping (replaces the broad table):

| Pane | Inactive | Active, future-only | Active, nothing | Active, fetch null/err |
|---|---|---|---|---|
| Thesis | n/a (no capability — gates on `thesis !== null`) | n/a | parent gates: `thesis === null && !errored` → null | errored → render error UI inside Pane |
| Dividends | parent gates on `activeProviders(cap.dividends).length === 0` → no mount | `data.upcoming.length > 0` → render normally with upcoming visible | `data.history.length === 0 && data.upcoming.length === 0` → child returns `null` | error → child renders error UI |
| News | parent gates on `data.items.length === 0` → no mount | n/a | `items.length === 0` → child returns `null` | error → child renders error UI |
| Insider | parent gates on `activeProviders(cap.insider).length === 0` → no mount | n/a | child renders existing "No insider data" state inside Pane (active provider but null payload) | error → child renders error UI |
| Fundamentals | parent gates on `!hasFundamentalsActive(summary)` → no mount | n/a | already self-gates (`FundamentalsPane.tsx:127`) | error → existing `SectionError` |
| Filings | parent gates on `activeProviders(cap.filings).length === 0` → no mount | n/a | child renders existing "No filings" empty inside Pane (active provider but no high-signal rows) | error → child renders error UI |
| KeyStats | n/a (always rendered when summary exists) | n/a | `stats === null` → child renders existing "No key stats" inside Pane | error → n/a (data is part of summary) |
| Narrative | parent gates on absent 10-K | n/a | child renders existing absent-narrative state | error → existing |

Net effect: no completely-blank empty cards (the thesis-dead-card and
news-placeholder problems), but panes with active capabilities still
get visible cards even when the latest fetch is empty — operator can
see "yes the data feed exists, it's just empty right now" rather than
"this capability doesn't exist for this instrument".

### E. Grid-cell sizing

`DensityGrid` uses `grid grid-cols-12 gap-2`. No `auto-rows`. No
`grid-flow-dense`. Each pane wrapper is `min-h-0` with content-driven
height. Chart pane keeps `min-h-[440px]` (existing). No internal
scrollbars (`overflow-auto` removed everywhere). Long lists (filings,
news) cap row count with `slice(0, N)` and rely on `PaneHeader.onExpand`
for "view all" via Open → affordance.

### F. Wiring summary

```
InstrumentPage
  -> ResearchTab(summary, thesis, thesisErrored)
       -> DensityGrid(summary, thesis, thesisErrored, profile = selectProfile(summary))
            -> 1-of-3 layout templates rendering child pane components directly
            -> conditional mounts use { cond && <ChildPane ... /> }
            -> each child component owns its own <Pane> wrapper internally
```

`ResearchTab` no longer hand-builds the `keyStatsBlock` / `thesisBlock`
/ `newsBlock` JSX. Each becomes its own component (`KeyStatsPane`,
`ThesisPane`, `RecentNewsPane`) and `DensityGrid` imports + mounts
them directly. `ResearchTab` becomes a thin pass-through (it stays
only for tab routing parity). `DensityGrid` itself does not import
or render the `<Pane>` primitive — only the child pane components do.

## Out of scope (locked — do not expand in implementation)

- Verdict ribbon / status badges. Codex: noise. Defer indefinitely.
- DetailSheet / drawer primitive. Codex: prove page-route insufficient
  first. Defer.
- Universal L3 raw-data contract. Per-pane opt-in only; chart's L3 is
  scoped to #576.
- New backend endpoints. None required.
- New chart workspace route. Tracked in #576.
- Indicator overlays / compare-mode for chart. Tracked in follow-ups
  on #576.
- New capability data sources. None added.
- Visual polish beyond layout (typography overhaul, color tokens,
  dark/light mode). Tracked in #559.
- Ownership pie / 13F ingest. Already deferred per refinement spec.

## Testing

### Vitest

- `PaneHeader.test.tsx` — title/scope/source/onExpand each render
  conditionally; `Open →` button calls `onExpand`; when `onExpand` is
  defined the button is rendered, when undefined it is not.
- `densityProfile.test.ts` — `selectProfile` returns `full-sec` for
  fixture with sec_xbrl + filings; `partial-filings` for fixture with
  filings but no sec_xbrl; `minimal` for fixture with neither.
- `DensityGrid.test.tsx` — extend existing tests:
  - Renders the `full-sec` profile for the existing GME-shaped fixture.
  - Renders the `partial-filings` profile for a Companies-House-only
    fixture with active dividends + insider; asserts no fundamentals
    pane, filings spans full 12 cols, insider + dividends share row 4
    (col-span-7 / col-span-5 respectively).
  - Renders the `partial-filings` profile for a fixture with filings
    but no insider and no dividends; asserts row 4 is absent entirely.
  - Renders the `minimal` profile for a crypto-shaped fixture with
    active dividends; asserts no filings/fundamentals/insider/narrative
    panes; chart + keyStats + (thesis if present) + dividends + (news
    if items).
  - Renders the `minimal` profile for a forex-shaped fixture without
    dividends; asserts only chart + keyStats render (plus optional
    thesis/news).
- Empty-pane suppression (per Section D four-state rule):
  - Thesis pane absent when `thesis === null && !errored`.
  - Dividends pane absent when `activeProviders(cap.dividends)` is
    empty (capability inactive case).
  - Dividends pane RENDERS when capability is active but only future
    ex-date is present (active + future-only state).
  - News pane absent when `data.items.length === 0`.
- `RecentNewsPane.test.tsx` — renders ≤5 items, hides when empty.

### Manual

- `/instrument/GME` — `full-sec` profile, no empty cards visible at
  1440×900, no internal scrollbars, filings pane visibly wider than
  insider pane. Dividends pane absent (no dividend history on file)
  per the empty-pane policy.
- A UK equity (Companies House only) — `partial-filings` profile,
  filings full-width. If the instrument has active dividends/insider,
  they share row 4 (insider 7-col, dividends 5-col) per the updated
  profile definition. If neither, row 4 absent.
- A crypto instrument with active CoinGecko dividends — `minimal`
  profile, chart + keyStats + dividends only; no SEC panes. If the
  instrument has no dividends capability, only chart + keyStats render
  (plus thesis/news when populated).

## Build sequence

Single PR by default. Split into 2 if pane-header standardization +
capability profiles together exceed reasonable review size.

Phase 1 — primitives (no behavior change):

1. `PaneHeader.tsx` + Vitest.
2. `Pane.tsx` wrapper component (article + hover + PaneHeader).
3. `densityProfile.ts` + Vitest.

Phase 2 — wire panes:

4. Replace `<Section>` with `<Pane>` in each instrument pane
   (`KeyStats`, `SecProfilePanel`, `FundamentalsPane`, `FilingsPane`,
   `InsiderActivitySummary`, `DividendsPanel`-when-on-instrument,
   `BusinessSectionsTeaser`).
5. Drop existing footer "View …" links from `FilingsPane.tsx:135-143`
   and `FundamentalsPane.tsx:166-173`.

Phase 3 — empty-pane suppression:

6. `ThesisPanel` returns `null` when empty.
7. New `RecentNewsPane.tsx` (replaces the `newsBlock` placeholder).
8. Dividends + insider gated in `DensityGrid` via `activeProviders`.

Phase 4 — layout:

9. `DensityGrid` switches to `grid-cols-12` + 3 profiles.
10. Drop combined dividends+insider card and its `overflow-auto`
    wrapper.
11. Update existing `DensityGrid` Vitest fixtures.

If splitting into two PRs: Phase 1+2 first (no visible layout change),
Phase 3+4 second (the layout flip the user sees).

## Risk register

| Risk | Mitigation |
|---|---|
| Profile selection produces a layout the operator doesn't expect for an edge instrument (e.g. partial-fundamentals: sec_xbrl provider listed but no data). | `selectProfile` checks `data_present` not just `providers.includes`. Fallback to `partial-filings` when fundamentals data is missing. |
| ~~Whole-card click conflicts with internal links~~ | **Resolved at design time:** `onExpand` is button-only (Decision 6). Internal links (filings rows, narrative anchor, chart range buttons) operate independently — no propagation handling needed. |
| `PaneHeader` provenance string overflows on narrow viewports. | Truncate with `text-overflow: ellipsis` on the source span; full list visible on hover via `title` attr. |
| Three profiles are not enough for some real instrument (e.g. ADR with FPI filings + no SEC XBRL but has ownership). | Add a 4th profile in a follow-up; profile system is designed to extend. v1 ships with 3. |
| `Pane` wrapper hover-state interferes with focus ring on internal buttons. | Internal buttons use `focus-visible:ring-2 focus-visible:ring-sky-500` independent of card hover state. |

## Acceptance criteria (mirror of #575 issue)

- Empty thesis/dividends/news panes do not render on load.
- No `overflow-auto` + `max-h-*` wrappers anywhere in `DensityGrid.tsx`.
- Dividends and insider in separate panes (no combined card).
- Filings pane visibly wider than insider pane on desktop viewport.
- Every pane on the instrument page uses `<Pane>` + `<PaneHeader>` with
  consistent label + source + drill affordance where applicable.
- `/instrument/GME` (full SEC), a UK Companies House instrument, and
  a crypto instrument each render a clean profile-appropriate layout
  with no holes.
- All existing Vitest tests pass; new tests cover the three capability
  profiles, empty-pane suppression, and the new `PaneHeader`.
- `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest` all green.
- `pnpm --dir frontend typecheck && pnpm --dir frontend test:unit` all green.
