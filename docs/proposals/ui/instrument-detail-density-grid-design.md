# Instrument detail page — density grid + filings rendering

Date: 2026-04-27
Status: design draft, awaiting user review

## Context

Operator just walked through `/instrument/GME` post-#552 and flagged five concrete pain points:

1. Page is too long — many stacked panels force scrolling.
2. 10-K drilldown panel is too narrow and has gaps in the section vertical line.
3. Cross-reference chips ("Item 7", "Note 5") look clickable but aren't.
4. 10-K embedded financial tables strip into garbled prose runs ("Reportable Segments\n2\nWe operate in three geographic segments…").
5. Only the most recent 10-K is reachable; thesis work needs cross-year comparison.

Adjacent data bug surfaced during the same review: `financial_periods` rows duplicate every quarter (e.g. Q4 2025 stored at both 2026-01-31 and 2026-03-18 with identical values + accession). Out of scope for this design — filed as #558.

Operator goal: Bloomberg-tier information density, strong financial reporting, fast navigation through long docs, exchange-agnostic so the per-region adapters wired in #516–#523 plug into the same shape. **No LLM** — extraction stays mechanical/programmatic.

## Decisions captured

Resolved during brainstorming with the operator (visual companion mockups; selections logged):

| # | Decision | Pick | Notes |
|---|---|---|---|
| 1 | Instrument page layout | Density grid | All structured panels visible at once; drilldowns for long-form (10-K, 8-K detail). |
| 2 | Grid pane allocation | Chart-led | Chart 2×2 top-left; right column = key stats / thesis / profile / filings; bottom row = segments / div+insider / news. |
| 3 | 10-K drilldown layout | Three-pane | Left TOC rail, center reader, right metadata rail (filing accession + prior 10-Ks + cross-related items). |
| 4 | 8-K rendering | Filterable table + detail panel | Left list (date / items / subject), right detail. Filters across severity / item code / date range. |
| 5 | Embedded financial tables | Render inline as real tables | Parser preserves `<table>` / `<tr>` / `<td>` through to a structured payload; renderer turns them into HTML tables. |
| 6 | Cross-ref chips | Hover preview popover | Click chip → 240-char preview + "Open full" link. No same-doc anchor jump in v1. |

## Architecture

### A. Instrument page (`/instrument/:symbol`)

Sticky `SummaryStrip` (already exists) + density grid below. Grid uses CSS `grid-template-columns: 2fr 1fr 1fr` and `grid-auto-rows: 220px` with explicit row spans so the chart pane occupies a 2×2 cell.

Pane layout (top-to-bottom, left-to-right):

```
┌──────────────────────┬─────────────┬─────────────┐
│                      │ Key stats   │ Thesis      │
│   Price chart        │ + position  │ + score     │
│   (1d/1w/1m/1y/5y)   ├─────────────┼─────────────┤
│                      │ SEC profile │ Filings     │
│                      │ + headcount │ (8-K + 10-K)│
├──────────────────────┴─────────────┼─────────────┤
│ Segments + Geographic mix          │ Recent news │
├────────────────────────────────────┴─────────────┤
│ Dividends + Insider activity (combined card)     │
└──────────────────────────────────────────────────┘
```

Each pane is a self-contained component reading its own data via `useAsync`. Independent loading / error states; one slow fetch never blanks neighbours. Internal scroll on overflow rather than pushing the page taller.

Responsive collapse: at viewport widths below `lg` (~1024 px) the grid degrades to a single column. Pane order reflects priority: chart → key-stats → thesis → filings → SEC-profile → segments → dividends-insider → news.

The existing tab nav (Research / Financials / Positions / News / Filings) stays — but Research becomes the density grid (replaces today's stacked-panel page); Financials tab inherits today's table; Positions / News / Filings unchanged. Cross-link: clicking a filing row in the Filings pane navigates to its drilldown route (10-K / 8-K).

### B. 10-K drilldown (`/instrument/:symbol/filings/10-k[?accession=...]`)

Three-pane layout, full-page width with the main reader pane the widest:

```
┌──────────┬───────────────────────────┬───────────────┐
│ TOC      │ Reader                    │ Metadata      │
│ (180px)  │ (flex, min-width 0)       │ (200px)       │
│          │                           │               │
│ General  │ Form 10-K · Item 1        │ FILING        │
│ Priorities│ accession 0001326...     │ 2026-03-24    │
│ Segments │                           │ 0001326380... │
│ Comp.    │ ## General                │               │
│ Human    │ GameStop Corp. ("we"...)  │ PRIOR 10-Ks   │
│ Cap…     │ ┌──────────────┐          │ 2025 ↗        │
│          │ │ Stores table │          │ 2024 ↗        │
│ ──       │ └──────────────┘          │ 2023 ↗        │
│ RELATED  │                           │               │
│ Item 1A↗ │ ## Reportable Segments    │ RELATED ITEMS │
│ Item 7↗  │ We operate in three…      │ Item 1A ↗     │
│ Item 8↗  │                           │ Item 7 ↗      │
└──────────┴───────────────────────────┴───────────────┘
```

Component file paths: `frontend/src/pages/Tenk10KDrilldownPage.tsx` (already exists; refactor for three-pane + width). Renames consider but defer.

Width fix: replace today's `max-w-6xl` container with full-bleed `max-w-screen-2xl mx-auto` and explicit grid template columns `grid-cols-[180px_minmax(0,1fr)_200px]` so the centre pane gets all remaining width. Min-width 0 on the centre column prevents whitespace overflow squashing the rail.

Vertical-line gap fix: today every section uses `border-l-2 border-slate-200 pl-4` on the article wrapper. Gaps appear because section bodies that contain block-level children with their own margins push the next sibling down outside the bordered area. Fix: render the bordered rail on the section column itself (a CSS `::before` decoration with `position: absolute; inset-block: 0`) so the line is continuous regardless of body content.

Embedded tables: parser change. The current `_strip_html` collapses `<table>` to whitespace. New behaviour:

1. Pre-strip pass detects `<table>` blocks and substitutes a placeholder sentinel `␞TABLE_N␞` while preserving the table HTML in a parallel list keyed by N.
2. After section extraction, each section's body carries `tables: ParsedTable[]` alongside the prose; sentinels in the prose mark insertion points.
3. Renderer walks the body and substitutes `<table>` JSX where sentinels appear.
4. Schema: extend `instrument_business_summary_sections` with `tables_json JSONB` (array of `{order, headers, rows}`). Migration adds nullable column; null = no embedded tables (current state for every existing row).

Cross-ref popover: on click of a chip with `reference_type='item'` and a target whose section is already ingested (today: only Item 1; later Items 1A / 7 / 8), pop a 240-char excerpt + "Open full" link that routes to that item's drilldown. For unresolvable targets ("Note 5" — no Note ingestion yet) or out-of-doc references ("Exhibit 21"), the popover shows a "Source: SEC iXBRL viewer ↗" link instead of a preview.

Historical 10-Ks: right rail queries `filing_events` for `(instrument_id, filing_type IN ('10-K','10-K/A'))` ordered by `filing_date DESC`. Each prior filing renders as a link to the same drilldown route with `?accession=...`. Default route (no `?accession=`) loads the latest. Existing endpoint `GET /instruments/{symbol}/business_sections` extends with optional `?accession=` query param; the parser already keys on `source_accession` per-row so the data is already there for any 10-K we've ingested.

### C. 8-K rendering (`/instrument/:symbol/filings/8-k`)

New route. Replaces today's `EightKEventsPanel` inline rendering. Layout:

```
┌─────────────────────────────┬───────────────────────┐
│ Filter strip                │ Detail panel          │
│ [severity] [items] [date]   │ (selected row)        │
│                             │                       │
│ Date    │ Items   │ Subject │ Item 5.02 · CFO depart│
│ ─────────────────────────── │ Effective Sept 13…    │
│ 2026-03 │ 2.02    │ Q4 res  │                       │
│ 2025-12 │ 8.01    │ Div…    │ Exhibits              │
│ 2025-09 │ 5.02    │ CFO out │ · 99.1 Press release  │
│ …                           │                       │
└─────────────────────────────┴───────────────────────┘
```

Existing endpoint `GET /instruments/{symbol}/eight_k_filings` already returns the structured payload. Frontend changes only:

- `EightKListPage.tsx` (new) — table + detail layout.
- Filter state in URL query string so deep-links work.
- Severity colour-coding on item code chips (red / amber / slate from existing `severity` field).

The instrument-page Filings pane shows a 5-row list (date + item chips + subject); clicking a row routes here with that row pre-selected.

### D. Data path summary

No new ingest pipelines. Schema changes:

| Table | Change |
|---|---|
| `instrument_business_summary_sections` | Add `tables_json JSONB` nullable. Empty array on existing rows after backfill. |

Two sibling endpoints to add (frontend-only or thin wrappers over existing data):

- `GET /instruments/{symbol}/filings/10-k?accession=...` — existing `business_sections` endpoint extended with `accession` query param + a sibling endpoint `GET /instruments/{symbol}/filings/10-k/history` returning the prior-10-Ks list for the right rail.
- No new 8-K endpoint; reuse existing `/eight_k_filings` from the table page.

### E. Out-of-scope / follow-ups

These are noted but tracked in separate tickets to keep this design shippable in one plan:

- **Duplicate-quarter rows in `financial_periods`** (#558): real bug — every quarter stored twice with different `period_end_date`. Filed.
- **Items 1A / 7 / 8 ingest**: cross-ref popover degrades gracefully when targets aren't ingested. Ingesting other 10-K items is its own design.
- **XBRL segments / geographic facts**: covered by #554 (already filed).
- **Per-region 10-K equivalents** (UK Companies House annual report, EU Universal Registration Document): the parser, drilldown route, and grid pane shapes are exchange-agnostic. The data adapter for each region is per-ticket #516–#523.

## Testing

- Unit: parser round-trip on a fixture with `<table>` embedded — sentinel insertion + render.
- Visual regression: snapshot test on `Tenk10KDrilldownPage` for three-pane width / vertical-line continuity.
- Integration: existing GME 10-K parses to ≥4 sections (already pinned by #550); regression test extends to assert at least one section has a non-empty `tables_json`.

## Build sequence

Phase 1 — schema + parser (one PR):

1. Migration: `tables_json` column on `instrument_business_summary_sections`.
2. Parser: detect `<table>` blocks, sentinel-substitute, persist parsed table data.
3. Backfill: re-parse all SEC-CIK instruments via `bootstrap_business_summaries`.

Phase 2 — 10-K drilldown rebuild (one PR):

4. Three-pane layout component, full-bleed container, vertical-line fix.
5. Right-rail historical filings list + accession-aware route param.
6. Cross-ref hover preview popover.
7. Embedded-table renderer.

Phase 3 — instrument page density grid (one PR):

8. Replace stacked Research-tab content with the chart-led grid.
9. Pane priority order for responsive collapse.
10. New 5-row Filings pane that links to drilldowns.

Phase 4 — 8-K detail page (one PR):

11. New `EightKListPage` route.
12. Filter strip + table + detail panel.
13. Wire instrument-page Filings pane link to here.

Each phase is an independent PR. Phase 1 unblocks Phase 2 (`tables_json` data needed); Phase 3 + 4 are independent of each other and can land in either order after Phase 2.
