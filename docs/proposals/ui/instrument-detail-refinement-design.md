# Instrument detail page — refinement (#567)

Date: 2026-04-27
Status: design draft (post Codex round 1 review), no further user input pending — operator stepped away

## Context

The Phase 3 density grid (PR #566) and Phase 4 8-K page (#568) shipped, but operator review flagged refinement needs:

1. **FilingsPane shows wrong types.** Calls `/filings/{id}?limit=5` which dominates with Form 4 (insider) + 144 (sale notice) noise. Operator can't see 8-K or 10-K rows from the grid → no path into the new `/filings/8-k` or `/filings/10-k` drilldowns.
2. **Fixed-height cells waste/clip space.** `lg:auto-rows-[220px]` forces every pane to a uniform 220 px even when content is shorter or longer. Panes with short stats waste vertical space; panes with long body force internal scrollbars.
3. **Internal scrollbars on multiple panes.** Operator wants Yahoo-Finance-style "fits the screen, no chopped boxes" feel.
4. **Charts missing.** No fundamentals time-series. Operator referenced Bloomberg-tier visual analysis as the bar.
5. **Typography + spacing.** Some labels too small, some panes have too much padding.

Operator stepped away — design here is locked via Codex collaboration, not user clarifying questions.

## Decisions captured (with rationale)

| # | Decision | Pick | Rationale |
|---|---|---|---|
| 1 | FilingsPane filter | Backend widens `/filings/{id}?filing_type=` to accept CSV; frontend passes a single static concat list | Single endpoint stays canonical; CSV is one query string change; one static list works for US + FPI without runtime issuer checks. |
| 2 | Significant filing types | Concatenated US + FPI list passed unconditionally: `8-K,8-K/A,10-K,10-K/A,10-Q,10-Q/A,6-K,6-K/A,20-F,20-F/A,40-F,40-F/A` | Codex flagged that a US-only list regresses ADR coverage. Issuer-aware switching adds complexity; the backend filters on `filing_type = ANY(...)` so listing FPI types alongside US types is harmless on US instruments (no rows match) and correct for foreign issuers. Single static CSV. |
| 3 | ROW_LIMIT | Bump from 5 → 6 to give 10-Q/6-K headroom alongside 8-K/10-K | Empirically: a 10-K every 12 months + 4 10-Q/year + ~6 8-K/year = 11 filings/year. 6 rows captures the most-recent 2 months of operator-relevant filings. |
| 4 | Grid row sizing | Replace `auto-rows-[220px]` with content-driven `auto` rows | Content-driven heights eliminate wasted space and internal scrollbars. Each pane sets its own minimum; chart pane gets explicit `min-h-[440px]`. |
| 5 | Pane wrappers | Drop unconditional `overflow-auto` | Panes that COULD overflow handle their own caps internally — no scrollbars. Trim rows / use ellipsis instead. |
| 6 | Sparkline library | Hand-coded SVG `<polyline>` (no new dep) | `lightweight-charts` is OHLCV-optimised; `recharts` adds ~400 KB. Fundamentals sparklines are 8-point line charts — trivial inline SVG. Test asserts the `<polyline>` `points` attribute, not "path data" (Codex round-1 nit). |
| 7 | Fundamentals data path | TWO parallel `useAsync` calls — `/financials?statement=income` + `/financials?statement=balance` — combined client-side | Codex round-1 flagged that the income statement alone doesn't carry debt fields. Real schema (`app/api/instruments.py:390-421`): income has `revenue / operating_income / net_income`; balance has `long_term_debt / short_term_debt`. Two parallel requests, each cheap. |
| 8 | Fundamentals pane gating | Render only when `summary.capabilities.fundamentals.providers` contains `sec_xbrl` AND `data_present[sec_xbrl]` is true | Codex round-1 flagged the financials tab is gated; the new pane and its "View statements" footer link must be gated the same way. Otherwise non-SEC instruments get a dead pane. |
| 9 | Insider summary | Use the EXISTING aggregated `/instruments/{symbol}/insider_summary` payload as-is — no weekly bars in this PR. **Lens: total-activity** (all reported insider transactions, not just open-market). Display fields: NET 90d = `total_acquired_shares_90d - total_disposed_shares_90d` (computed client-side, signed), ACQUIRED = `total_acquired_shares_90d`, DISPOSED = `total_disposed_shares_90d`, TXNS = `acquisition_count_90d + disposition_count_90d`, LATEST = `latest_txn_date` | Codex round-1: weekly series not in contract. Codex round-2 + 3 + 4: pick a single lens, no API field mixing. The API exposes both an "open-market" lens (`open_market_*` + the legacy aliases `net_shares_90d` / `buy_count_90d` / `sell_count_90d` which map to open-market) AND a "total-activity" lens (`total_*_shares_90d` + `acquisition_count_90d` / `disposition_count_90d`). We use total-activity throughout. NET 90d must be computed client-side from the acquired/disposed share fields — the `net_shares_90d` legacy alias is open-market and would cross lenses. |
| 10 | "View all" links | ONLY on FilingsPane (routes to `?tab=filings`, which exists). Drop the spec mention for Insider/Dividends. | Codex round-1: no `insider` or `dividends` tab exists. Don't ship dead links. |
| 11 | Ownership pie | Defer until 13F ingest lands | No `13F` ingestion → can't split institutional vs public float. |

## Architecture

### A. Backend — `/filings/{id}` accepts CSV `filing_type=`

Current: `filing_type=10-K` matches one type.
New: `filing_type=10-K,10-K/A,8-K,8-K/A` matches any in the list.

```python
# app/api/filings.py
filing_type: str | None = Query(default=None)
# ...
if filing_type is not None:
    types = [t.strip() for t in filing_type.split(",") if t.strip()]
    where_clauses.append("filing_type = ANY(%(filing_types)s)")
    filter_params["filing_types"] = types
```

Backwards-compatible: old single-value callers split into a 1-element list.

Test: extend `tests/test_api_filings.py` with a CSV-multiple-types case + assert non-listed types are excluded.

### B. FilingsPane — filter to high-signal types + footer link

```ts
// frontend/src/components/instrument/FilingsPane.tsx
const SIGNIFICANT_FILING_TYPES = [
  // US issuers
  "8-K", "8-K/A", "10-K", "10-K/A", "10-Q", "10-Q/A",
  // FPI / ADR — keep the same pane shape working for non-US instruments
  "6-K", "6-K/A", "20-F", "20-F/A", "40-F", "40-F/A",
].join(",");

const ROW_LIMIT = 6;

// fetchFilings call:
fetchFilings(instrumentId, 0, ROW_LIMIT, { filing_type: SIGNIFICANT_FILING_TYPES });

// drilldownLink already maps 10-K → /filings/10-k and 8-K → /filings/8-k.
// 10-Q / 20-F / 40-F / 6-K rows get NO drilldown link in v1 — render the
// row as a non-clickable text line. Follow-up ticket can add their drilldowns.

// Footer:
<div className="mt-2 border-t border-slate-100 pt-1.5 text-right">
  <Link
    to={`/instrument/${encodeURIComponent(symbol)}?tab=filings`}
    className="text-[11px] text-sky-700 hover:underline"
  >
    View all filings →
  </Link>
</div>
```

`fetchFilings` API client extends to accept an optional `{ filing_type?: string }` opts arg.

### C. DensityGrid — content-driven row heights

Replace:

```tsx
<div className="grid grid-cols-1 gap-3 lg:grid-cols-[2fr_1fr_1fr] lg:auto-rows-[220px]">
```

With:

```tsx
<div className="grid grid-cols-1 gap-2 lg:grid-cols-[2fr_1fr_1fr]">
```

Each pane wrapper drops unconditional `overflow-auto`:

```tsx
// Before:
<div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">

// After:
<div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm">
```

Chart pane keeps `min-h-[440px]` to retain its tall footprint regardless of content.

### D. New `Sparkline` component

`frontend/src/components/instrument/Sparkline.tsx`:

```tsx
export interface SparklineProps {
  readonly values: ReadonlyArray<number>;
  readonly width?: number;     // default 80
  readonly height?: number;    // default 24
  readonly stroke?: string;    // default "currentColor"
  readonly className?: string;
}

export function Sparkline({
  values, width = 80, height = 24, stroke = "currentColor", className,
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
    <svg width={width} height={height} className={className} aria-hidden="true">
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

Test asserts `<polyline>` is rendered with the correct `points` attribute (8 comma-separated coords for 8 input values). One unit test only.

### E. New `FundamentalsPane` — sparkline grid (gated)

`frontend/src/components/instrument/FundamentalsPane.tsx`:

- Two parallel `useAsync` calls: `fetchInstrumentFinancials(symbol, { statement: "income", period: "quarterly" })` + `fetchInstrumentFinancials(symbol, { statement: "balance", period: "quarterly" })`.
- **Period-safe join:** keyed on `(period_end, period_type)`. Build a `Map<periodKey, {income?: row, balance?: row}>`, drop entries where either side is missing, sort by `period_end DESC`, slice the latest 8.
- Build `{ revenue: number[], operatingIncome: number[], netIncome: number[], totalDebt: number[] }` arrays from the JOINED set (so all four sparklines plot the same 8 quarters). `totalDebt = (long_term_debt ?? 0) + (short_term_debt ?? 0)` per period. If the joined set has fewer than 2 periods, the pane renders an `EmptyState` ("Not enough fundamentals history to chart").
- Render a 4-column `<div className="grid grid-cols-2 gap-3 sm:grid-cols-4">` with one labelled sparkline per metric.

Each cell:

```tsx
<div className="flex flex-col items-start">
  <span className="text-[10px] uppercase tracking-wider text-slate-500">Revenue</span>
  <Sparkline values={fundamentals.revenue} stroke="#0ea5e9" className="text-sky-500" />
  <span className="text-xs font-medium tabular-nums text-slate-800">
    {formatLatest(fundamentals.revenue)}
  </span>
</div>
```

Footer link routes to `?tab=financials`. Pane RENDERS only when `summary.capabilities.fundamentals.providers.includes("sec_xbrl") && summary.capabilities.fundamentals.data_present.sec_xbrl === true` (Codex round-1: same gate as the financials tab itself).

Empty / loading: standard `SectionSkeleton` while either request is pending; render an empty `EmptyState` if both return zero rows.

### F. `InsiderActivitySummary` — replaces inline list in the grid

`frontend/src/components/instrument/InsiderActivitySummary.tsx`:

Reads existing `/instruments/{symbol}/insider_summary`. Renders a compact summary block:

```text
NET 90d        ACQUIRED    DISPOSED    TXNS         LATEST
+24,061 sh     42,392 sh   18,331 sh   38           2026-04-13
```

Where:
- `NET 90d` = `total_acquired_shares_90d - total_disposed_shares_90d` (computed client-side, signed, with leading `+` / `-` and arrow glyph for sign). The `net_shares_90d` legacy alias on the response is the open-market net, NOT total-activity — do not use it here or it crosses lenses.
- `ACQUIRED` = `total_acquired_shares_90d` (always positive, "sh" suffix).
- `DISPOSED` = `total_disposed_shares_90d` (always positive, "sh" suffix — the negative shown in earlier draft was a display error; the field is unsigned magnitude).
- `TXNS` = `acquisition_count_90d + disposition_count_90d` (integer count, no unit — total-activity lens, matches the share fields above).
- `LATEST` = `latest_txn_date` (ISO date, dash if null).

No weekly bars. No footer link (no insider tab today).

Existing `InsiderActivityPanel` (the row list) stays unchanged; it's used elsewhere on the page (or will be — it currently renders inside DensityGrid; this PR replaces THAT use with `InsiderActivitySummary` and keeps the row-list component file for future use on a dedicated page).

### G. Layout — refined density grid

```
┌────────────────────────────────┬───────────────┬───────────────┐
│ Price chart                    │ Key stats     │ Thesis        │
│ (1w/1m/3m/6m/1y/5y/max picker) │ (10 stats)    │ (memo +       │
│ 440 px tall                    │ tabular nums  │  base/bull/   │
│                                │               │  bear)        │
├────────────────────────────────┼───────────────┼───────────────┤
│ Fundamentals (4 sparklines)    │ SEC profile   │ Filings (6    │
│ Revenue · Op income · Net      │ industry,     │ rows, type-   │
│ income · Debt                  │ exchange,     │ filtered) +   │
│ (gated on sec_xbrl)            │ headcount     │ view-all link │
├────────────────────────────────┴───────────────┼───────────────┤
│ Company narrative (10-K Item 1 teaser)         │ Insider       │
│ + view-full link                               │ summary       │
│                                                │ (net, acq,    │
│                                                │  disp, txns)  │
├────────────────────────────────────────────────┴───────────────┤
│ Dividends (history + next ex-date)              + Recent news  │
└────────────────────────────────────────────────────────────────┘
```

Below `lg`: single column. Pane order = priority: chart → key stats → thesis → fundamentals → filings → SEC profile → narrative → insider → dividends → news.

If `fundamentals/sec_xbrl` is inactive, `FundamentalsPane` does NOT render; the bottom-left half of row 2 collapses to just `SecProfilePanel`. Acceptable — non-SEC instruments are rarer.

### H. Spacing + typography refinements

| Token | Before | After |
|---|---|---|
| Grid gap | `gap-3` | `gap-2` |
| Pane padding | `p-3` | `px-3 py-2.5` (asymmetric — tighter vertical) |
| Section title | `text-base font-semibold` | `text-xs uppercase tracking-wider text-slate-500` |
| Stat label | `text-slate-500 text-sm` | `text-slate-600 text-xs` (better contrast at smaller size) |
| Stat value | `text-sm` | `text-sm font-medium tabular-nums` |
| Stat row gap | `gap-y-2` | `gap-y-1` |
| Section spacing inside pane | `space-y-3` | `space-y-2` |

WCAG 4.5:1 still met (slate-600 on white = 6.4:1).

### I. Out-of-scope / follow-ups

- **Ownership pie** — needs 13F ingest (separate ticket).
- **Site-wide visual polish** — typography overhaul, color tokens, dark/light mode pass — covered by parent ticket.
- **Insider weekly series** — needs backend scope on `/insider_summary` to add a per-week aggregate. Separate ticket.
- **10-Q / 20-F drilldown routes** — only 8-K and 10-K have drilldown pages today. 10-Q rows in FilingsPane render as plain text. Separate ticket.
- **Dividends / Insider dedicated tabs** — operator can use `?tab=...` once those tabs exist. Not introduced here.

## Testing

- **Backend:** `tests/test_api_filings.py` extended with `?filing_type=10-K,8-K` case asserting both types returned and Form 4 / 144 excluded.
- **Frontend Vitest:**
  - `FilingsPane` test asserts `fetchFilings` is called with the expected CSV string + asserts the "View all filings →" link routes to `?tab=filings`.
  - `Sparkline` test renders a `<polyline>` and asserts the `points` attribute has 8 comma-separated coords for 8 input values.
  - `FundamentalsPane` test mocks both income + balance financials calls, asserts 4 sparklines render, asserts `totalDebt` is `long_term_debt + short_term_debt` per period.
  - `InsiderActivitySummary` test asserts the four metrics render from the mocked payload.
  - `DensityGrid` existing tests stay green; update fixture if needed for the new `FundamentalsPane` capability gate.
- **Manual:** load `/instrument/GME` and confirm:
  - 8-K and 10-K rows visible in FilingsPane (and 10-Q underneath if recent).
  - Click "View all filings →" routes to the Filings tab.
  - No internal scrollbars on any pane at 1440×900.
  - Fundamentals pane shows 4 sparklines.
  - Insider summary shows net + buy + sell + latest, no row list.

## Build sequence

Phase A — backend filter + frontend filings fix (one PR):

1. `/filings/{id}` accepts CSV `filing_type=`.
2. `FilingsPane` calls with the SIGNIFICANT_FILING_TYPES CSV + ROW_LIMIT 6 + adds footer link.
3. Vitest update.

Phase B — grid sizing + typography refinements (one PR):

4. Drop `auto-rows-[220px]`.
5. Drop `overflow-auto` from pane wrappers.
6. Tighten spacing tokens per Section H.
7. Asymmetric padding on panes.
8. Existing DensityGrid tests adjusted if needed.

Phase C — fundamentals sparklines (one PR):

9. `Sparkline.tsx` shared component + Vitest.
10. `FundamentalsPane.tsx` reading both income + balance, gated on `sec_xbrl`.
11. Insert into the grid layout.

Phase D — insider summary (one PR):

12. `InsiderActivitySummary.tsx` (new) replacing the inline `InsiderActivityPanel` in the grid.
13. Existing `InsiderActivityPanel` stays for future use elsewhere (no delete).

Each phase is an independent PR. Phase A and Phase B can land in either order. Phase C depends on Phase B (grid layout shape). Phase D is independent of B + C.
