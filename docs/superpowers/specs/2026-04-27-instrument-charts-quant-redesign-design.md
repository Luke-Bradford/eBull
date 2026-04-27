# Instrument page — quant-grade chart redesign (parent spec)

Date: 2026-04-27
Status: design draft, ticket sequence approved, execution deferred to next session
Parent epic: filed as new GH issue (see "Tickets" below)
Predecessors: #575 (polish round 2 — landed), #576 (chart workspace L2 — landed phases 1-4), #578 (dividends drill-through — landed)

## Operator-stated problem (2026-04-27 review)

Two distinct asks:

1. **Overview-page charts are "basic Bob".** PriceChart compact, no hover-rich tooltip, no line/area toggle, white background clashes with dark slate page. Sparklines (FundamentalsPane) too small to be useful.
2. **Drill pages need a quant-grade analytical view.** Operator wants what a data scientist / quant would propose for equity research: margin trends, DuPont, drawdown, insider clusters, sector radar, etc. Insider activity in particular regressed from rich panel to 5-stat summary in #574 with no drill route.

User direction:
- Don't bloat overview chrome — keep glance compact
- Drill pages can be richer (real workspaces)
- Apply UI/UX skills + frontend-design skills + quant/data-science framing
- Cohesive color scheme tied to dark slate page theme

## Settled decisions

| # | Decision | Pick | Why |
|---|---|---|---|
| 1 | Charting library for non-OHLC viz | **`recharts`** (npm package, MIT, ~96KB gz tree-shaken) | Declarative React JSX matches codebase; covers bar/line/pie/area/radar/scatter/composed; lazy-loaded per drill page so overview bundle untouched |
| 2 | Charting library for OHLC | **Keep `lightweight-charts`** (already in) | Already excellent at OHLC + volume; canvas-based; battle-tested |
| 3 | Charting library — when to use what | OHLC/price/volume → lightweight-charts. Everything else → recharts. One-offs (heatmaps, weird viz) → hand-roll SVG | Two-lib mix is the standard fintech move |
| 4 | Charts NOT picked | plotly (3MB bundle), echarts (JSON-config, non-React-idiomatic), Highcharts (commercial license required), nivo (200+KB/chart heavier than recharts) | See bundle/ergonomic table below |
| 5 | Universal drill contract | **L1 / L2 / L3** continues from #576: L1 = compact overview pane (always Pane chrome), L2 = dedicated route `/instrument/:symbol/<domain>` for rich charts, L3 = raw data table + CSV export inside L2 via `?view=raw` | Same vocabulary every domain — already works for chart, dividends |
| 6 | Color theme | New `frontend/src/lib/chartTheme.ts` constants file. Dark-mode chart palette (slate-900 bg, slate-700 grids, emerald/red up-down, accent rotation cyan/blue/purple/amber/pink/lime). Applied to BOTH lightweight-charts options + recharts components. | Currently charts render on white inside dark slate page — visual clash. |
| 7 | When to apply chart theme | Charts now (this work). Site-wide typography + color tokens defer to existing tracker #559 | Don't widen scope; charts are the operator's complaint |
| 8 | Quant data we don't have yet | Beta vs benchmark, peer-comp radar, sector heatmap need new data ingest. File as data-pipe sub-tickets when they come up; stub-render until ingest lands | Don't block visual work on backend |
| 9 | What CHARTS for what data (quant view) | See "Per-domain chart catalog" below | Long-horizon thesis vocabulary, NOT day-trading TA |
| 10 | Add `tremor` (recharts wrapper)? | **No, not now** | Adds layer; reconsider if we want pre-styled dashboard primitives later |

## Why recharts (decision 1 detail)

| Lib | Bundle (gz) | Chart types | API | OHLC | Verdict |
|---|---|---|---|---|---|
| **recharts** | ~96KB | 15+ standard | declarative JSX | no | **picked** |
| visx | varies | low-level primitives | composable | hand-roll | flexible but more code per chart |
| nivo | 200+KB/chart | 20+ polished | declarative | no | heavier + opinionated |
| tremor | ~80KB | wraps recharts | dashboard-shaped | no | could layer on later |
| echarts-for-react | ~280KB | 30+ incl candlestick/heatmap | JSON config | yes | non-React-idiomatic, heavier |
| chart.js | ~80KB | 8 standard | imperative | basic | older feel, less React |
| plotly | ~3MB | 40+ scientific | declarative | yes | bundle brutal — only if specific quant viz needs it |
| Highcharts | ~150KB | 20+ | declarative | yes | **commercial license — skip** |

Decision: recharts is the React-idiomatic, MIT-licensed, tree-shakable, lazy-loadable fit. Lightweight-charts stays for OHLC (already in, no replacement needed). Hand-rolled SVG for one-off viz (already do this for Sparkline, dividend bars).

## Color theme (decision 6 detail)

`frontend/src/lib/chartTheme.ts`:

```ts
export const chartTheme = {
  bg:        "#0f172a", // slate-900 (matches page shell)
  bgAlt:     "#1e293b", // slate-800 (for inset panels)
  gridLine:  "#334155", // slate-700, alpha 0.4 in lightweight-charts
  axisText:  "#94a3b8", // slate-400
  textMuted: "#64748b", // slate-500
  textBody:  "#cbd5e1", // slate-300

  up:        "#10b981", // emerald-500
  down:      "#ef4444", // red-500
  neutral:   "#64748b", // slate-500
  warning:   "#f59e0b", // amber-500
  positive:  "#22c55e", // green-500 (slightly different from up — for fundamentals positivity)
  negative:  "#dc2626", // red-600 (slightly different from down)

  accent: [
    "#06b6d4", // cyan-500
    "#3b82f6", // blue-500
    "#a855f7", // purple-500
    "#f59e0b", // amber-500
    "#ec4899", // pink-500
    "#84cc16", // lime-500
  ],

  volumeAlpha: 0.3,
} as const;
```

Apply in both:
- lightweight-charts: pass these into `createChart(...)` options (background, grid, textColor, scale borders, candle up/down)
- recharts: pass via `<XAxis stroke={chartTheme.axisText}>`, `<CartesianGrid stroke={chartTheme.gridLine} />`, etc. or via a wrapped recharts theme provider component

## Per-domain chart catalog (decision 9)

This is the quant-DS-shaped vocabulary for long-horizon equity research. Day-trader TA (RSI/MACD/candle patterns) is secondary — already covered enough by the chart workspace.

### Fundamentals (highest impact)
**L1 (overview):** existing 4 sparklines (Revenue / Op income / Net income / Total debt) — keep
**L2 (`/instrument/:symbol/fundamentals`):**
- Revenue + COGS + Opex stacked bar (per quarter)
- Margin trends multi-line (gross / operating / net) — most important quant signal
- YoY growth rates bar chart (revenue, EPS, FCF)
- Cash flow waterfall (operating → investing → financing → net change in cash)
- Balance sheet structure: assets vs liabilities horizontal stacked
- Debt structure: long-term vs short-term over time + interest coverage trend
- DuPont decomposition: ROE = NPM × Asset turnover × Equity multiplier (3-line composed chart)
- ROIC trend line
- Free cash flow yield over time

**L3:** raw statement table (existing `Financials` tab repurposed) + CSV export

### Insider activity (biggest current gap — currently ZERO drill)
**L1 (overview):** existing 5-stat summary (NET 90d / Acquired / Disposed / Txns / Latest) — keep
**L2 (`/instrument/:symbol/insider`):**
- Net shares (acquired - disposed) per month bar chart, ±coloured
- Top officers horizontal bar (net 90d activity by name) — top 10
- Transaction scatter overlaid on price chart (optional, opt-in)
- Buying/selling cluster timeline (highlight unusual concentrations)

**L3:** Form-4 transaction list (full, sortable by date / officer / shares / value) + CSV export

### Dividends (already shipped #580 — upgrade)
**L1 (overview):** existing compact panel (TTM yield, last 4Q bars, upcoming banner) — keep
**L2 (`/instrument/:symbol/dividends` — already exists):** ADD:
- DPS over time line chart (currently just bar list)
- Payout ratio trend (needs FCF data joined — verify availability)
- Yield-on-cost progression (only when held — uses position cost basis)
- Cumulative dividends paid (line chart)

**L3:** raw history table — already shipped

### Price/Returns/Risk (`/instrument/:symbol/risk`)
**L2:**
- Drawdown chart (peak-to-trough %, underwater chart)
- Rolling annualized vol (line, 30-day window)
- Returns histogram (distribution of daily returns)
- Beta vs benchmark scatter (needs benchmark data — likely SPY for US)
- Correlation matrix with sector peers (needs peer set + correlation calc)

**L3:** raw OHLCV (already shipped in chart workspace)

### Filings (`/instrument/:symbol/filings/analytics` — extends existing list views)
**L2:**
- Filing density timeline (bar per quarter)
- Red-flag score trend over time
- Form-type heatmap by quarter

### News (`/instrument/:symbol/news-analysis`)
**L2:**
- Sentiment score line over time
- News volume bar per week
- Source breakdown pie

### Peer comparison (`/instrument/:symbol/peers`)
**L2:**
- Multi-factor radar (P/E, ROE, growth, margin, debt) vs sector median
- Sector heatmap (instrument vs peers across factors)
- Peer return scatter

## Tickets (file these in next session — order = priority recommendation)

### Phase 1: Foundation

1. **`feat: chart theme + recharts dep`** — adds `recharts` to package.json (lazy-loaded), creates `chartTheme.ts` constants, applies dark-mode theme to existing `lightweight-charts` (PriceChart + ChartWorkspaceCanvas), adds a small recharts theme wrapper. No new charts yet — foundational only.

2. **`feat: PriceChart overview polish`** — backport rich tooltip from `ChartWorkspaceCanvas.RichTooltip`, add line/area chart toggle, log-scale toggle, whole-card-click drill (so operator discovers workspace), apply chartTheme. Stays compact (~340px).

### Phase 2: Drill pages (highest impact first)

3. **`feat: insider drill page /instrument/:symbol/insider`** — biggest gap (currently zero drill). Net-by-month bar, top officers horizontal bar, full Form-4 transaction list with CSV export. Reuses `<Pane>` chrome.

4. **`feat: fundamentals drill page /instrument/:symbol/fundamentals`** — replaces thin existing Financials tab. Margin trends, YoY growth, cash flow waterfall, balance sheet structure, debt structure, DuPont decomp, ROIC, FCF yield. Recharts-heavy.

5. **`feat: dividends drill upgrade`** — adds DPS line, payout ratio, yield-on-cost (when held), cumulative paid. Builds on #580.

6. **`feat: risk/returns drill page /instrument/:symbol/risk`** — drawdown, rolling vol, returns histogram, beta scatter (note: beta needs benchmark data — file sub-ticket if not available).

### Phase 3: Lower-priority drill pages (defer if scope creeps)

7. **`feat: filings analytics drill`** — density timeline, red-flag trend, form-type heatmap. Extends existing 8-K/10-K list views.

8. **`feat: news analytics drill`** — sentiment line, volume bar, source pie.

9. **`feat: peer-comp radar`** — multi-factor radar, sector heatmap. Likely needs peer-set ranking data first (depends on existing rankings infrastructure).

### Cross-cutting

10. **(if needed) `data: ingest peer set per instrument`** — for peer-comp ticket.
11. **(if needed) `data: ingest benchmark candle data (SPY)`** — for beta scatter.

## Phasing recommendation (when next session opens)

1. File parent epic GH issue + all sub-tickets
2. Start with **Phase 1.1** (theme + recharts) — small, foundational, unblocks everything
3. Phase 1.2 (PriceChart polish) — easy operator-visible win
4. Then **Phase 2.3** (insider drill) — biggest gap
5. Then **Phase 2.4** (fundamentals drill) — biggest impact
6. Stop after Phase 2 first run; checkpoint with operator before Phase 3

Each ticket = its own PR following the standard branch / Codex / push / review cycle established this session.

## Open questions for next session

1. Drill page navigation: where do operators discover the new routes? Likely `Open →` button on each L1 pane (already established pattern). Confirm wiring in each ticket's spec.
2. Tab vs route for fundamentals: existing Financials TAB (`?tab=financials`) vs new ROUTE (`/fundamentals`). Recommend: keep tab pointing at raw statement table (it's the L3), make new ROUTE the L2 analytical view. Operators on the tab see "View analytics →" link to the route.
3. Quant data we don't have:
   - Beta needs benchmark candles — confirm SPY in `price_daily` first
   - Peer set — uses sector classification (SIC code) + market cap proximity? Or curated peer list? Backend ticket
   - Sector aggregates for heatmap — needs new aggregate views
   - Payout ratio needs FCF data joined — verify XBRL coverage
4. recharts version: pin v2.x (mature) or v3 beta? Recommend v2 for stability; revisit when v3 ships GA.
5. Are there existing charting components we should retire? `Sparkline.tsx` is hand-SVG — keep for compact use, no need to migrate. Dividend `HistoryBar` similar — keep.

## What this spec replaces

This is the parent design for chart-redesign work. Individual ticket-level specs (per drill page) get filed when each ticket starts execution — they reference back to this doc for color theme + library + L1/L2/L3 contract decisions, but spec their own data shape + chart inventory + route.

## Risk register

| Risk | Mitigation |
|---|---|
| Bundle bloat | Lazy-load recharts per drill page route via `React.lazy` + Suspense; verify bundle size doesn't grow on overview |
| Chart-library fragmentation | Stick to two libs (recharts + lightweight-charts) + hand-roll SVG for one-offs. Don't add a third unless necessary |
| Quant data gaps blocking visual work | Stub-render with placeholder data on charts that need new ingest; ship visual layer; backfill data ingest in parallel |
| Recharts SSR/hydration issues | App is client-rendered (Vite SPA), no SSR — non-issue |
| Color theme inconsistency between lightweight-charts and recharts | Single source of truth in `chartTheme.ts`; both consumers import from there |
| Operator wants more charts mid-flight | Stop after Phase 2; checkpoint before adding more. Don't try to ship all 9 tickets in one go |

## Out of scope (locked)

- RSI / MACD oscillators on chart workspace (already deferred during #576)
- Drawing tools (trendlines, fibs) on chart workspace
- Multi-timeframe split view
- 3D / scientific viz (plotly territory) — not on roadmap
- Real-time/streaming data updates — eBull is research-tier, not day-trading
- Tick-level data
- Options chain viz
- Site-wide visual polish (typography, color tokens, dark/light mode toggle) — tracked at #559

## References

- Spec for #575 (polish round 2): `docs/superpowers/specs/2026-04-27-instrument-detail-polish-round-2-design.md`
- Spec for #576 (chart workspace): chart workspace was scoped via the issue body (#576) and shipped in 4 PRs (#581, #582, #583, #584)
- Spec for #578 (dividends drill): the issue body itself
- Memory file (auto-loaded next session): `feedback_chart_redesign_handover.md` — see "Memory entry" section below

## Memory entry — content to save

When next session opens, this needs to auto-load. Save as `C:\Users\LukeB\.claude\projects\d--Repos-eBull\memory\project_chart_redesign.md`:

```markdown
---
name: Instrument-page chart redesign — quant grade L2 drill pages + theme
description: Multi-PR work tracked under <epic#>. Library = recharts + lightweight-charts. Theme = chartTheme.ts dark slate. Per-domain L2 drill pages.
type: project
---
Work scope: redesign instrument-page charts to quant-grade analytical views with per-domain L2 drill pages. Spec at docs/superpowers/specs/2026-04-27-instrument-charts-quant-redesign-design.md.

Locked decisions (do not re-debate next session):
- recharts is the chart lib for non-OHLC viz (declarative JSX, ~96KB gz, MIT, lazy-loaded per drill page)
- lightweight-charts stays for OHLC (already in)
- chartTheme.ts is the single source of truth for chart colors (dark slate palette)
- L1 / L2 / L3 drill contract continues from #576 — every domain gets a route /instrument/:symbol/<domain>
- Quant vocabulary: margins, growth, DuPont, drawdown, insider clusters, sector radar — NOT day-trader TA

Why: operator review 2026-04-27 — overview charts felt "basic Bob"; drill from sparklines/insider summary to rich analytical pages was missing. Sub-tickets were filed at end of session 2026-04-27 (see "Tickets" section in spec).

How to apply: when starting work on any chart-redesign sub-ticket, read the parent spec for color theme + library + L1/L2/L3 conventions. Each sub-ticket's spec only needs to define its own data shape + chart inventory + route, not re-litigate library/theme.
```

This memory entry tells future-me: "we made these decisions, here's the spec, don't ask 'what's recharts?' again."
