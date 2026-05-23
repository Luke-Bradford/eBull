# Per-stock research page — layout + link surfaces

Author: @Luke-Bradford
Date: 2026-04-20
Status: DRAFT (round 2 — Codex review applied)

## 1. Problem

Per-instrument data lives across seven places but there is no single
page that tells the operator "is this stock worth owning and why?"

Current surfaces that touch a single instrument:

| Surface | Path | What it shows |
|---|---|---|
| InstrumentPage | `/instrument/:symbol` | Summary + Financials + News + Filings + Positions + Thesis tabs |
| InstrumentDetailPage | `/instruments/:instrumentId` | Admin / coverage-focused detail |
| PositionDetailPage | `/portfolio/:instrumentId` | Position trades, P&L, broker-native currency |
| Dashboard positions | `/` | Row click → `/portfolio/:id` (position detail) |
| Portfolio table | `/portfolio` | Row click → `/portfolio/:id` (position detail) |
| Rankings table | `/rankings` | Row click → `/instruments/:id` (admin detail) |
| Reports page | `/reports` | Top-level snapshot JSON, no per-stock slice |

Problems:
1. **Entry confusion** — rankings drill to one page, portfolio drills
   to another. Same instrument, two views.
2. **Overlap** — InstrumentPage and InstrumentDetailPage duplicate
   identity + quote data.
3. **No research landing** — operator looking at a ranking row sees
   score breakdown but no way to jump to "why is this company good
   long-term" without navigating elsewhere.
4. **Reports page** — weekly/monthly `report_snapshots` exist but the
   per-stock contribution is buried in JSON; operator can't drill
   from a stock in a report to its research page.

## 2. Goal

One canonical per-stock research page at `/instrument/:symbol`
(existing route), redesigned as the single drill-in target for every
per-instrument surface in the app. Admin-flavoured
`InstrumentDetailPage` at `/instruments/:instrumentId` retires *after*
its coverage surface content is folded into the admin coverage page.

Operator entering from any surface lands on the same page and sees:
- Identity + live price + live key stats (with field_source badges)
- Thesis + score with reasoning
- Financials (SEC XBRL preferred; yfinance fallback)
- Recent filings (SEC 10-K/Q/8-K + CH for UK)
- News (sentiment-tagged)
- Position context (if held)
- Copy-trader exposure (if parent-traders hold it) — see §5 Slice 6 for scope
- Peer / sector comparison

### Canonical key — symbol vs instrument_id

`instruments.symbol` is indexed but **not unique** — exchange-specific
duplicates exist (e.g. `VOD` on NMS vs `VOD.L` on LSE — distinct
instruments). Existing symbol lookups use `LIMIT 1` and win
nondeterministically on collisions.

Decision: URL remains `/instrument/:symbol` (operator memorability),
but the route handler resolves symbol → `instrument_id` via a
deterministic tiebreaker:
1. `is_primary_listing = TRUE` on the instrument row (or equivalent
   flag — check current schema; else add column in pre-req migration).
2. Failing that, prefer the US listing (sector-neutral default).
3. Failing that, `ORDER BY instrument_id ASC LIMIT 1`.

When ambiguity is detected (>1 row pre-tiebreaker), include a
disambiguation chip on the page: `VOD · LSE — Vodafone Group PLC — also
on NMS →`. The alternate listing chip links to
`/instrument/{symbol}?id={instrument_id}` — the canonical research
URL with an id override that pins the page to a specific
instrument_id when the symbol alone is ambiguous. The research page
handler:
1. If `?id=` present → load that instrument_id (verify its symbol
   matches the path).
2. Else → resolve symbol via primary-listing tiebreaker.

This avoids introducing a second public URL and makes the id route
retirement in Slice 5 safe — disambiguation remains possible under
`/instrument/:symbol` permanently.

Pre-requisite ticket: confirm / add `is_primary_listing` flag on
`instruments`; expose in `/instruments/{symbol}/summary`. Issue filed
as part of Slice 0 (§5).

## 3. Proposed layout

```
╭─ SummaryStrip ────────────────────────────────────────────╮
│ AAPL · Apple Inc.   [EQUITY]   $200.50 +1.50 (+0.75%)     │
│ NMS · Technology · Consumer Electronics · $3.0T mkt cap   │
│                                                           │
│ [Thesis: BUY 72%]  [Score: 8.4 · rank 12]  [Held: 2u]     │
│ [Add]  [Close]  [Generate thesis]                         │
╰───────────────────────────────────────────────────────────╯

╭─ Left column (8/12) ──────────────────╮ ╭─ Right (4/12) ─╮
│ Tabs:                                  │ │ Recent filings │
│   [Research] [Financials] [News]       │ │   • 10-Q 04/20 │
│   [Filings] [Positions] [Thesis]       │ │   • 8-K 04/15  │
│                                        │ │                │
│ Default = Research:                    │ │ Peer snapshot  │
│   • Thesis memo (markdown)             │ │   MSFT 8.1 →   │
│   • Break conditions                   │ │   GOOG 7.9 →   │
│   • Score breakdown (5 factors)        │ │                │
│   • Key stats with field_source badges │ │ Recent news    │
│   • Red flags / risks                  │ │   • 1h ago ... │
╰────────────────────────────────────────╯ ╰────────────────╯
```

Principles:
- Identity + price + action surface always visible (sticky strip).
- Tabs drive the main column; right rail always shows recent filings +
  peer snapshot + news.
- Actions (Add / Close / Generate thesis) live in the strip, not in a
  modal footer, so they're reachable from every tab.
- Action visibility gated: Close only if holding; Generate thesis only
  if no thesis or thesis > 30d old.

### Tab contents + confirmed endpoints

| Tab | Data | Endpoints (verified against app/api) |
|---|---|---|
| Research (default) | Thesis memo, break conditions, score breakdown, key stats w/ field_source, red flags | `GET /instruments/{symbol}/summary` (existing), `GET /theses/{instrument_id}` (existing), `GET /rankings/history/{instrument_id}` (existing) |
| Financials | Income / Balance / Cashflow, QvA switch | `GET /instruments/{symbol}/financials` (existing) |
| News | Headlines + sentiment + importance, 30d window | `GET /news/{instrument_id}` (existing) |
| Filings | Full filings list with filter by type | `GET /filings/{instrument_id}` (existing) |
| Positions | Trades for held instruments | `GET /portfolio/instruments/{instrument_id}` (existing) |
| Thesis history | All thesis versions + critic feedback | `GET /theses/{instrument_id}/history` (existing) |

Empty states:
- No thesis → CTA "Generate thesis" button calls `POST /instruments/{symbol}/thesis` (existing).
- No filings → "No filings ingested yet — is this a US or UK ticker?"
  (link to admin coverage).
- Positions tab **always visible** — if not held, renders a "No open
  position" empty state with a CTA to open one via the Add button in
  the strip. Resolves the §4 redirect conflict for closed positions.

### Peer snapshot (existing rankings endpoint)

Top-5 **ranked** peers in the same sector, from the Tier 1/2 universe.
Each row: symbol, **rank**, total_score.

Current `/rankings` endpoint DOES accept `sector=<str>` filter
(`app/api/scores.py:175`, sent from `frontend/src/api/rankings.ts:35`)
and returns rank + total_score + identity. Peer snapshot can render
from those fields alone without any backend change.

Price and market_cap are NOT on `/rankings` rows and are out of
scope for v1. Operator clicks through to each peer's research page
for price detail. Design sorts by our rank, not Yahoo's.

## 4. Link surfaces — full consolidation list

Every per-instrument row drills into `/instrument/:symbol`.
Consolidation (verified via grep `to=\`/instrument` + `to=\`/instruments/${`):

| Page / component | Current drill | New drill |
|---|---|---|
| `RankingsTable` (`components/rankings/RankingsTable.tsx:203`) | `/instruments/:instrumentId` | `/instrument/:symbol` |
| `RecentRecommendations` (`components/dashboard/RecentRecommendations.tsx:39`) | `/instruments/:instrumentId` | `/instrument/:symbol` |
| `InstrumentsPage` row | `/instruments/:instrumentId` | `/instrument/:symbol` |
| `RecommendationsTable` (`components/recommendations/RecommendationsTable.tsx:98`) | `/instruments/:instrumentId` | `/instrument/:symbol` |
| `WatchlistPanel` (dashboard) | `/instrument/:symbol` | unchanged |
| `PositionsTable` (dashboard) | `/instrument/:symbol` | unchanged |
| `PortfolioTable` positions (after #324) | `/portfolio/:instrumentId` | `/instrument/:symbol?tab=positions` |
| Portfolio mirrors | `/copy-trading/:mirrorId` | unchanged (mirror ≠ instrument) |
| Audit drill | `/instruments/:instrumentId` (if present) | `/instrument/:symbol` |
| Reports per-contributor (new in Slice 4) | — | `/instrument/:symbol` |

Note: `components/portfolio/DetailPanel.tsx` (links at :128 and :370)
is deleted in PR #374 (revert of #314) — no migration needed.

Slice 3 is expected to grep `/instruments/\${` and
`/instruments/\{` across `frontend/src` to catch anything the list
above missed; the full enumeration is the grep result, not this list.

### Route redirects during transition

Old routes kept as **one-version shims** that redirect:
- `/instruments/:instrumentId` → `/instrument/:symbol` (resolve symbol via DB lookup by id)
- `/portfolio/:instrumentId` → `/instrument/:symbol?tab=positions`

Shims live for one release; delete after operator bookmarks migrate.

## 5. Implementation slices

Each slice = one PR, sequential to avoid conflicts. Each slice is
shippable.

### Slice 0 — Pre-reqs

- Confirm `is_primary_listing` flag (or equivalent) on `instruments`;
  add migration if absent.
- Verify `/instruments/{symbol}/summary` resolves symbol → primary
  instrument_id deterministically; adjust lookup in `app/api/instruments.py`
  if current `LIMIT 1` is nondeterministic.

`/rankings?sector=` already exists (`app/api/scores.py:175`,
`frontend/src/api/rankings.ts:35`) — no endpoint work needed for the
peer snapshot.

### Slice 1 — Research tab + SummaryStrip restructure

- `SummaryStrip` component with identity + price + thesis + score +
  held badge + action buttons (Add / Close / Generate thesis).
- New `Research` tab becomes the default view, composing thesis memo +
  score breakdown + key stats + red flags from existing endpoints.
- Existing `Summary` tab removed (merged into Research).

**Touches**: `InstrumentPage.tsx`, new
`components/instrument/SummaryStrip.tsx`, new
`components/instrument/ResearchTab.tsx`.

**Tests**: tab switching, action button gating (Close only if holding,
Generate thesis only if thesis missing or >30d old), sticky strip
rendering, disambiguation chip appears when multiple listings exist.

### Slice 2 — Peer snapshot right rail

- `RightRail` component with Recent filings + Peer snapshot + Recent
  news (last 3).
- Uses the existing `/rankings?sector=<str>&limit=5` endpoint; no
  backend change required.

### Slice 3 — Consolidate drill-in routes

- Rankings, RecentRecommendations, RecommendationsTable,
  InstrumentsPage rows → `/instrument/:symbol`. Enumerate via
  `grep -rn '/instruments/\${\|/instruments/{' frontend/src` to catch
  anything this spec missed.
- Portfolio + Dashboard position row drill-ins →
  `/instrument/:symbol?tab=positions`.
- `InstrumentPage` reads `tab` via `useSearchParams` from
  `react-router-dom` and uses it as the initial tab selection;
  tab-change updates the query param in place (replace, not push, so
  tab navigation doesn't spam browser history). Current page defaults
  to overview state (`InstrumentPage.tsx:602`) — replace with URL-driven
  state.
- Route shims: `/instruments/:id` and `/portfolio/:id` redirect to new
  path.
- `Positions` tab handles not-held state so the redirect is safe when
  the instrument is no longer held.

### Slice 4 — Reports page per-contributor drill-in

- **Pre-req**: define report `contributors` shape on the backend.
  Current `report_snapshots` stores current-state open-position P&L
  (weekly/monthly snapshots) — not period-contribution deltas. Need:
  - Server-side job: compute per-instrument period contribution when
    generating each snapshot (diff against prior snapshot). Store in
    `snapshot_json.contributors` as
    `[{instrument_id, symbol, pnl_gbp, pnl_pct}]`.
  - Backfill one historical snapshot with zero contributors for
    pre-feature snapshots so the UI gracefully degrades.
- UI: "Top contributors" + "Top drags" lists, each row links to
  `/instrument/:symbol`.

### Slice 5 — Retire InstrumentDetailPage + admin surface migration

- **Pre-req**: move any coverage-focused content from
  `InstrumentDetailPage` into the admin coverage page at
  `/admin/coverage/insufficient`. Pre-req MUST land before slice 5's
  redirect. Issue filed separately.
- Redirect `/instruments/:instrumentId` shim removed; 410/404 on unknown
  instrument_id paths.
- Delete `InstrumentDetailPage.tsx` + `PositionDetailPage.tsx` + their
  tests.

### Slice 6 — Copy-trader exposure (deferred but in scope for page completeness)

- Panel in the SummaryStrip or right rail showing "Held by copy
  traders" when any parent trader the operator mirrors holds the
  instrument. Endpoint: likely new `/instruments/{symbol}/copy-exposure`
  aggregating across `/portfolio/copy-trading` data. Defer detailed
  design to its own spec if scope grows.

## 6. Risks

- **Query blowup**: Research tab fires 4+ queries on mount (summary,
  thesis, scores, filings, news). Start with 4 parallel `useAsync`
  calls; add an aggregator `/research/:symbol` endpoint if p95 latency
  regresses vs current InstrumentPage.
- **Symbol collision**: primary-listing resolution MUST be
  deterministic before Slice 1 ships. Test with `VOD.L` vs any US
  symbol sharing a prefix.
- **URL churn**: route shims protect bookmarks for one version. After
  that, cold bookmark = 404 or redirect to `/instruments` list.
- **Reports contributor shape**: current snapshots don't carry
  period-contribution data. Slice 4 is gated on that backend work.

## 7. Out of scope

- Chart component (candle chart). Will land with #316 Instrument terminal.
- Keyboard shortcuts beyond current tabs. Revisit after chart lands.
- Mobile layout. Desktop-first for v1 per operator posture.
- Copy-trader exposure beyond a placeholder panel (Slice 6 is a
  scaffolding ticket; deep design lives elsewhere).

## 8. Open questions

1. Does the `instruments` table already have `is_primary_listing` or
   equivalent? Grep first; add migration if not.

## 9. Definition of done

- [ ] Slice 0 — pre-reqs: primary-listing flag confirmed/added; symbol-resolution tiebreaker deterministic
- [ ] Slice 1 — SummaryStrip + Research tab landed, default view
- [ ] Slice 2 — right rail populated (filings + peer + news)
- [ ] Slice 3 — all per-instrument drill-ins land on `/instrument/:symbol`, shims in place
- [ ] Slice 4 — Reports page contributors ship (backend + UI)
- [ ] Slice 5 — admin coverage surface absorbs retired page content, `InstrumentDetailPage` + `PositionDetailPage` deleted
- [ ] Slice 6 — copy-trader exposure placeholder in place
- [ ] Skills cross-referenced: `frontend/operator-ui-conventions.md` for
      color + density, `frontend/async-data-loading.md` for query
      parallelism, `ui-ux-pro-max` for layout + typography
