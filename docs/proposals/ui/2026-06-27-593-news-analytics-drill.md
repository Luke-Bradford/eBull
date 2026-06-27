# #593 — News analytics drill (`/instrument/:symbol/news-analysis`)

Phase 3 of #585. FE recharts drill over the now-populated `GET /news/{instrument_id}`
(#1750 RSS provider). Sibling of the #592 filings-analytics drill — mirrors its
structure exactly (page shell, pure `lib/` transforms, theme-driven charts).

## Source rule / endpoint contract (verified 2026-06-27 vs live :8000)

`GET /news/{instrument_id}` (`app/api/news.py:89`) → `NewsListResponse`:
- Items: `{news_event_id, instrument_id, event_time (tstz ISO), source, headline,
  category, sentiment_score (signed numeric|null), importance_score, snippet, url}`.
- **Defaults to last 30 days** unless `since` passed; `limit` default 50, cap **200**;
  ordered `event_time DESC`; **404 on unknown instrument**.
- Endpoint is `instrument_id`-keyed → resolve `:symbol → id` via `fetchInstrumentSummary`
  first (same one-lifecycle pattern as `FilingsAnalyticsPage.tsx:29`).
- Sentiment is a signed numeric score (settled-decisions §News/Sentiment) — emerald >0 /
  red <0 fill is the correct encoding.

## Dev-data reality (full-population check, NOT a sample)

`news_events` on dev: 4 instruments only — **GME 20, TPR 18, BBBY 12, ATEX 2** (not AAPL).
Span **2026-06-22 → 06-27 = 1 ISO week**, **single source** "Yahoo Finance", categories
general/analyst_note/earnings. So on dev: source-pie = 1 slice, weekly-volume = 1 bar,
7d-rolling-mean ≈ raw mean. This is RSS recency, not a bug.

**Posture (mirrors the operator-mandated `dev_limited`/`sector_n` honesty for #594):** build
all three charts spec-faithful for production correctness; render honestly-sparse on dev with
a low-history caption. Do NOT dumb charts down to current dev sparsity (transient state).
Fetch window = 365d (`since`) so the trend fills as news accrues; 200-row cap is ample
(max 20 rows/instrument today). Dev fixture for verification: **GME**.

## Charts (spec §News, lines 143-147)

Pure transforms in `frontend/src/lib/newsAnalytics.ts` (table-tested, no DB/render — the
"pure policy over real DB" prevention-log lesson). All series colors from `useChartTheme()`
`theme.*`, never `lightTheme.*` (prevention-log 1917 — #591 risk-drill regression).

1. **Sentiment trend** (AreaChart) — daily mean `sentiment_score` + 7-day rolling mean.
   Bicolor-by-sign via the canonical recharts **gradient-offset** technique (NOT a pos/neg
   null-split — that gaps at zero crossings): single `<Area dataKey="rolling" baseValue={0}
   fill="url(#sentSplit)" stroke="url(#sentSplit)">` + a `<linearGradient>` with a hard stop
   at `offset = maxV/(maxV-minV)` (fraction of value-domain above 0) — emerald above the stop,
   red below. `baseValue={0}` anchors fill to the zero line; emerald fills line→0 above, red
   fills 0→line below. `YAxis domain` forced to include 0 (`[min(0,minV), max(0,maxV)]` padded)
   + `<ReferenceLine y={0}>`. Rolling mean = trailing 7-day window with **minPeriods=1**
   (partial window early, so 6-day dev data renders, not all-null). Days with no item, or items
   with null `sentiment_score`, excluded from the daily mean (no fabricated zeros).
2. **News volume** (BarChart) — count per **ISO week** (ISO week-*year* key `YYYY-Www`, UTC
   date math — not `getFullYear`/local, which mis-bucket at year/midnight boundaries),
   `theme.accent[1]`. "Spot bursts."
3. **Source breakdown** (PieChart) — count by `source` (null/blank coalesced to "Unknown"
   before grouping), `theme.accent` rotation. 1-slice on dev is honest; correct once multi-source.

### Codex ckpt-1 resolutions (2026-06-27)

1. **200-row cap vs 365d** — `fetchNews` paginates (bounded loop, `offset+items >= total`,
   hard stop ~10 pages/2000 rows) so a hot name isn't silently truncated to newest-200; caption
   if the cap is hit. 2-4. **Area baseline / null-gap / partial window** — resolved by the
   gradient-offset + `baseValue={0}` + `minPeriods=1` design above. 5. **ISO week-year + UTC** —
   pure helper keys on ISO week-year in UTC. 6. **Nullable source** — coalesced to "Unknown".

Chart components in `frontend/src/components/news/newsAnalyticsCharts.tsx`; each carries its
own empty guard (mirror `NoFilings`). Custom tooltips wrap `<ChartTooltip>`.

## Page + wiring

- `frontend/src/pages/NewsAnalysisPage.tsx` — mirror `FilingsAnalyticsPage`: `useParams`,
  back-link, header + subtitle, `useAsync` (symbol→id→news), loading/error/empty states,
  three `<Section>` wrappers. Low-history caption when span < 2 weeks or sources < 2.
- `frontend/src/App.tsx` — static import + route `instrument/:symbol/news-analysis`
  (drills are static-imported in this codebase, not lazy — spec's lazy note is stale).
- `frontend/src/api/news.ts` — add optional `since` to `fetchNews` (back-compat default).
- Header "Analytics →" link from the existing news pane (`RecentNewsPane`) to the drill.
  Existing pane stays (L1 unchanged).
- `frontend/src/lib/newsAnalytics.test.ts` — unit-test the 3 transforms (fast `test:unit`).

## Out of scope

Multi-source ingest (data-layer), importance-weighted sentiment, per-category split. No
backend change. No daemon restart (FE-only).

## Gates

`pnpm --dir frontend typecheck && test:unit && dark:check`. Codex ckpt-1 (this spec) +
ckpt-2 (staged diff pre-push). In-browser render verify on GME via chrome-devtools.
