# #594 — Peer-comparison drill (`/instrument/:symbol/peers`)

Phase 3 of #585. FE recharts drill over the live `GET /instruments/{symbol}/peer-comparison`
(#1751 data layer) + the existing candles path. Sibling of the #592/#593 drills — mirrors
their shell, pure `lib/` transforms, theme-driven charts.

## Source rule / endpoint contract (verified 2026-06-27 vs live :8000, AAPL)

`GET /instruments/{symbol}/peer-comparison` → `PeerComparison` (`app/api/instruments.py:299`):
- `symbol, instrument_id, sector (raw code "1".."9", TEXT — no name lookup table),
  sector_member_count`.
- `factors: PeerFactor[]` — `{key, label, instrument_value|null, sector_median|null,
  sector_n, dev_limited, better_when: "higher"|"lower"}`. **6 factors**: pe_ratio
  (dev_limited=true, n=2, lower), roe (n=813, higher), revenue_growth_yoy (n=39, higher),
  operating_margin (n=792, higher), debt_equity_ratio (n=843, lower), net_margin (n=824,
  higher). **`better_when` is read from the API — never hardcoded.**
- `peers: PeerInstrument[]` (8 for AAPL) — `{instrument_id, symbol, company_name|null,
  size_proxy|null, factors: Record<key, number|null>}`.

`GET /instruments/{symbol}/candles?range=1w|1m|3m|6m|ytd|1y|5y|max` → `InstrumentCandles`
(`{symbol, range, days|null, rows: CandleBar[]}`, `CandleBar.close: string|null`). **Keyed by
symbol; OHLCV values are STRINGS** — parse to float; skip null/unparseable closes.

**No FE type/fetcher exists** for peer-comparison → add `PeerFactor`/`PeerInstrument`/
`PeerComparison` to `types.ts` (mirror the Pydantic above) + `fetchPeerComparison(symbol)` to
`api/instruments.ts`. (#592/#593 frontend-skill rule: `types.ts` mirrors the `response_model`.)

## Settled-decision preservation

Scoring bans **cohort-relative normalization** (`docs/settled-decisions.md` §Scoring model
style). The radar/heatmap below normalize factors **cohort-relative for DISPLAY only** — this
is an evidence/display layer (exactly like `instrument_risk_metrics` is "DISPLAY/EVIDENCE, NOT
a scoring input"), it never feeds `scores`/ranking. The ban is scoping the scoring model, not
viz. No backend change, no scoring path touched.

## Dev-data reality (full-population check)

Sector "3" is a broad SIC division (951 members); on dev the medians are noisy (ROE median
0.78% vs AAPL 89%) and AAPL's "peers" by size-proxy are F/GM/CMCSA/TSLA/PG/PEP/KO/COST — odd,
but that is the #1751 data layer's call, NOT this FE drill's bug. `pe_ratio` is dev_limited
(n=2); `revenue_growth_yoy` is low-n (39 vs ~800). AAPL's `revenue_growth_yoy` instrument_value
is **null** (missing radar axis point).

**Posture (operator-mandated honesty):** render the real charts; **grey/flag `dev_limited`
factors**, **surface `sector_n`** (low-n medians are noisy), and show a sector-breadth note.
Don't fix the data here. Dev fixture: AAPL.

## Charts (spec §Peer comparison, lines 149-153)

Pure transforms in `frontend/src/lib/peerComparison.ts` (table-tested). All colors from
`useChartTheme()` `theme.*` (never `lightTheme.*`).

### 1. Multi-factor radar — instrument vs sector median, 2 overlays

Factors have wildly different scales (P/E ~40 vs ROE ~0.9 vs margins ~0.3) → a raw radar is
meaningless. **Normalize per factor to [0,1] across the visible cohort, oriented so outward =
better:**
- cohort per factor = non-null values among `{instrument_value, sector_median, each peer's
  factor value}`.
- `norm = (v - lo) / (hi - lo)` where lo/hi = min/max of that cohort; **degenerate `hi==lo`
  → 0.5** (neutral, no spurious spread).
- orient by `better_when`: `better_when==="lower"` → `score = 1 - norm` (so outward = better).
- plot two `<Radar>` overlays: instrument (accent[1]) + sector median (accent[2], dashed).
- `instrument_value === null` → instrument point is **null** (recharts gaps it); **likewise
  `sector_median === null` → median point null** (median is nullable too — Codex ckpt-1).
  `dev_limited` axis label greyed + flagged.
- Tooltip shows the **raw** value + sector_n + better_when (normalized score is for layout
  only, never shown as the "number").
- Caveat (documented, not a bug): min-max is outlier-sensitive on a ~10-point cohort; this is
  a relative-position radar, not an absolute scale. Acceptable for "where does it sit vs peers".

### 2. Sector heatmap — instrument + peers × factors, colored by relative rank

Hand-rolled CSS grid (mirror `filingsAnalyticsCharts.FilingHeatmapChart`, NOT recharts). Rows =
instrument (pinned top, labeled) + 8 peers; cols = 6 factors. Each **cell** colored by the
value's normalized position **within its factor column** (same per-factor cohort + `better_when`
orientation as the radar) interpolated `theme.down` (red, worst) → `theme.up` (emerald, best);
null cell = neutral/empty. `dev_limited` column header greyed. "Spot outliers."

### 3. Peer return scatter — instrument vs median-peer same-day return

Fetch the instrument's + each peer's candles (`range=6m`, parallel `Promise.allSettled` — a
failed peer fetch drops that peer, never the page). Build per-symbol `date → close` (parsed
float, null/unparseable skipped). **Iterate the instrument's own consecutive candle pairs
`(prev, d)`**: `instrument_ret = close_inst(d)/close_inst(prev) - 1`. For each peer, include its
return at `d` **only if the peer has closes at BOTH `prev` and `d`** (same interval — Codex
ckpt-1: a peer that skipped `prev` would yield a multi-day return masquerading as a 1-day one);
`peer_ret = close_peer(d)/close_peer(prev) - 1`. Take the **median** across qualifying peers.
Plot `(instrument_ret_x, median_peer_ret_y)` + a `y = x` `<ReferenceLine>` (diagonal).

**Interpretation is same-day relative performance, NOT temporal lead/lag** (Codex ckpt-1): a
point **below** the diagonal (`y < x` → instrument_ret > peer_median) = the instrument
**outperformed** the sector that day; above = underperformed. Label/caption say
"outperformed/underperformed", never "led/lagged". Needs ≥ a handful of aligned days or an
empty hint. `prev` must be the instrument's immediately-preceding candle row (gap-tolerant on
the instrument side is fine — the peer same-interval guard is what enforces comparability).

## Page + wiring

- `frontend/src/pages/PeersPage.tsx` — mirror `FilingsAnalyticsPage`/`NewsAnalysisPage`:
  `useParams`, back-link, header + subtitle, loading/error/empty states, three `<Section>`s,
  sector-breadth + dev-limited caption. **One `useAsync` keyed on `[symbol]`** (Codex ckpt-1 #4
  — a second useAsync keyed on peer symbols would render stale instrument scatter when two
  instruments share a peer list): it fetches peer-comparison, then `Promise.allSettled`s the
  instrument + peer candles in the same lifecycle and returns `{ pc, candlesBySymbol }`. One
  source → one error surface (peer-comparison drives the error; candle failures degrade the
  scatter to empty, never error the page). The radar/heatmap need only `pc`; the scatter reads
  `candlesBySymbol`.
- `frontend/src/App.tsx` — static import + route `instrument/:symbol/peers`.
- `frontend/src/api/instruments.ts` + `types.ts` — `fetchPeerComparison` + the 3 types.
- Entry-point link to the drill from an existing instrument-page surface (peer/valuation pane
  if one exists; else a header link — grep at build).
- `frontend/src/lib/peerComparison.test.ts` — unit-test normalization (orientation, degenerate,
  null handling), heatmap ranking, return-alignment + median.

## Out of scope

Sector name lookup (raw code only — data layer); peer-set curation (#1751); benchmark scatter
(#591 risk drill owns SPY beta). No backend change. No daemon restart (FE-only).

## Gates

`pnpm --dir frontend typecheck && test:unit && dark:check`. Codex ckpt-1 (this spec — the
normalization is the key correctness risk) + ckpt-2 (staged diff). In-browser render blocked by
cookie auth (as #593); covered by render tests + typecheck.
