# #1751 — peer-comparison data layer (unblocks #594)

## Problem

#594 (peer-comparison radar + sector heatmap) needs, per instrument: (a) a
peer set, (b) sector-median aggregates across the radar factors (P/E, ROE,
revenue growth YoY, operating margin, debt/equity). The issue framed this as
an *ingest* gap ("no peer-set table, no sector medians").

## Premise falsified (full-population, dev DB)

It is **not an ingest gap** — the inputs already exist:

- `instruments.sector` populated 9256/12565 (custom int code 1–9; **no lookup
  table** — `resolve_sector_spdr` #1634 maps code→SPDR). `instrument_sec_profile.sic` present.
- `financial_periods_ttm`: **4062** `is_complete_ttm=TRUE` rows; per-sector
  computable (revenue>0 AND equity>0) is in the **hundreds** (sector 3:747,
  4:380, 6:325, 8:320, 7:252, 1:117, 9:109, 5:62; sector 2 thin:7).
- `financial_periods` FY revenue → YoY computable for **2600** instruments
  (per-sector hundreds).

So sector medians + peer sets are a **server-side derivation** from existing
data. The pre-existing `instrument_valuation` view only emits ~32 rows because
of its live-price join — bypassed here by reading `financial_periods_ttm`
directly for the price-free factors.

## Source rule (factor formulas — MIRROR `instrument_valuation`, sql/201)

Do not re-derive; mirror the settled per-instrument formulas (all guard denom>0):

- `roe = net_income_ttm / shareholders_equity` (when `shareholders_equity > 0`)
- `operating_margin = operating_income_ttm / revenue_ttm` (when `revenue_ttm > 0`)
- `debt_equity_ratio = (COALESCE(long_term_debt,0) + COALESCE(short_term_debt,0)) / shareholders_equity`
  (when `shareholders_equity > 0`) — **COALESCE both debt legs** (Codex: view
  does this; a NULL leg must not NULL the ratio).
- `net_margin = net_income_ttm / revenue_ttm` (when `revenue_ttm > 0`; extra)
- `pe_ratio = price / eps_diluted_ttm` — **price-gated** → only ~32 on dev.
  Marked `dev_limited` so the FE can grey it out; sector median computed from
  `instrument_valuation` (thin on dev, correct in prod).
- `revenue_growth_yoy` — two most recent FY rows per instrument from
  `financial_periods` WHERE `period_type='FY' AND revenue > 0 AND superseded_at
  IS NULL AND normalization_status='normalized'` (Codex: mirror the
  `financial_periods_ttm` canonical filter — sql/032:218 — so a superseded/raw
  FY row cannot win the pair); `(cur-prev)/prev`. **Consecutive-year guard**
  (real-data catch: GME has only FY2025 + FY2020): require
  `cur.period_end_date - prev.period_end_date BETWEEN 300 AND 430` days, else NULL.

## Design

### Service — `app/services/peer_comparison.py`

`compute_peer_comparison(conn, instrument_id) -> PeerComparison | None`:

1. Resolve the instrument's `sector` (TEXT column — code "1".."9"). If NULL →
   return `None` (caller 404s "no sector classification").
2. **Self must have a complete-TTM row** with `total_assets > 0` (needed both as
   the factor anchor and the peer-proximity reference). If absent → return
   `None` (404 "no fundamentals"). The legacy `fundamentals_snapshot` valuation
   path is intentionally ignored — the radar requires the TTM factor set.
3. **Per-instrument factor CTE** (`instrument_factors`): from
   `financial_periods_ttm` WHERE `is_complete_ttm`, compute roe / operating_margin /
   debt_equity_ratio / net_margin (price-free) per the formulas above. LEFT JOIN
   a `revenue_growth_yoy` CTE (the FY-pair calc with the canonical filter +
   day-gap guard). LEFT JOIN `instrument_valuation` for `pe_ratio` (price-gated).
4. **Sector medians**: `percentile_cont(0.5) WITHIN GROUP (ORDER BY <factor>)`
   (ordered-set agg skips NULLs) over `instrument_factors` joined to
   `instruments.sector`, GROUP BY sector; plus `count(*) FILTER (WHERE <factor>
   IS NOT NULL)` per factor for `sector_n` (a thin/empty sector then reads
   median=NULL, sector_n=0). P/E median computed separately over the priced set.
5. **Peer set**: same `sector`, exclude self, require peer `total_assets > 0`,
   rank by **size proximity** `ABS(ln(peer.total_assets) - ln(self.total_assets))`
   (keyless, broad — market cap is price-gated), nearest `_PEER_LIMIT=8`. Each
   peer carries its factor row (for the #594 heatmap). Self `total_assets > 0` is
   already guaranteed by step 2, so `ln(self...)` is safe.
6. Assemble the instrument's own factor values + sector medians + peers.

All pure SQL + a thin assembler; no external calls, no new dependency.

### Endpoint — `app/api/instruments.py`

`GET /instruments/{symbol}/peer-comparison` → `PeerComparison` (Pydantic),
mirroring the sibling symbol-keyed endpoints (resolve symbol→instrument_id,
`conn = Depends(get_conn)`, 404 when no sector / no fundamentals).

Response model:

```
PeerFactor:      key, label, instrument_value: float|None, sector_median: float|None,
                 sector_n: int, dev_limited: bool, better_when: "higher"|"lower"
PeerInstrument:  instrument_id, symbol, company_name, size_proxy: float|None,
                 factors: dict[str, float|None]   # the 6 factor keys
PeerComparison:  symbol, instrument_id, sector: str, sector_member_count: int,
                 factors: list[PeerFactor], peers: list[PeerInstrument]
                 # sector is the raw code string ("1".."9"); instruments.sector
                 # is TEXT (sql/001:7). No int cast — no lookup table exists.
```

Factor set (radar order, #594): pe_ratio, roe, revenue_growth_yoy,
operating_margin, debt_equity_ratio (+ net_margin). `better_when`:
roe/operating_margin/net_margin/revenue_growth_yoy = higher; debt_equity_ratio
= lower; pe_ratio = lower (cheaper).

### Scope boundary

- **In**: factors + sector medians + peer set (data for radar + heatmap).
- **Out**: #594.2 peer-return scatter — that needs `price_daily` return series;
  the endpoint returns peer `instrument_id`s so the FE fetches returns via the
  existing candles/returns path. No new price work here.
- **Out**: the #594 FE itself (separate ticket, unblocked by this).

## Tests (pure-logic where possible)

- `revenue_growth_yoy` day-gap guard: consecutive FY → value; 5-year-gap (GME
  shape) → NULL. (DB-backed, one fixture.)
- factor formula parity vs `instrument_valuation` for a priced instrument.
- peer selection: same-sector only, self excluded, ordered by size proximity,
  capped at 8.
- endpoint: 200 shape for a covered instrument; 404 for no-sector.

## Dev-verify

- `GET /instruments/GME/peer-comparison` → 200; sector medians non-null for the
  price-free factors; `pe_ratio.dev_limited=true`; peers populated; assert the
  instrument's roe matches `instrument_valuation.roe` (if priced) / the manual
  TTM calc.
- Spot a second sector (e.g. a sector-3 name) for non-empty peers.

## Files

- NEW `app/services/peer_comparison.py`
- `app/api/instruments.py` (+ endpoint + Pydantic models, or a models module)
- NEW `tests/test_peer_comparison.py`
