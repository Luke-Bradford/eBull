# Ownership history pane (#922)

Status: live spec for #922 (third of 4 split tickets under closed parent #846).
Scope: new `OwnershipHistoryChart` on the L2 ownership page + a small, additive
extension to the existing `/instruments/{symbol}/ownership-history` endpoint
(aggregate mode). No new endpoint, no schema change, no ingest change.

## Stale-premise resolution

The issue claims the existing endpoint "already returns the data shape" for the
default category-level view. **It does not**: `holder_id` is REQUIRED for every
holder-scoped category (`insiders`, `blockholders`, `institutions`, `def14a`) —
a deliberate guard (Codex review of #840.F) because holder-less `DISTINCT ON
(period_end, ownership_nature)` would silently return one arbitrary holder per
period. The issue itself allows "add fields if not" — the gap is closed with an
explicit `aggregate=true` mode, not by weakening the guard.

## Where the component lives

`OwnershipPage` (L2), below the existing pie + legend. The issue says "below the
ownership rollup pie"; the L2 page is where holder ROWS exist (the drill-in
acceptance — "click on Cohen's GME insider row" — needs them) and where the
`?category=` / `?filer=` selection params already live (set by wedge clicks
since #746/#921). The chart consumes that same selection state: no new selection
mechanism, no prop drilling from a second source of truth.

## Backend design

### D1 — aggregate mode is honest only where the data cadence allows it

- **institutions** (13F): quarterly by construction — every filer reports per
  quarter-end. Aggregate per `period_end` = `SUM` over the per-`(filer_cik,
  ownership_nature)` deduped winner rows (same `filed_at DESC,
  source_document_id ASC` winner rule as the per-holder reader — raw SUM would
  double-count amendments). Dev-verified on AAPL: 5,577–6,069 filers/quarter,
  sums 3.1–5.8B vs 14.9B outstanding; older quarters visibly partial (backfill
  still draining) — the chart shows what we have, faithfully.
- **treasury**: already issuer-level; aggregate == the existing series.
- **insiders / blockholders / def14a**: event-driven filings (Form 4 on trade,
  13D/G on threshold). A per-period SUM would only count holders who happened to
  file that period — misleading. Honest aggregation needs carry-forward-latest-
  per-holder semantics; **out of scope**, returns 400 with an explicit detail,
  follow-up issue filed. Drill-in (per-holder) covers these categories, which is
  also exactly what the issue's GME/Cohen acceptance exercises.
- No ETF split in v1: `ownership_institutions_observations.filer_type` is 100%
  `'INV'` on dev (ETF tagging hasn't landed in observations). When it lands, the
  split is one GROUP BY term. Documented divergence from the pie (which splits
  ETFs from the 13F **current** snapshot by `filer_type`).

### D2 — pinned aggregate SQL + provenance honesty (Codex ckpt-1 P0/P1)

Pinned shape (institutions):

```sql
SELECT period_end, SUM(shares) AS shares,
       COUNT(DISTINCT filer_cik) AS holder_count,
       MAX(filed_at) AS filed_at
FROM (
  SELECT DISTINCT ON (period_end, filer_cik)
         period_end, filer_cik, shares, filed_at
  FROM ownership_institutions_observations
  WHERE instrument_id = … AND known_to IS NULL AND shares IS NOT NULL
    AND exposure_kind = 'EQUITY' AND ownership_nature = 'economic'
    [AND period_end >= …] [AND period_end <= …]
  ORDER BY period_end, filer_cik, filed_at DESC, source_document_id ASC
) winners
GROUP BY period_end ORDER BY period_end
```

- `ownership_nature = 'economic'` is FILTERED, not summed-across: today it is
  the only nature (verified, 6.25M rows); if other natures ever land they are
  excluded rather than silently mislabelled into an "economic" total.
- Dedup-before-sum: the inner `DISTINCT ON` picks one winner per
  `(period_end, filer_cik)` so amendments cannot double-count.
- `holder_count = COUNT(DISTINCT filer_cik)` — filers, not filer-nature rows.
- **NOT claimed to reconcile with the pie** (Codex P0): the pie reads the
  `*_current` tables through cross-source survivor logic + an ETF filer_type
  split; this series is raw-13F-by-quarter. Dev-verify is a magnitude
  sanity-check only; the chart line is labelled "Institutions (13F)".

### D3 — API surface (additive only)

`GET /instruments/{symbol}/ownership-history?category=…&aggregate=true`:

- `aggregate=true` + `holder_id` → 400 (mutually exclusive).
- `aggregate=true` + category not in `{institutions, treasury}` → 400 with the
  cadence rationale in `detail`.
- Existing per-holder contract unchanged, including inclusive date bounds
  (`from_date > to_date` ⇒ empty list, same as today; test pins it).
- **Institutions** aggregate points reuse `OwnershipHistoryPointResponse`:
  `ownership_nature="economic"`, `source="13f"`, `source_accession=null` (an
  aggregate has no single accession), `filed_at=MAX(filed_at)` in the bucket.
- **Treasury** aggregate = the EXISTING issuer-level series verbatim — XBRL
  source/accession provenance untouched (Codex P0: the institutions bullet
  does not apply to treasury).
- New OPTIONAL field `holder_count: int | None` on the point response
  (institutions-aggregate only; `null` in per-holder mode and for treasury).
  Pydantic + `types.ts` updated in the same PR (api-shape-and-types skill).

## Frontend design

### D4 — selection-driven modes (Codex ckpt-1 P0: `?category=` respected)

Aggregate (no `?filer=`):

- No `?category=` → institutions + treasury lines. Two parallel fetches; either
  failing alone degrades to the other line + a per-section error note; both
  failing → `SectionError`.
- `?category=institutions` → institutions line only; `?category=treasury` →
  treasury line only.
- `?category=insiders|blockholders|def14a` → explanatory `EmptyState`: these
  filings are event-driven, only per-holder series are honest — "click a holder
  row to chart one filer".
- `?category=etfs` → same explanatory state; the 13F aggregate line includes
  ETF filers (no filer_type split in observations yet — see D1).

Per-holder (`?filer=` set) — **per-category key → `holder_id` mapping**
(verified key shapes; Codex ckpt-1 P1):

| category | leaf/row key shape | holder_id |
| --- | --- | --- |
| institutions / etfs | raw `filer_cik` | key as-is, queried as `category=institutions` |
| insiders (Form 4) | `cik` or `name:{name}` | key if all-digits, else unmappable |
| insiders (baseline) | `baseline:{cik}:d\|n` | middle segment |
| blockholders | `block:{cik\|name:…}` | suffix if all-digits, else unmappable |

- Mappable key → per-holder series for the selected category; one line per
  `ownership_nature` (service doc: beneficial and direct render as TWO lines,
  never summed).
- Unmappable key (`name:` fallback anywhere) → `EmptyState`: "No per-holder
  history: this holder has no resolved CIK."
- Stale/unknown-but-mappable key → endpoint returns an empty list →
  `EmptyState` ("no history on file for this holder").
- `?filer=` without `?category=` → filer ignored, aggregate default renders.
- `?category=treasury` + `?filer=` → issuer series unchanged (treasury is
  issuer-level).

### D5 — time windows and axis

- 1Y / 3Y / 5Y / All toggle → `from_date` (UTC today minus window). Local
  component state, not a URL param (the issue names only `holder_cik` as param
  state; windows are ephemeral view state).
- **Y axis = absolute shares, not %.** The issue's "50% → 53%" example implies a
  percent axis, but a percent of TODAY's `shares_outstanding` applied to old
  quarters misstates history (buybacks shrink the denominator — AAPL
  specifically). We have no historical outstanding series wired (#1580 notes
  stale multi-class dei rows). Tooltip shows shares + "X% of current
  outstanding" explicitly labelled "current". Conscious deviation, recorded in
  the PR; percent axis = follow-up when a historical denominator exists.

### D6 — states + conventions

Loading `SectionSkeleton`, error `SectionError onRetry`, empty `EmptyState` with
next-action copy ("Ownership history appears after the 13F backfill drains for
this instrument."), all structurally symmetric per
`loading-error-empty-states.md`. Recharts `LineChart` themed via `useChartTheme`
accents; formatting via existing `formatShares`/`formatPct`; timestamps via
`format.ts`.

### D7 — fetcher

New `frontend/src/api/ownershipHistory.ts` thin fetcher +
`OwnershipHistoryResponse` types in `types.ts` (mirrors the Pydantic model
field-for-field; `holder_count: number | null`).

## Tests

1. Backend: pure param-validation cases via existing endpoint-test pattern;
   ONE db-tier integration test for the genuinely-new aggregate SQL (dedup-
   before-sum proven with an amendment fixture: two accessions same
   filer×quarter → aggregate counts the winner once). Run targeted (`pytest
   <file> -m db`), never bare `-m db` (wedge precedent).
2. Frontend: pure series-builder (points → per-nature recharts series), mode
   decision (aggregate / holder / name-fallback), window → from_date; render
   states (loading/error/empty/data) in jsdom with mocked fetchers.
3. Dev-verify: AAPL / GME / MSFT — aggregate renders; GME insider drill via a
   known CIK; figures sanity-checked for MAGNITUDE against the pie's
   institutions slice (not exact reconciliation — different snapshot + dedup
   semantics, see D2 / Codex ckpt-1 P0).
