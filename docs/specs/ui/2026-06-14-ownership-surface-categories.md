# Ownership panel — surface buried categories (#1627)

Renderer-only round-out of the ownership panel. NO change to
`app/services/ownership_rollup.py` dedup/denominator/math. Parent epic #788.

## Problem (verified live on dev, panel AAPL/GME/MSFT/JPM/HD)

1. **def14a folded into Insiders.** `rollupToSunburstInputs` (L1) and
   `SLICE_TO_TABLE_CATEGORY` (L2) both map `def14a_unmatched → insiders`. On
   AAPL the chart "Insiders" wedge is 99.6% unmatched proxy holders (2.48B
   shares vs 10.3M real insiders). `def14a_unmatched` carries
   `denominator_basis=pie_wedge` (additive — already in server residual /
   concentration), so it is a real wedge, not a sub-set of insiders.
2. **funds overlay shows one aggregate row.** `FundsMemoOverlay` renders the
   slice total only; the 2106 per-fund series for AAPL are surfaced nowhere
   (L2 excludes funds too).
3. **blockholders** wedge code path exists but `ownership_blockholders_current`
   = 0 (13D/G drain mid-flight) → absent live. Verify-on-data.

## Invariants preserved

- `denominator_basis` is the authoritative additive-vs-overlay signal:
  `pie_wedge` → counts toward the pie + server residual; `institution_subset`
  → memo overlay, never summed into the pie. (data-engineer I4/I5.)
- Denominator stays `shares_outstanding` only (treasury additive on top).
- Wedge ↔ L2 row key parity: a chart click sets `?filer=<key>`; the L2 table
  resolves rows by the same `filer_cik ?? name:` key under the same category.
- One server snapshot drives chart + table + residual; the renderer must not
  introduce a figure the server didn't compute.

## Decisions (operator, 2026-06-14)

- **def14a → own pie wedge** (un-fold; stays additive `pie_wedge`).
- **funds → top-N fund series + aggregate** inside the overlay.

## Changes

### 1. `ownershipRings.ts`
- `SunburstHolder.category` union + `CategoryKey` union: add `"def14a"`.
- `CATEGORY_LABEL`: `def14a: "DEF 14A"`.
- `SunburstInputs`: add `def14a_total: number | null` + `def14a_as_of?: string | null`.
- `buildSunburstRings`: filter `def14a` holders; push a category via
  `buildCategoryFromTotal("def14a", input.def14a_total, def14a_holders,
  threshold, /*bypass*/ true, input.def14a_as_of ?? null)` after insiders.
  **bypass=true** (like blockholders): DEF 14A holders are 5%+ beneficial
  owners — every one surfaces as its own leaf, no "Other DEF 14A" tail, so
  wedge↔row parity holds for every L2 row (Codex ckpt-1 Medium).

### 2. `OwnershipSunburst.tsx`
- `CATEGORY_FILL_INDEX`: `def14a: 5` (accent[5] = lime-500, the last free
  accent slot; chart is the documented exception to the no-new-color rule).

### 3. `OwnershipPanel.tsx`
- `rollupToSunburstInputs`: drop the def14a→insiders fold. `insiders_total`
  / `insiders_as_of` = insiders slice only. Add `def14a_total` /
  `def14a_as_of` from `def14a_unmatched`. Push `flattenHolders(
  "def14a_unmatched", "def14a")` into `holders`.
- Table/overlay split driven off `denominator_basis`: overlay slices =
  `slices.filter(s => (s.denominator_basis ?? "pie_wedge") === "institution_subset")`;
  pie-wedge slices keep the explicit ordered list. (Correct-by-construction
  for funds + any future `institution_subset` overlay.)
- Overlay renders top-N (8) holders by shares + aggregate total + "+N more"
  when truncated. Holder selection extracted to a pure helper
  `topHoldersByShares(holders, n)` in `ownershipMetrics.ts` (table-tested).

### 4. `OwnershipPage.tsx`
- `SLICE_TO_TABLE_CATEGORY`: `def14a_unmatched: "def14a"` (was `"insiders"`).
- `CATEGORY_LABELS`: `def14a: "DEF 14A"`. `_CATEGORY_ORDER`: insert `def14a`.

### 5. Tests
- `OwnershipPanel.test.ts`: rewrite the def14a-fold block → def14a is its own
  category (`insiders_total` = insiders only; `def14a_total` separate; holder
  `category === "def14a"`).
- `OwnershipPage.test.ts`: def14a maps to `def14a` category/label; wedge↔row
  parity still holds.
- `ownershipRings.test.ts`: def14a total renders its own category.
- `OwnershipSunburst.test.tsx`: `CATEGORY_FILL_INDEX` includes def14a.
- `ownershipMetrics.test.ts`: `topHoldersByShares` ordering + truncation.

## Out of scope
- `etfs` (empty — filer-classification gap, needs `etf_filer_cik_seeds`; separate ticket).
- Any backend `denominator_basis` reclassification.

## Codex ckpt-1 — findings + resolutions (supersede above where noted)

- **High — CSV `?category=def14a` would break / drift.** `rollup_csv_slice_filter`
  (`app/api/instruments.py:4558`) mirrors the fold server-side: `"insiders" →
  {"insiders","def14a_unmatched"}`. Un-folding the FE without this re-introduces
  prevention-log #1767 in reverse (Insiders-drill CSV would carry def14a the
  table no longer shows). Resolution (ONE backend hunk — the CSV category map,
  NOT rollup math): `rollup_csv_slice_filter("insiders") → {"insiders"}`;
  `"def14a_unmatched"` is already self-addressable. FE export href maps the
  chart key `def14a → "def14a_unmatched"` for `?category=`. Update
  `tests/test_ownership_rollup_csv.py::test_rollup_csv_slice_filter_folds_def14a_into_insiders`
  → un-fold assertion.
- **High — pie_wedge slice could be dropped from the table while server residual
  counts it.** Resolution: the L1 `SliceTable` renders EVERY `pie_wedge` slice
  (ordered by the known list, any unknown pie_wedge appended) — basis-driven,
  not a hardcoded allow-list. Test: a pie_wedge slice always appears in the table.
- **Medium — generic overlay vs funds-only.** Resolution: overlay selection is
  `denominator_basis === "institution_subset"`; rendering is per-slice using the
  slice's own `label`. The 13F-double-count memo line is funds-specific (only
  `institution_subset` member today); a generic "non-additive overlay — not in
  pie/residual" line covers any future overlay.
- **Medium — bypass=false → Other-tail parity gap.** Resolution: def14a uses
  **bypass=true** (above). Parity invariant scoped to rendered leaves.
- **Medium — no residual-reconciliation test.** Resolution: add a fixture with a
  large funds slice; assert funds NEVER enters `inputs.holders` / any `*_total`
  (non-additive), and that un-folding leaves `insiders_total` + `def14a_total`
  summing to the old combined value (residual math unchanged).
- **Low — stale `SunburstInputs` denominator comment** ("callers compute
  shares_outstanding + treasury"). Resolution: fix the comment.
- **Low — duplicate holder key across categories.** Resolution: scope the L2
  `filerLabel` lookup by `key && category` so a `name:` collision across
  insiders/def14a can't pick the wrong label.

## Verification (DoD clauses 8/11/12)
- `pnpm --dir frontend typecheck` + `test:unit` green.
- Dev panel AAPL/GME/MSFT/JPM/HD: def14a renders as its own wedge (not folded);
  Insiders wedge reads honest (~0.07% AAPL); funds overlay lists top funds;
  pie + residual still reconcile (residual unchanged — additive math untouched).
- blockholders: re-check AAPL/GME after the 13D/G drain lands `>0` rows.
