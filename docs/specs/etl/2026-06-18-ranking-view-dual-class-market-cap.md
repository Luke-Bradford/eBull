# Ranking-view dual-class market cap (#1664, follow-up to #1662)

**Status:** spec → Codex ckpt-1 (resolved) → impl

## Codex ckpt-1 resolution

- **HIGH (pb/debt_equity CTE provenance) — REBUTTED with evidence.** Codex feared the
  final-SELECT blanket suppression mis-handles columns that are distorted in one CTE and clean
  in the other. Verified empirically: legacy `fs.book_value` is **per-share** (ADBE 28.87,
  COST 75.55 — not a balance-sheet total), = total_equity/combined_shares. So legacy
  `pb = price/book_value` = price×combined_shares/equity → **distorted** (same as new-pipeline);
  legacy `debt_equity = debt/(book_value×shares)` = debt/total_equity → **clean** (same as
  new-pipeline). pb distorted in BOTH, debt_equity clean in BOTH → the blanket final-SELECT
  suppress (NULL pb, KEEP debt_equity) is correct regardless of CTE origin. No provenance flag
  needed.
- **MED (overlay scope).** Scoring overlays only the two fields it consumes and can recompute
  company-wide-correctly: `market_cap_live` + `fcf_yield`. The other suppressed columns
  (`enterprise_value`, `price_sales`, `p_fcf_ratio`, `pb_ratio`, `ev_revenue`, `ev_ebitda`) stay
  NULL for view readers — scoring does not consume them and Python has no company-wide
  debt/cash/revenue/equity recompute here. Honest degrade.
- **MED (fcf numerator).** `fcf_ttm` is issuer-level (combined-company) TTM FCF — dual-class
  siblings share one CIK's fundamentals — so it is the correct numerator against the
  total-company cap. Cited in code, not rested on GOOG/GOOGL.
- **MED (partial-DB guard).** `resolve_market_cap_basis` reads `instrument_class_shares_outstanding`
  directly → wrapped in its own savepoint + `UndefinedTable` guard in scoring.
- **LOW.** Detection matches `resolve_market_cap_basis` exactly. "GOOG+GOOGL" is the current
  dev-observed instance, NOT a safety boundary — the migration + overlay are CIK-general and
  correct for all curated issuers (incl. future HEI/METC rows once they gain a quote).
**Type:** view migration (sql/201, no data backfill) + PURE READ-PATH Python overlay
**Depends on:** #1623 (`instrument_class_shares_outstanding`, sql/200), #1662 (`resolve_market_cap_basis`)

## Problem

`instrument_valuation` VIEW (sql/080) computes `market_cap_live = price × ttm.shares_outstanding`
— the **combined** all-class count × this class's price. The exact distortion #1662 retired on
the *display* path, still live on the *ranking* path. Dev, today:

| instrument | combined shares | price | market_cap_live | fcf_yield |
|---|---|---|---|---|
| GOOG (iid 1002, Class C) | 12,116M | $358.20 | **$4.340T** | 0.002331 |
| GOOGL (iid 6434, Class A) | 12,116M | $369.20 | **$4.473T** | 0.002261 |

Two caps for ONE company (correct #1662 total = **$4.4476T**, identical). Every shares-derived
column inherits the error: `market_cap_live`, `enterprise_value`, `pb_ratio` (`price×shares/equity`),
`price_sales`, `p_fcf_ratio`, `fcf_yield`, `ev_revenue`, `ev_ebitda`. Clean (per-share / ratio,
no `shares×price` term): `pe_ratio`, `roe`, `roa`, margins, `dividend_yield`, `debt_equity_ratio`
(new-pipeline = debt/equity).

**Consumers (repo-wide grep, py/ts/tsx/sql):** only `app/services/scoring.py:800` (the ranking
engine) + hypothetical operator BI/SQL (the view's own comment notes external readers). No FE/API
reader → no TS mirror. scoring SELECTs pe/pb/p_fcf/fcf_yield/debt_equity/market_cap_live/current_price
but `_value_score` consumes only `pe_ratio` (clean) + `fcf_yield` (distorted, **fallback path only**
— thesis-less instruments).

## Source rule — SETTLED, not researched

`instrument_valuation.market_cap_live` is named as the open debt in data-engineer skill **I20** /
prevention-log "Market cap of a multi-class issuer is the total company…". The rule:
`resolve_market_cap_basis` → `total_company` (Σ class×price, identical across siblings) /
`multiclass_unavailable` (curated dual-class, no clean total → **suppress null**, never publish the
broken product) / `not_multiclass` (legacy product, exact for single-class). Multi-class oracle =
presence in curated `instrument_class_shares_outstanding` (by CIK), never a noisy shared-CIK sibling
count.

## Full-population check (dev DB)

Curated set = **6 instruments / 3 issuers**: GOOG/GOOGL (CIK 1652044), HEI/HEI.A (46619),
METC/METCB (1687187). Only **GOOG + GOOGL** have an `instrument_valuation` row (HEI/METC: no quote
or no complete TTM → no row). Both are scored. So the live blast radius = GOOG + GOOGL.

## Why not compute the total in SQL (rejected option a)

The correct total needs per-class prices + untraded-residual imputation + the fail-closed guards
(`_assemble_total_company_cap`: Σ-overage ≤ combined+0.5%, residual ≤ 25%, combined-near ≤ 400d,
future-period reject, `class_shares_usable` freshness/structural). Replicating that policy in the
view duplicates the load-bearing source of truth in SQL → drift risk, forbidden by CLAUDE.md
(single source of truth, no fragile cleverness). The view's only honest move is **suppress**; the
correct number must come from the Python helper.

## Decision — hybrid (view suppress + Python overlay), strictly better than any single option

### Surface 1 — view (sql/201): suppress, fail-closed for ALL readers

Recreate `instrument_valuation`. For curated dual-class issuers (instrument's primary SEC CIK ∈
`instrument_class_shares_outstanding.source_cik`, CIK zero-padded to 10 to match #1623/#1662 storage),
NULL the eight shares-distorted columns above. Detection CTE:

```sql
dual_class AS (
    SELECT DISTINCT ei.instrument_id
    FROM external_identifiers ei
    JOIN instrument_class_shares_outstanding c
      ON c.source_cik = lpad(ei.identifier_value, 10, '0')
    WHERE ei.provider = 'sec' AND ei.identifier_type = 'cik' AND ei.is_primary = TRUE
)
```

Final SELECT wraps each distorted column `CASE WHEN v.instrument_id IN (SELECT instrument_id FROM
dual_class) THEN NULL ELSE v.<col> END`. = the `multiclass_unavailable` posture for BI + scoring
fallback. Clean columns untouched. No data backfill (view recreate).

### Surface 2 — scoring.py: overlay the correct number on the decision path (don't sacrifice)

`_value_score` consumes `fcf_yield` (now view-NULLed for dual-class). Restore it via the #1662 helper
so thesis-less dual-class instruments keep a CORRECT value signal rather than a degraded one:

- `_load_instrument_data`: add `fcf_ttm` to the valuation SELECT (clean column). After the existing
  savepoint read, call `resolve_market_cap_basis(conn, instrument_id=...)` (read-only; guard
  `UndefinedTable` for partial test DBs → treat as not_multiclass).
- New pure `_apply_market_cap_basis(valuation_row, resolution) -> valuation_row` (table-tested):
  - `total_company`: `market_cap_live = total.value`; `fcf_yield = fcf_ttm / total.value` when
    `fcf_ttm` present and `total.value > 0`, else None.
  - `multiclass_unavailable`: leave the view's NULLs (graceful degrade).
  - `not_multiclass` / no valuation_row: unchanged.

Separation of truth is intentional: the SQL view can't compute the total (no per-class price policy
in SQL) so it degrades honestly to NULL; the Python consumer upgrades to the correct figure via the
shared helper. Single source of policy = `resolve_market_cap_basis`.

## Tests

- Pure (`tests/test_scoring_market_cap_basis.py`): `_apply_market_cap_basis` — total_company recomputes
  market_cap + fcf_yield; multiclass_unavailable keeps None; not_multiclass unchanged; fcf_ttm None →
  fcf_yield None; total.value 0 → guarded.
- DB (1, `-m db`): after sql/201, GOOG + GOOGL valuation rows have NULL market_cap_live / fcf_yield /
  price_sales / pb_ratio / ev_ebitda; a single-class control (AAPL) keeps them.

## DoD / dev-verify

- Smoke panel: AAPL (single-class, unchanged), GOOG + GOOGL (view NULLs; scoring overlays $4.4476T +
  correct fcf_yield), HEI/METC (no row — unaffected).
- Cross-source: GOOG total cap $4.4476T ≈ companiesmarketcap Alphabet ~$4.42T (already #1662-verified).
- Backfill: N/A (view recreate, no rows). Migration applies on `run_migrations`.
- Operator-visible: re-score GOOG/GOOGL, confirm value-score uses the corrected figure; confirm view
  emits NULL (no two-caps lie).

## Skill / prevention

data-engineer I20: strike the "VIEW … still carries the legacy distortion → follow-up" tail; record
the ranking view is now fail-closed + scoring overlays the #1662 total. Prevention-log: "a SQL view
cannot reproduce a fail-closed multi-step policy — suppress in the view, compute the correct figure in
the Python consumer that owns the policy."
