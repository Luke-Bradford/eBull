# Per-class total-company market cap (#1662, #1623 scope item 2)

**Status:** spec → Codex ckpt-1 → impl
**Type:** PURE READ-PATH (no migration / ingest / backfill / sec_rebuild / jobs-restart)
**Depends on:** #1623 (`instrument_class_shares_outstanding`, sql/200, FSDS per-class shares — shipped 8f52bfad)

## Problem

`compute_market_cap` (`app/services/xbrl_derived_stats.py`) = `instrument_share_count_latest.latest_shares`
(the **combined** all-class count) × this instrument's quote. For dual-class siblings sharing
one issuer CIK this is structurally wrong:

| instrument | combined shares | this price | market_cap today |
|---|---|---|---|
| GOOGL (iid 6434, Class A) | 12,116M | $369.20 | **$4.474T** |
| GOOG (iid 1002, Class C) | 12,116M | $358.20 | **$4.340T** |

Two different "market caps" for ONE company. Both multiply ALL 12,116M shares by ONE class's
price. The error is small when class prices are close (~1.5% here) but grows when they diverge
(UAA/UA, NWS/NWSA, FOX/FOXA can run 5–15% apart) and the two-numbers-for-one-company defect is
unconditional.

## Premise check (issue body said "per-class shares × per-class price, ~2× overstated")

**FALSIFIED.** Web + 3-lens committee (metrics-analyst / valuation-analyst / data-engineer)
unanimous: the per-class **float value** (Class A shares × price = ~$2.16T) is a *different*
metric, never labelled "market cap." The conventional "Market Cap" stat = **TOTAL company
capitalization** = Σ over all classes (class_shares × class_price), identical across share-class
siblings (companiesmarketcap quotes Alphabet ONE $4.424T total for both GOOG and GOOGL). The
"2× overstated" reading only holds against the per-class-float interpretation, which is not what
the field means.

## Decision

1. **market_cap → total company cap** = Σ_class (class_shares × class_price) + residual untraded
   class imputed at the representative traded price. Identical across siblings.
2. **dilution_summary → NO change.** `net_dilution_pct_yoy` is a growth-RATE of the *total* share
   count + TTM issuance/buyback flows — a company-level capital-structure signal. Per-class would
   shrink coverage (FSDS has a handful of rows) and add noise for zero analytical gain. Document
   the deliberate no-op; do not touch sql/052.
3. **Out of scope (follow-ups filed):**
   - `instrument_valuation.market_cap_live` (sql/080 view) = `price × ttm.shares_outstanding` —
     SAME dual-class bug, feeds the **ranking engine** (`scoring.py:800`) + P/S, P/FCF, EV/EBITDA.
     SQL-view, decision-grade; a distinct harder change. → follow-up ticket.
   - per-class **float value** as a separately-labelled stat → follow-up ticket.

## Algorithm — `resolve_market_cap_basis(conn, *, instrument_id) -> MarketCapResolution`

Returns `basis ∈ {total_company, multiclass_unavailable, not_multiclass}` (+ the total when
computable). **Multi-class detection = presence in the #1623 curated FSDS table
`instrument_class_shares_outstanding` (keyed by issuer CIK), NOT a raw shared-CIK sibling count.**
A sibling-count oracle is noise: on dev, 56 CIKs / 122 instruments have ≥2 tradable co-CIK
siblings, but only **3** are genuine curated dual-class equity (GOOG/GOOGL, HEI/HEI.A, METC/METCB)
— the rest are `.US` dual-listings, ETF trust families (BOIL/KOLD/SVXY…), warrants and preferreds
(the same junk the #1646 detector filters with CUSIP gates). Suppression therefore applies only to
the curated set; everything else keeps the legacy product.

1. **Issuer CIK** — primary `(sec, cik)` row. None → `not_multiclass`.
2. **Multi-class oracle** — `EXISTS` a row in `instrument_class_shares_outstanding` for the CIK.
   Absent → `not_multiclass` (single-class or uncurated → legacy `compute_market_cap`).
3. **Build total** (`_build_total_company_cap`): success → `total_company`; any guard miss →
   `multiclass_unavailable` (**fail closed = suppress**; never publish combined×price for a known
   dual-class issuer — Codex ckpt-1 HIGH).

`_build_total_company_cap(conn, instrument_id, cik)`:

1. `period_end = MAX(period_end)` over the CIK's FSDS rows. **Reject `period_end > today`** (a
   future period is corrupt; the staleness policy treats future as not-stale — the cap path must
   not inherit that loophole — Codex ckpt-1 MED).
2. `(instrument_id, shares)` rows AT `period_end` (driven off `source_cik`). Require **≥2 distinct
   siblings** → else None.
3. **Combined at the SAME instant** (`_read_combined_shares_near`, us-gaap
   `CommonStockSharesOutstanding` nearest `period_end`). None → None. **Bound the delta**:
   `|combined_as_of − period_end| ≤ 400d` (~4 quarters) — else None (a far/orphan combined row must
   not drive residual math — Codex ckpt-1 MED). `financial_facts_raw` holds only the combined count
   (companyfacts strips dimensional per-class facts, #1646).
4. **Per class**: `class_shares_usable(class_shares, period_end, combined, today)` (freshness ≤548d
   + structural `0 < class < combined`, the predicate extracted from `_should_use_class_denominator`
   so the policy is single-source) **and** a positive `_latest_price` for the sibling. Any miss → None
   (fail closed).
5. `sum_mapped_shares = Σ class_shares`. **`sum_mapped_shares > combined × (1 + 0.5%)` → None**
   (a real excess is a source/period mismatch, not a residual — don't clamp-and-publish a broken
   invariant — Codex ckpt-1 HIGH).
6. `residual = max(0, combined − sum_mapped_shares)`. **`residual > combined × 25%` → None** (a
   large residual means a major class is unmapped → the imputed leg would carry too much of the
   value — Codex ckpt-1 HIGH/MED).
7. `impute_price = price of the largest-share leg` (representative traded class; Alphabet Class A
   prices the Class B residual — identical economic rights, 1:1 convertible).
8. `value = Σ(class_shares × price) + residual × impute_price` (pure `_sum_class_caps`, table-tested).
   Return `TotalCompanyMarketCap(value, period_end, combined_shares, sum_mapped_shares,
   residual_shares, imputed_residual, leg_count)`. market_cap stays a bare `Decimal | None` on the
   API identity (no new API surface — the endpoint reads `.value`).

## Endpoint (`app/api/instruments.py` ~3416)

```python
res = resolve_market_cap_basis(conn, instrument_id=iid)
if res.basis == "total_company" and res.total is not None:
    cap_value = res.total.value
elif res.basis == "multiclass_unavailable":
    cap_value = None                                       # fail closed
else:                                                      # not_multiclass
    single = compute_market_cap(conn, instrument_id=iid)
    cap_value = single.value if single is not None else None
```

## Fail-closed table

| case | basis | result |
|---|---|---|
| genuine single-class / uncurated co-CIK (.US, ETF, warrant, preferred) | `not_multiclass` | combined × price (legacy, unchanged) |
| curated dual-class, both quoted, fresh FSDS | `total_company` | Σ class×price + imputed residual |
| curated dual-class, a sibling unpriced / FSDS stale / combined far / Σ-overage / residual-heavy | `multiclass_unavailable` | **null** (suppressed) |
| curated multi-class, only one class in universe (`<2` siblings) | `multiclass_unavailable` | **null** (combined×price for unequal-rights classes, e.g. BRK-style, is nonsensical — Codex HIGH) |

Suppression is bounded to the 3 curated CIKs (+ future curated additions). Expanding #1623's
curated FSDS map automatically extends correct total-cap (and, where unbuildable, honest
suppression) to more issuers without touching this code.

## Reuse (grepped)

- `app/services/sec_identity.py:26 siblings_for_issuer_cik` — available, but we drive the sibling
  set off `instrument_class_shares_outstanding.source_cik` instead (FSDS-covered only).
- `ownership_rollup.py:2807 _read_shares_outstanding_near` — combined-at-instant reader (reuse).
- `ownership_rollup._should_use_class_denominator` — extract its freshness+structural predicate
  into public pure `class_shares_usable(...)`; the rollup gate then calls it `and
  max_pie_holder_shares <= class_shares`. Single source for the policy.
- `xbrl_derived_stats.py:72-81` quote-read expression.

## Tests

- Pure `_sum_class_caps`: two equal-price classes; diverging-price classes; residual>0 imputed;
  residual clamped at 0 when Σ>combined; single leg (n/a — gated ≥2).
- DB: GOOGL/GOOG total cap equal on both siblings; AAPL (single class) unchanged via fallback;
  HEI (sibling, no dev quote) → fallback; BRK.B (one class in universe) → fallback.
- Fallback parity: dual-class with a missing quote returns the same as `compute_market_cap` today.

## Dev-verify (panel + dual-class)

GOOGL, GOOG (equal total, ~$4.4T), AAPL (control, unchanged), HEI/HEI.A, METC/METCB, BRK.B.
Hit `/instruments/{symbol}/summary` and confirm `market_cap` renders + GOOGL==GOOG.
