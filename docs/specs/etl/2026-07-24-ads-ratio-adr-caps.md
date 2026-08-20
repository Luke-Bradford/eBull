# ADS-ratio ingestion → exact ADR caps (#2117, #1939 step-2)

**Status:** spec → Codex ckpt-1 → impl
**Type:** schema migration (curated ref table) + view recreate + read-path change
**Depends on:** #1939 step-1 (PR #2116 `bacef745`: FPI/ADR fail-closed suppression, sql/237, `resolve_market_cap_basis::fpi_adr_unavailable`)

## Problem

Step-1 SUPPRESSES rather than corrects. The SEC DEI share count is the issuer's
**ordinary** count; the tradable price is **per-ADS** (1 ADS = N ordinary). So every
ordinary-shares × ADS-price product overstates by the ADS ratio. Two residual cohorts:

1. **AKTX class — still WRONG (not even detected).** Akari Therapeutics PLC files
   DOMESTIC forms (10-K), so the Rule 3b-4 `fpi` fingerprint misses it, and its name
   carries no ADR/ADS marker → it is NOT in the sql/237 suppress set and renders a fake
   **$1,425,698,338,428.81** cap (91.6B ordinary × $15.57; iid 1050391, dev).
2. **TEVA class — over-suppressed.** 1:1-ratio ADRs whose ordinary×price product is
   already correct, hidden by fail-closed until the ratio proves it.

## Source rule

The ADS ratio (ordinary shares per 1 ADS) is fixed in the **Form F-6 registration
statement / deposit agreement** (Securities Act Rule 466 / Form F-6 eligibility); the
20-F cover restates it. **No XBRL tag exists** for it.

**Standard-filing reuse check (mandated) — DONE, empirically:** edgartools 5.30.2 has
**no F-6 form constant and no F-6 parser** (registration coverage stops at S-1/F-1/S-3/
424B/497K). AKTX's F-6 POS confirms the ratio is deposit-agreement PROSE
(*"the new ratio shall be one (1) American Depositary Share to two thousand ordinary
shares"*), not a structured field. F-6 is also NOT linked to the scored ADR instruments
in our `filing_events` (0 rows for iid 1050391; the 60 F-6 rows we hold are non-scored /
depositary-entity CIKs). **→ curated per-instrument reference table, not a parser.**

## Population (full-pop dev scan)

Only **5 scored ADR instruments** render (or would render) a market cap:

| sym | iid | ratio | source (primary) | current state |
|---|---|---|---|---|
| AKTX | 1050391 | 2000:1 | F-6 POS 2023-08-17 acc 0000919574-23-004884 (explicit ratio-change) | WRONG $1.43T (undetected) |
| ONC (BeiGene) | 8692 | 13:1 | F-6 2025-05-01 (*"thirteen (13) ordinary shares"*) | suppressed |
| TEVA | 4336 | 1:1 | F-6 2023-02-08 (*"one (1) ordinary share"*) | over-suppressed |
| CRTO | 6185 | 1:1 | 20-F 2015-03-27 (*"Each ADS represents one ordinary share"*) | suppressed |
| ZLAB | 8878 | 10:1 | 20-F + independent web; cap-math 1,110M/10=111M ADS ✓ | suppressed |

Cross-source: each ratio also cap-math-checked (ordinary ÷ ratio = a plausible real ADS
count). AKTX: 91.6B/2000 × $15.57 = **$713M** (matches ticket's ~$0.7B).
⚠ Ratios CHANGE (AKTX 2023 change; ZLAB was 1:1 pre-2022) → table stores a
`source_date` + accession per row; treat as periodically re-verified curated data.

**Detection:** no clean auto-signal (F-6 unlinked; foreign-suffix over-includes
directly-listed ordinaries like Luxfer/Prothena/ADC whose caps are correct).
**Curated-table membership IS the detection** — it also catches the AKTX class the
fingerprint/marker miss.

## Design

### Schema — `ads_ratio` (sql/240)
```
instrument_id BIGINT PRIMARY KEY REFERENCES instruments(instrument_id) ON DELETE CASCADE,
ratio          NUMERIC NOT NULL CHECK (ratio > 0),  -- ordinary shares per 1 ADS
effective_date DATE,      -- when this ratio took effect (ratio-change aware)
source_form    TEXT,      -- 'F-6 POS' / '20-F'
source_accession TEXT,
source_date    DATE,      -- filing date of the evidence
note           TEXT,
created_at / updated_at TIMESTAMPTZ DEFAULT now()
```
Keyed by **instrument_id** (not CIK): a ratio applies to the specific ADS listing, not
all of an issuer's listings — CIK-keying would mis-apply to a same-CIK ordinary listing.
Seed the 5 rows via symbol-resolution (`INSERT … SELECT instrument_id … WHERE symbol=…`,
`ON CONFLICT DO UPDATE`) — portable (resolves per-DB), idempotent.
**CURRENT-ratio contract (Codex ckpt-1 #3):** this table holds ONLY the ratio in force
NOW; consumers compute only CURRENT figures (current price × current shares). Ratios
change (AKTX 2023; ZLAB pre-2022) — `effective_date`/`source_date` support periodic manual
re-verification. Do NOT use for historical-period recompute; a `valid_to`/history table is
a future concern if historical ADR recompute is ever needed.

### Correction — one substitution fixes every metric
Effective per-ordinary price = `ADS_price / ratio`. Every price-bearing column pairs the
per-ADS price with a per-ordinary basis (shares_outstanding is ordinary; eps/book/dps are
per-ordinary), so replacing `price → price/ratio` in the metric math corrects
market_cap, EV, pe, pb, ps, p_fcf, fcf_yield, ev_*, dividend_yield at once. The DISPLAYED
`current_price` stays the raw per-ADS price (that is what trades).

### View (sql/241, recreate `instrument_valuation`)
- New `ratio_known` CTE = `ads_ratio` **EXCEPT `dual_class`** (Codex ckpt-1 #1: curated
  multiclass dominates; a name that is both must NOT get its price divided — the `dc`
  suppression owns it). `ratio_known` and `dual_class` are ⊥ by curation; the EXCEPT is
  belt-and-suspenders.
- `priced` CTE LEFT JOINs `ratio_known`; add `metric_price = price / COALESCE(ratio, 1)`.
- `new_pipeline` / `legacy` compute all price-bearing columns from `metric_price`
  (ratio absent → COALESCE→1 → identical to today; **no-op for all non-ADR names**).
- Final SELECT `current_price` = raw `price` (unchanged).
- Suppress CTE becomes `fpi_adr EXCEPT (SELECT instrument_id FROM ratio_known)` — ratio-known
  names publish their (now-correct) metrics; ratio-absent fpi_adr still fail-closed.

### Python read path (`resolve_market_cap_basis`, instruments.py endpoint)
- New basis `fpi_adr_ratio` + scalar `value: Decimal | None` on `MarketCapResolution`
  (ratio-corrected cap; distinct from the multiclass `total`).
- **Ordering (Codex ckpt-1 #1): curated multiclass dominates.** Resolve in this order:
  1. curated multiclass (`instrument_class_shares_outstanding` present) → existing
     `total_company` / `multiclass_unavailable` logic (unchanged).
  2. else `ads_ratio` row present → `fpi_adr_ratio`, `value = compute_market_cap / ratio`
     (catches AKTX — not `fpi` — and un-suppresses CRTO/ONC/TEVA/ZLAB).
  3. else `fpi`/marker → `fpi_adr_unavailable` (fail-closed, unchanged).
  4. else `not_multiclass` → legacy `compute_market_cap` (unchanged).
- Endpoint branch: `basis == "fpi_adr_ratio"` → `computed_cap_value = cap_resolution.value`.

## Codex ckpt-1 resolutions
1. **Multiclass dominance** — encoded above (view `ratio_known EXCEPT dual_class`; resolve
   checks multiclass before `ads_ratio`).
2. **EPS/DPS/book basis** — PROVEN per-ordinary on dev: `ni/eps ≈ shares_outstanding` for
   ONC (1.045×, the decisive 13:1 case), TEVA (1.0×), ZLAB (0.94×); AKTX/ZLAB eps≤0 → pe
   NULL; no seeded name pays a dividend (`dps` NULL → `dividend_yield` NULL). `shares_outstanding`
   proven ordinary by cap-math. → the `price/ratio` substitution is correct for every
   populated price-bearing column incl. `pe_ratio`.
3. **Ratio-change validity** — `effective_date` added; CURRENT-only contract documented.

## Full-population verification (post-impl, to record in PR — clauses 8-12)
- Migration applied on dev; `ads_ratio` = 5 rows.
- `instrument_valuation` for AKTX/ONC/TEVA/CRTO/ZLAB: market_cap_live now finite & correct
  (AKTX ≈ $0.71B); non-ADR panel (AAPL/GME/MSFT/JPM/HD) byte-unchanged.
- `/instruments/AKTX` endpoint market cap ≈ $0.71B (was $1.43T).
- Cross-source: 4/5 ratios from EDGAR primary filings; ZLAB independent + cap-math.

## Out of scope
- Auto-detecting new AKTX-class names (curated additions are the maintenance path).
- Ratio-change monitoring (source_date supports periodic manual re-verify).
- MU's implausible $969 price (separate, not an ADR issue).
