# #1914 part-1 — FY history gap-fill (period-anchored fiscal_year)

## Problem (full-population, dev DB 2026-07-04)
`financial_periods` materialises ~1 annual row per *filing*, keyed on the filing's
SEC-stamped `fiscal_year`, so the comparative prior-years every 10-K reports are
discarded even though their facts sit in `financial_facts_raw` now.

- AAPL FY rows: 2025 / 2024 / 2023 / **2012** — FY2013–2022 missing; the Financials
  tab jumps 2023 → 2012.
- Full-pop (4,700 SEC-covered): 325 instruments have exactly 1 FY row; 609 ≤2;
  **386 have a year gap** (max−min+1 > row count). Mega-caps included.

## Root cause (code)
`_derive_periods_from_facts` (`app/services/fundamentals/__init__.py:1035`) groups
facts by `(fact.fiscal_year, fp)` where `fact.fiscal_year` is the **filing-stamped**
`fy`. SEC re-stamps every prior-year comparative in a 10-K with the *filing's* `fy`
(the #682 comment documents this). The #682 fix then takes `period_end =
max(period_end)` per bucket and keeps only that period_end's facts
(`canonical_facts`, line 1073) — correctly fixing the *value* of the filing-year row
but **discarding the comparative period_ends entirely** (they never become their own
`PeriodRow`). `financial_periods` is built `DISTINCT ON (fiscal_year, fiscal_quarter,
period_type)` from `_raw` (best-source match on `fp.fiscal_year = bs.fiscal_year`,
line 1546), so the sparsity propagates to the canonical table.

## Source rule
- **SEC XBRL companyfacts `fy`/`fp`** are the *filing's* fiscal context, NOT the
  fact's own period — a comparative annual duration ending 2021-09-25 emitted inside
  a FY2023 10-K is stamped `fy=2023`. The fact's authoritative fiscal-period identity
  is its **`period_end`** (+ `period_start`), not the stamp. (Documented in #682.)
- **Reg S-X 17 CFR 210.3-02** governs the *duration* statements (income,
  comprehensive income, cash flows): a 10-K presents the **three most recent,
  consecutive** fiscal years — so those comparatives are real, consecutive annual
  periods, recoverable and continuous by construction.
- **Reg S-X 17 CFR 210.3-01** governs the balance-sheet *instant* columns: **two**
  most recent fiscal year-ends. So gap-filled balance-sheet history may be **one year
  shorter** than income/cashflow history — expected, not a defect. Both are recovered
  by the same `period_end` regroup (instants also carry `period_end`).
- **`DocumentFiscalYearFocus`** for a filing's *own* primary period IS reliable; only
  the comparative re-stamps drift. So the fiscal_year of a `period_end` = the `fy`
  stamped when that `period_end` was the **primary (max) period** of some filing.

## Full-population verification (already run, per the #1914 root-cause comment)
Candidate fiscal_year mechanisms, distinct-period_end→one-fiscal_year collisions:

| mechanism | collisions | note |
|---|---|---|
| offset from SEC `fy` (originally greenlit) | 352 (7.6%) | inherits `fy`-stamp drift |
| naive `period_end.year` | 157 groups | + 2.6% mislabelled ±1 vs issuer (Feb-enders) |
| **primary-period-anchored (this spec)** | **137 (~1.5%)** | fewest; matches issuer labels |

The residual ~137 are *genuine* (fiscal-year-end changes / stub years / source
errors) where two real annual periods fall near one integer label. No integer-
`fiscal_year` key can represent them without dropping a real period — the bijective
identity is `period_end`. That is the #541 model fix, out of part-1 scope.

## Mechanism (part-1)
Re-derive the annual `fiscal_year` from the period's own identity, anchored on the
primary filing:

1. **Primary-anchor map** (pass 1): built from the SAME #558/#1835-admitted facts —
   only `_TAG_TO_COLUMN`-mapped facts that pass the duration guard (NOT raw filing
   facts; DEI/unmapped "as-of" contexts would lift the anchor to the filing date, per
   #558). For each `fp` and each accession, the *primary* period_end is
   `max(period_end)` over that accession's admitted `fp` facts; map that
   `period_end → fy` from the accession's stamp. (A period_end that is the max of any
   retained filing is "seen as primary".)
2. **Group by the period, not the stamp** (pass 2): group facts by `(fp,
   period_end)` instead of `(fact.fiscal_year, fp)`. Each distinct `(fp, period_end)`
   becomes a `PeriodRow` — the comparative years now survive.
3. **fiscal_year per row**:
   - if `period_end` is in the primary-anchor map → use that `fy`;
   - else (comparative-only, e.g. an aged-out 10-K's year) → **year-delta from the
     nearest anchor**: `fy = anchor_fy − (anchor_period_end.year − period_end.year)`.
     This carries the issuer's fiscal-year convention (Feb-enders included).
4. **Value assignment** unchanged in spirit: within a `(fp, period_end)` group, take
   the latest-filed fact per concept (restatement priority — same `filed_date DESC,
   accession DESC` order the #682 fix uses). This is *simpler and stricter* than the
   old max-and-discard: each period_end takes its own value, so a comparative can
   never overwrite a primary.
5. **Collision tail (~137)**: when two distinct `period_end`s re-derive to the same
   `(fiscal_year, fiscal_quarter, period_type)`, the collapse is done by the
   **existing** SQL best-source merge `DISTINCT ON (fiscal_year, fiscal_quarter,
   period_type)` in `_canonical_merge_instrument` (`__init__.py:~1588`), whose winner
   order — source priority → `filed_date DESC` → `period_end_date DESC` → `source_ref
   ASC` — is authoritative and unchanged. **Detection + logging lives in
   `_derive_periods_from_facts`** (Python), which alone sees every `(fp, period_end)
   → fiscal_year` mapping for the instrument before the SQL collapse: group the
   emitted rows by `(fiscal_year, fiscal_quarter, period_type)`, and for any group of
   ≥2, `logger.warning` the `period_end`(s) that the SQL order will drop (computed
   with the same winner order) — so nothing collapses silently. Winner selection
   itself stays in one layer (SQL); Python only mirrors its order to name the loser.
   The #541 `period_end`-key migration removes the collapse entirely.

Preserve the existing guards: #558 (DEI-context period_end exclusion — boundary
derivation stays on `_TAG_TO_COLUMN` facts), #1835 FY duration guard
(`_FLOW_DURATION_DAYS`, and the `months_covered < 11` FY delete), #682 restatement
priority.

## Scope (DECIDED — part-1)
Ship the clean gap-fills now on the existing integer `fiscal_year` key; defer the
~137 genuine collisions to #541 (logged). Delivers continuous mega-cap FY history
(AAPL FY2013–2025) immediately.

## Implementation loci
- `_derive_periods_from_facts` (`fundamentals/__init__.py:~1010–1145`): the regroup +
  primary-anchor map + year-delta fallback + collision log. Core change.
- Verify the periods_raw → `financial_periods` best-source materialisation
  (`__init__.py:~1500–1550`, match on `fiscal_year`) now sees distinct fiscal_years
  and no longer collapses comparatives.
- No schema migration (columns exist). No FE change (the read at
  `instruments.py:818` already orders by `period_end DESC` and renders whatever rows
  exist).

## Tests
- Pure test of `_derive_periods_from_facts` with a synthetic AAPL-shaped fact set:
  a FY2023 10-K carrying FY2021/2022/2023 comparatives → 3 distinct PeriodRows with
  fiscal_year 2021/2022/2023 (not all 2023). Feb-ender case (period_end 2025-02-01 →
  fiscal_year 2024 via anchor). Collision case → one row + a logged drop.

## Backfill + verification (ETL clauses 8–12)
1. `POST /jobs/sec_rebuild/run` `{"source": "sec_edgar"}` (or per-instrument for the
   panel first) on dev DB — re-derives `financial_periods` from raw.
2. Panel smoke AAPL/GME/MSFT/JPM/HD: `/instruments/{sym}/financials?period=annual`
   shows continuous prior years.
3. Cross-source: AAPL FY2021 revenue = **$365.817B** (10-K) vs rendered.
4. Record job invocation + figures + commit SHA in the PR.
