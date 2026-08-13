# #735 — project EntityPublicFloat (DEI cover-page) into financial_periods

Closes #735. Split out from #731; `sql/088` line 14 reserved `public_float_usd` for this ticket.

## Problem

`EntityPublicFloat` (DEI cover-page fact, 10-K only) is already ingested into
`financial_facts_raw` (43,117 rows / 4,096 instruments on dev; AAPL FY2025 =
$3,253,431,000,000) but never reaches `financial_periods` because:

- It is a **DEI** fact, deliberately **not** in `_TAG_TO_COLUMN`, so it is
  excluded from `mapped_facts` (correct — including it would lift the FY anchor
  to the cover-page date, #558).
- Its `period_end` is the issuer's **most-recent-Q2-end** (SEC-prescribed
  public-float "as of"), NOT fiscal-year-end. AAPL FY2025: float period_end
  2025-03-28 vs FY anchor 2025-09-27. The `canonical_facts` filter
  (`f.period_end == period_end`, line 1017) drops any fact whose period_end ≠
  the FY anchor.

Empirically confirmed: EntityPublicFloat is stamped `(fiscal_year=Y,
fiscal_period='FY')` per its 10-K context, so it lands in the `(Y, 'FY')` group
in `period_facts` — available to an overlay even though excluded from
`mapped_facts`/`canonical_facts`. DEI facts are already loaded into the derive
input (the #558 comment relies on it).

## Fix — option 1 (issue default: separate DEI overlay, no new table)

### Schema — `sql/197_public_float_usd.sql`
`ADD COLUMN IF NOT EXISTS public_float_usd NUMERIC(24,2)` to **both**
`financial_periods_raw` and `financial_periods` (mirrors `sql/088`
treasury_shares, idempotent). USD dollars; AAPL ~3.25e12 → 13 digits, fits.

### `app/services/fundamentals/__init__.py`
- `PeriodRow`: add `public_float_usd: Decimal | None = None`.
- `_derive_periods_from_facts`: after the `canonical_facts` value-application
  loop, for **FY rows only** (`period_type == "FY"`), overlay:
  - `pf = [f for f in period_facts if f.concept == "EntityPublicFloat"]`
  - pick the current float: `max(period_end)` then latest `filed_date` (guards
    against any comparative re-stamp under the same fy).
  - `row.public_float_usd = pf_pick.val`.
  - NOT added to `_TAG_TO_COLUMN`, `_BALANCE_SHEET_COLUMNS`, `_FLOW_COLUMNS`, or
    the Q4-derivation copy loops — annual-only, never carried to a quarter and
    never anchor-bearing. The Q4 row therefore has `public_float_usd = NULL`
    (correct — public float is a 10-K annual disclosure).
- `_upsert_period_raw`: add `public_float_usd` to the INSERT column list, the
  `%(public_float_usd)s` VALUES, the `DO UPDATE SET`, and the params dict.
- Projection merge into `financial_periods` (the `best_source` SELECT): add
  `public_float_usd` to the INSERT columns, the SELECT projection, and the
  `DO UPDATE SET`.

Concept name `"EntityPublicFloat"` referenced via a module constant
`_DEI_PUBLIC_FLOAT_CONCEPT` (no magic string; mirrors `DEI_TRACKED_CONCEPTS`).

## Out of scope
Surfacing on the ownership card / rollup API (renderer changes land in the #729
follow-up per #755). This PR only populates the column.

## Tests
- Pure (`tests/test_fundamentals_*` or new): `_derive_periods_from_facts` with a
  fixture of FY us-gaap facts (anchor = FY-end) + an EntityPublicFloat fact
  (period_end = Q2-end, fiscal_period='FY') → FY row gets `public_float_usd`,
  the FY anchor/period_end is unchanged (no #558 regression), quarters and the
  derived Q4 have `public_float_usd = None`, latest-filed wins on duplicates.
- DB (`-m db`): upsert a PeriodRow with public_float_usd → projection carries it
  to `financial_periods`.

## DoD (ETL clauses 8–12)
- Backfill: re-derive `financial_periods` for the panel (AAPL/MSFT/GME/JPM/HD)
  on dev via the fundamentals re-derive path; record `COUNT(*) WHERE
  public_float_usd IS NOT NULL` non-zero.
- Cross-source: AAPL FY2025 public_float vs SEC EDGAR 10-K cover page
  ($3,253,431,000,000 in `financial_facts_raw`, frame CY2025Q1I).
- Acceptance (#735): `SELECT COUNT(*) FROM financial_periods WHERE
  public_float_usd IS NOT NULL` non-zero across covered FY rows.
