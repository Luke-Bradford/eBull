# #2182 — FY history erosion: presentation-scope minting gate

Status: part A shipped by this PR; part B deferred on #2182 (see end). Replaces the falsified B1 framing
("require ≥1 duration fact", #2182 comment 2026-08-03).

## Mechanism (dev DB + code, 2026-09-23)

1. `financial_facts_raw` keeps only the latest **3** 10-K accessions per instrument
   (`app/services/financial_facts_retention.py`, `KEEP_10K`; data-engineer §13.D). Settled; unchanged.
2. `normalize_financial_periods` re-derives `financial_periods_raw` from whatever raw survives.
3. `_derive_periods_from_facts` merges FY facts globally by `period_end`, so a year that a retained 10-K
   touches only through an equity-rollforward / cash-flow **opening balance** got its own FY row.
4. `_canonical_merge_instrument` upserts `col = EXCLUDED.col`, so that near-empty row **overwrites** the
   durable canonical history (the invariant stated at `_canonical_merge_instrument`'s B2 comment:
   "the canonical row is the durable history").

AAPL (retained: FY2023/24/25 10-Ks): FY2020 became a row holding only the FY2021 opening equity, sourced
solely from the FY2023 10-K.

## Source rule

Reg S-X, 17 CFR 210 (text checked against the CFR, 2026-09-23):
- **3-01(a)**: audited balance sheets as of the end of each of the **two** most recent fiscal years.
- **3-02(a)**: income and cash-flow statements for each of the **three** fiscal years preceding the most
  recent audited balance sheet.
- **3-04**: equity changes as a reconciliation from the **beginning** balance for each period an income
  statement is required → an instant at the end of the year before the earliest income-statement year.

So an instant fact more than one fiscal year before its filing's primary year end is a rollforward
boundary: a true figure, but not a reported period.

## Change (part A)

`_fy_period_is_presented`: a FY `period_end` mints a row only if one of its mapped facts is
- an annual **duration** (any distance — transitions and recasts are never dropped), or
- an **instant** within one fiscal year (`_FLOW_DURATION_DAYS["FY"]` upper bound, 395 d) of its own
  accession's primary end.

Primary end = max `period_end` of the accession's mapped fp=FY facts: the same population the existing
fiscal-year anchor uses, so the gate and the anchor cannot disagree (ckpt-1 finding 21). Every accession
with mapped FY facts has one; there is no fail-open branch. Facts that do not mint still contribute values to
a row that a presented fact mints. Quarterly path is untouched.

Effect on canonical: a period no longer minted is not re-upserted, so its canonical row keeps whatever
history it holds. Nothing is deleted.

## Full-population A/B (two real arms, same raw)

`origin/main` (detached worktree) vs this branch, `_derive_periods_from_facts` over every instrument in
`financial_facts_raw` (5,264), read-only.

| | rows |
|---|---:|
| A rows / B rows | 74,542 / 67,877 |
| FY rows no longer minted | 6,664 (4,405 instruments) |
| rows changed in value | **0** |
| Q4 | 2 removed, 1 added: derived Q4 re-keyed off a crumb FY end (one value-less, one moved 2024-02-01 → the real FY end 2024-01-31) |

Gain-side inspection of the 6,664:
- 6 carry a duration column. All are duration concepts tagged as instants on a non-fiscal date (JXN
  2022-01-01 LDTI transition date; CANF 20-F). Correctly not fiscal years.
- 149 carry `total_assets`: 120 on off-cycle dates (quarter ends, transitions), 27 more than 2 years back,
  **2** at a genuine P−2 year end. Both of those already exist in canonical and are left untouched.

Stored arm: 4,861 of the no-longer-minted rows would **win their fiscal label** under main's canonical
merge. For **3,101** of those rows (2,941 instruments) the canonical row still holds real income and
balance-sheet history, which main would overwrite on the instrument's next re-normalize. This PR prevents
that. A further 1,689 canonical rows at those keys are already thin (clobbered earlier), so they need a
restore.

## Restore

`scripts/force_refresh_fundamentals.py` re-fetches full companyfacts history, then re-normalizes.
Panel (AAPL GME MSFT JPM HD) done pre-merge. Durability was checked on AAPL: after a real
`sweep_retention_for_instrument` and re-normalize, FY2020 survives (274,515,000,000 revenue, the same value the
SEC `frames` API gives for CY2020, accn 0000320193-22-000108).

## Part B — deferred on #2182

The P−2 year (the third Rule 3-02 income statement) still mints and still overwrites its balance-sheet
columns with NULL (AAPL FY2021 `total_assets` re-clobbered in the durability check above). The obvious fix,
a COALESCE canonical merge, was rejected at ckpt-1:
- it is bypassed by the fiscal-label Phase-B delete;
- it breaks the latest-FY-only `public_float_usd` overlay;
- it leaves wrong cells sticky with no way to repair them;
- it cannot keep per-cell provenance.

That needs its own design (per-cell or per-statement provenance).
