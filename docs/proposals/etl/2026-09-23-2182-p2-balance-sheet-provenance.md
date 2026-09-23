# #2182 part B — P−2 balance-sheet overwrite: per-row balance-sheet provenance

Status: this PR. Follows part A (`docs/proposals/etl/2026-09-23-2182-fy-presentation-scope.md`, #3333).

## Mechanism

`financial_facts_raw` keeps the latest 3 10-Ks (`KEEP_10K`, `app/services/financial_facts_retention.py`).
The oldest retained 10-K's earliest income-statement year (P−2) mints a FY row: it has an annual
duration, so part A's gate keeps it, correctly. But no retained filing presents a balance sheet at
that date, so the row's balance-sheet columns are NULL or hold only 3-04 rollforward figures
(closing equity, retained earnings, cash). `_canonical_merge_instrument` sets `col = EXCLUDED.col`,
so every re-normalize overwrites the durable canonical balance sheet (AAPL FY2021 `total_assets`,
reproduced in part A).

## Source rule

Reg S-X, 17 CFR 210 (the text part A cites):
- **3-01(a)**: balance sheets as of the end of the **two** most recent fiscal years.
- **3-02(a)**: income and cash-flow statements for **three** fiscal years.
- **3-04**: the equity reconciliation for each income-statement year, which carries the P−2
  closing equity captions.
- **5-02.18**: total assets is a required balance-sheet caption. It is not an equity caption, so a
  3-04 reconciliation never carries it.

The P−2 year is reported for income and not for the balance sheet. That is a property of the
statement, not of the row, which is why no row-shape test found it (two discriminators falsified
on #2182).

## Change

1. **Derivation** (`_fy_balance_sheet_is_presented`): a FY row's `balance_sheet_presented` is True
   iff some `Assets` instant at that period_end lies within one fiscal year (0–395 d,
   `_FLOW_DURATION_DAYS["FY"]`) of its own accession's primary end. It is the same window as part
   A's instant branch, narrowed to the one caption a rollforward cannot supply. It fails closed:
   with no known primary end the answer is False, and False can only keep a canonical cell, never
   NULL one. A derived Q4 inherits its FY row's flag. Part A's `_fy_period_is_presented` is unchanged.
2. **Schema** (`sql/416`): `financial_periods_raw.balance_sheet_presented BOOLEAN NOT NULL DEFAULT TRUE`.
   The default keeps today's overwrite for every stored row until its instrument re-normalizes
   (DELETE-then-INSERT).
3. **Canonical merge**: for the 21 instant balance-sheet columns
   (`_PRESERVED_WHEN_UNPRESENTED_COLUMNS` = `_BALANCE_SHEET_COLUMNS` minus `antidilutive_securities`,
   whose concept is a duration fact: 152,096 of its 152,193 raw facts carry a `period_start`):
   - presented → raw overwrites every cell, NULLs included (unchanged);
   - not presented → keep the live (`superseded_at IS NULL`) canonical cell at the same PK, and fill
     from raw only when that cell is NULL.

   Every other column is overwritten as today, including income, cash flow and `public_float_usd`.

## Full-population A/B: two real arms

`PYTHONPATH=. uv run python -m scripts.ab_2182_p2_merge_arms` runs `normalize_financial_periods`
twice per instrument inside one REPEATABLE READ transaction, each arm in its own savepoint, and
rolls both back. Arm A forces every row to presented, which makes the merge SQL identical to main's
overwrite-all. Arm B is this branch. It then diffs every canonical cell. Scope: the 4,119
instruments with at least one unpresented row. Every other instrument runs identical SQL in both
arms.

| | |
|---|---:|
| instruments whose canonical differs between arms | **1,127** |
| cells B keeps where A writes NULL | 15,207 (`total_assets` 982) |
| cells where both are non-NULL and differ | 458 (439 equity) |
| cells where B is NULL and A is not | **0** |
| differing cells outside the 21 preserved columns | **0** |
| instruments whose canonical row SET differs | **0** |

Equity disagreements. `financial_facts_raw` is checked for a parent `StockholdersEquity` instant at
the row's date:
- **418 of 439**: raw has none, so the raw value is the lower-priority
  `StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` from the rollforward.
  That is not what the column means; B keeps the balance-sheet figure.
- **21**: raw has the parent concept at a different value, i.e. a later filing's recast. B keeps the
  originally presented figure. Residual: repaired by the next full companyfacts re-fetch, where the
  presented comparative is back in raw and latest-filed priority applies.

14 instruments fail normalization identically in both arms (each fails twice, once per arm): 11 with
the pre-existing `CardinalityViolation` (#2182 item 3) and 3 with `NumericValueOutOfRange`. Both
are unrelated to this change. Tally: `grep -oE "Failed to normalize instrument [0-9]+"` over the run log.

Supporting classifier run (`scripts.ab_2182_p2_balance_sheet`, same predicate): 12,010 unpresented
FY rows and 176 unpresented derived Q4 rows. 7,212 of them have no canonical row at their PK, so
they are plain INSERTs, identical in both arms.

## Codex ckpt-1 dispositions (40 findings)

Acted on:
- **#4** "any instant authorises the overwrite": the predicate now requires an `Assets` instant.
- **#10**: fail-closed with a `0 ≤ days` bound.
- **#23**: preservation reads only live canonical rows.
- **#32/#33** (the A/B executed no SQL and counted losing candidates): replaced by the two-real-arm
  run above.
- **#12/#14** (the equity preference rested on a sample): now the full-population concept check above.

Residuals. All are pre-existing or fail in the safe direction:
- **#1–3, #5** Transition-period and third-balance-sheet cases, and BS evidence found only
  outside `fp=FY` or in unmapped concepts. These make the flag False, which only keeps a cell the
  canonical row already holds, and fills NULLs as before.
- **#6–9** The primary-end and amendment heuristics are part A's.
- **#19/#25** A companies_house row deleted by Phase B before the preservation read: canonical and
  raw hold **0** non-`sec_edgar` rows (`SELECT source, count(*) FROM financial_periods GROUP BY 1`),
  so the path has no population today. This is also the ckpt-2 P2.
- **#20/#21** Phase B deletes same-label siblings at another date. That behaviour is unchanged, and
  it is the #541 fiscal-label collision class.
- **#16–18** A historical composite row (the retained balance sheet next to newer
  `source_ref`/`filed_date`) is the durable-history invariant `_canonical_merge_instrument`'s
  Phase-B2 comment already states. The flag is not carried to canonical.
- **#22, #24, #26, #28–30** Phase B2 transitions, concurrency, the `CardinalityViolation` class
  (#2182 item 3), and Q4 derivation are all pre-existing.

## Restore

`scripts.ab_2182_p2_balance_sheet` writes `/tmp/ab_2182_restore_ids.txt`: the 2,199 instruments with
an unpresented row whose canonical `total_assets` is already NULL. They feed
`scripts.force_refresh_fundamentals`, which re-fetches full companyfacts so the presented comparative
returns to raw. Pre-first-10-K years have no balance sheet anywhere, so for them the re-fetch is a
no-op. After this PR, the next retention sweep plus re-normalize keeps what the restore wrote.

## Out of scope

The pre-first-10-K ghost rows and the 13 `CardinalityViolation` instruments are #2182 items 2 and 3.
