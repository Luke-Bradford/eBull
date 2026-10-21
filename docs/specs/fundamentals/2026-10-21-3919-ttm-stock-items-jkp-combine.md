# #3919: `financial_periods_ttm` stock items under JKP's annual/quarterly combine

**Ticket:** #3919 (refs #3903, #3920). **Status:** spec v4. Codex checkpoint 1 ran three rounds, the cap:
- round 1 raised 15 findings, applied in v2;
- round 2 left 6 open and raised 7 new, applied in v3;
- round 3 left 4 open and raised 3 new, applied in v4 without a fourth round, as #3910, #3915 and #3903 did
  ("Checkpoint record").

`financial_periods_ttm` (sql/508) takes its twelve stock items from the latest Q1–Q4 row only
(`MAX(x) FILTER (WHERE rn = 1)`). When an issuer's newest balance sheet is on an FY row (a 10-K with no Q4 row),
the view reads the quarter before it. This spec replaces that rule with the source's documented combine of the
annual and quarterly series, item by item. It also points P/B's staleness date at the date of the equity it
actually reads.

## Problem

All figures come from `PYTHONPATH=. uv run python scripts/measure_3919_premise.py` (dev DB, 2026-10-21), section
numbers in brackets.

- **[1]** 296 of the view's 4,564 rows have a latest FY end later than their latest quarter end.
  - NTNX reads Q3 (2026-04-30) equity −725.6M; its FY 2026-07-31 row holds +702.6M.
  - HRB reads −24.4M; its FY holds 117.5M.
  - MCFT reads 189.1M against 381.1M; PURR reads 743.5M against 1,872.9M [4].
- **Readers today:**
  - `instrument_valuation`:
    - P/B *refuses* these rows in both arms, because sql/509 Amendment 1's `NOT EXISTS` on a later period was
      written to wait for this ticket;
    - the complete-TTM arm also reads the stale quarter in `total_assets`, `total_liabilities`,
      `shareholders_equity`, `cash`, `long_term_debt`, `short_term_debt`, `shares_outstanding`, D/E, ROA, ROE, EV,
      EV/revenue and EV/EBITDA;
    - the legacy arm reads only `pb_equity` and `pb_equity_end` from this view (`sql/509:176-177`). Its cash, debt,
      shares and D/E come from `fundamentals_snapshot`, and stay so.
  - `peer_comparison.py:176-187` reads `total_assets`, ROE and D/E.
  - `fair_value_band.py` reads `total_assets`, `shareholders_equity`, `shares_outstanding`, `long_term_debt`,
    `short_term_debt` and `cash` through two query paths, `:938-953` (pass-1 cohort) and `:1305-1330` (the
    per-instrument read). It also reads `reported_currency` and `ttm_end`, which this does not change.

## Source rule

| decision | rule | source |
|---|---|---|
| which series supplies an item | per item: the quarterly value when the annual one is missing, or when the quarterly one is non-missing and its period end is later; otherwise the annual value. On equal period ends, the annual value wins unless it is missing | JKP, `bkelly-lab/ReplicationCrisis` at `67174c7f170b`, `GlobalFactors/accounting_chars.sas:1229-1237` (`combine_ann_qtr_chars`: `if missing(&ann_var.) or (not missing(&qtr_var.) and datadate&q_suffix. > datadate) then &ann_var. = &qtr_var.`) |
| which row of a series is current | the latest row with accounting data. The next row's start supersedes it whatever that row holds for a given item, so a series' current value of an item is its latest row's value, a missing item included | `accounting_chars.sas:1061-1075` (`where data_available=1`; sort by descending `datadate`; `end_date = min(intnx('month', next_start_date, -1, 'e'), …)`) |
| how long a value stays usable | never past the month-end 18 months after its period end, each series bounded before the combine | `accounting_chars.sas:1075` (`intnx('month', datadate, &max_data_lag., 'e')`); `main.sas:92-93` (`max_data_lag=18`, annual and quarterly) |
| duplicate rows on one period end | the FY series has none: the primary key is `(instrument_id, period_end_date, period_type)`. The quarter series keeps the view's dedup (latest `filed_date` wins, #1914 class) | schema; sql/220. 5 quarter groups tie on `filed_date` today, and none can win the view's dedup (its instrument's latest quarter end at the greatest `filed_date`) [3], so none reaches a stock item. This existing behaviour is not changed here |

### Local rules, stated as departures

1. **"A row with accounting data" = a normalized, non-superseded `financial_periods` row.** That is the view's
   existing eligibility, the local counterpart of JKP's `data_available=1`.
2. **Quarter-anchored population.** JKP left-joins quarterly characteristics onto annual rows. The view keeps
   its existing population, one row per instrument with normalized quarters. Instruments with only FY rows still
   get no row, and quarter-only instruments keep theirs.
3. **No publication lag.** JKP starts a value 4 months after its period end (`lag_to_public=4`). The view, as
   today, reads a stored row at once; that convention is unchanged.
4. **The 18-month bound is applied to the annual series only** (a deliberate, temporary departure, owned by #3920).
   - An annual row past the bound never supplies an item. Today 46 latest FY rows are past it.
   - The quarterly series keeps today's unbounded read. Today 108 latest quarters are past the bound [1], and
     bounding them would NULL stock items for those rows. #3920 owns the bound on the existing columns.
   - Two consequences are accepted until #3920 lands, and the tests pin them:
     - an expired quarter supplies an item whenever the annual side is unusable or missing that item;
     - when the annual row expires, the view falls back to an older quarter. That is today's value, unchanged.
5. **Two clocks.**
   - The view has no session, so the annual bound is read at `CURRENT_DATE`.
   - P/B applies its own 18-month bound at the valuation session, to the date of the equity the view actually
     returns (`shareholders_equity_end`).
   - When the session lags today and an annual equity expires between the two, the view returns the quarterly
     equity (or NULL) and P/B bounds that at the session.
   - The view's annual eligibility never admits an annual value past the bound at `CURRENT_DATE`, and P/B never
     publishes from equity past the bound at the session. Under departure 4, the view's *quarterly* stock values
     can still be past the bound. A test pins the crossing.
6. **Currency guard.** An annual row supplies items only when both `reported_currency` values are known and equal.
   The view keeps one `reported_currency` (the quarter's) for every column, which is why the guard is needed.
   - **The rule:** a NULL on either side, or a mismatch, refuses the annual row. Fixtures test it (test 8),
     whatever the corpus holds.
   - **The measurement, dated 2026-10-21:** 0 of the 4,487 instruments with both series differ or carry a NULL
     [1]. So the guard changes no row today.
7. **Twelve items only.** The items are `total_assets`, `total_liabilities`, `shareholders_equity`, `cash`,
   `long_term_debt`, `short_term_debt`, `shares_outstanding`, `inventory`, `receivables`, `payables`, `goodwill`
   and `ppe_net`, the columns sql/508 labels "Stock items". The per-period columns `eps_basic_latest`,
   `eps_diluted_latest`, `shares_basic`, `shares_diluted` and `reported_currency` stay on the latest quarter.

## Construction

One migration, `sql/512_financial_periods_ttm_stock_items_combine.sql`, in one transaction:

1. **`financial_periods_ttm`.** A CTE `latest_annual` holds the latest normalized, non-superseded FY row per
   instrument. It is LEFT JOINed to the grouped quarters as `a`. `usable` is a two-valued boolean, so a NULL can
   never select the annual side:

   ```sql
   COALESCE(a.period_end_date IS NOT NULL
            AND a.reported_currency IS NOT NULL AND q_reported_currency IS NOT NULL
            AND a.reported_currency = q_reported_currency
            AND CURRENT_DATE <= (date_trunc('month', a.period_end_date + interval '18 months')
                                 + interval '1 month - 1 day')::date, FALSE)
   ```

   The annual row is usable on the bound's month-end and unusable from the day after.
   - The latest FY row is chosen *first*; usability is judged on that row alone. An unusable latest FY row never
     makes an older FY row eligible (the row-supersession rule).
   - Likewise `q_x` is the latest quarter's own value, NULL included. It never searches older quarters.

   Each stock item becomes
   `CASE WHEN usable IS NOT TRUE OR a.x IS NULL OR (q_x IS NOT NULL AND ttm_end > a.period_end_date) THEN q_x ELSE a.x END`,
   where `q_x` is today's `MAX(x) FILTER (WHERE rn = 1)` and `ttm_end` is the latest quarter's end. One column is
   appended:
   - `shareholders_equity_end date`: the period end of the row that supplied `shareholders_equity`, NULL when the
     equity is NULL.
   - Every other column, its order and its type are unchanged.
2. **`instrument_valuation`** (sql/509's body with two edits; output columns unchanged).
   - `pb_equity_end` reads `ttm.shareholders_equity_end` in both arms.
   - Amendment 1's `NOT EXISTS (later stored Q1–Q4/FY period)` is removed. The invariant that replaces it is
     narrower. The chosen equity is the latest row of its own series, so no later value *of that series* is
     stored. A later row of the other series does not end it: either its equity is missing (the combine rule), or
     it is an annual row that is unusable (departures 4 and 6). The 18-month bound and the USD guard stay.
3. **`thesis.py`.**
   - `_WRITER_SYSTEM`'s "P/B is market cap / the latest quarter's equity" becomes "P/B is market cap / the
     selected equity: the latest annual equity when it is reported, usable through the month-end 18 months after
     its period end (as of today) and in the quarter's currency, unless the latest quarter reports equity at a
     later period end; otherwise the latest quarter's".
   - `_PROMPT_VERSION` goes v11 → v12, with a change note.

### Expected movement ([2], [4])

1,201 instruments change at least one stock item. Each instrument has one cause, because the cause is a property
of its two latest rows:
- 284 have a newer FY;
- 916 have a quarter item that is NULL, filled from the latest usable FY;
- 1 is an equal-end tie, XLAB, where the annual equity −25,158,775 replaces the quarterly −618,911.

No item changes to NULL. 647 of the changed rows have a published cap (13 newer-FY, 634 fill), so the fill is
the dominant operator-visible effect. Per item, the largest fill counts are `long_term_debt` 228,
`shares_outstanding` 185 and `goodwill` 131.

### Stored consumers

- **`fair_value_band`.** It runs on a 24h cadence. It appends to `fair_value_band_observations`, a history, and
  upserts `fair_value_band_current`.
  - Its next run writes `_current` from the new inputs.
  - Observations already written stay as they were computed: a history row records what was known then.
  - `fvb_v5` is unchanged, because the band method does not change.
  - Verification is A/B item 8 (frozen inputs before deploy) and corpus item 7 (refreshed rows after it).
- **Theses.** Theses keep their `prompt_version`. Those written under v11 stay, and the next writes are v12.
  Nothing is regenerated.
- No base-data backfill: both objects are views.

## Acceptance (corpus rung)

**Tests** (`tests/test_financial_periods_ttm_stock_items_combine.py`, DB tier). Expected values are written out
by hand, one fixture instrument per case. The view reads `CURRENT_DATE`, which a test cannot set, so the bound is
tested twice:
- at day granularity, the bound expression evaluated in SQL with literal clocks (a month-end and the day after,
  against the same period end);
- at month granularity, through the view, with period ends placed relative to `CURRENT_DATE` (test 6).

A separate assertion checks that the deployed view's definition reads `CURRENT_DATE`.
1. newer FY;
2. older FY filling a NULL quarter item;
3. a quarter value newer than a non-NULL FY;
4. an equal-end tie with the annual value present, and one with it missing;
5. no FY;
6. an FY ending in the month 18 months before the current one, whose bound is this month-end (usable), and one
   ending a month earlier, whose bound was last month-end (unusable);
7. row supersession on both series:
   - a latest FY row whose item is NULL superseding an older FY that holds it gives the quarterly value (or
     NULL), never the older FY;
   - a latest FY row that is currency-ineligible over an older eligible FY with a distinctive value gives the
     quarterly value, never the older FY;
   - a latest quarter whose item is NULL over an older quarter that holds it gives the usable FY's value when
     there is one, else NULL, never the older quarter, with the right `shareholders_equity_end` either way;
8. currency, with an annual value that would otherwise win in each variant: a mismatch, the annual side NULL, the
   quarter side NULL, and both NULL. Each refuses the annual row;
9. mixed provenance across items on one instrument;
10. departure 4's pinned behaviours:
    - an expired quarter supplying an item that the usable annual row lacks;
    - an expired annual row newer than an expired quarter, giving the quarter's value, which is today's;
11. `shareholders_equity_end` on each case;
12. P/B over cases 1, 2 and 6 in both arms, the legacy arm through a `fundamentals_snapshot` row without a
    complete TTM, including the session-versus-`CURRENT_DATE` crossing of departure 5;
13. a migration test:
    - apply sql/512, then the rollback body;
    - assert `pg_get_viewdef` of both views equals the definitions sql/508 and sql/509 create on a fresh schema;
    - assert the column types, the owner and ACL, and the two `COMMENT`s;
    - assert that `instrument_valuation` depends on `financial_periods_ttm`;
    - run cases 1 and 2 again against the restored views, and assert the old values;
14. tie candidates for A/B item 2's policy:
    - a latest quarter duplicated on one `filed_date` with differing values: the output must equal one whole
      candidate row;
    - an older-filed duplicate is never a candidate.

**A/B** (`scripts/ab_3919_stock_items_combine.py`). One REPEATABLE READ transaction that is rolled back.
- The control is the sql/508 and sql/509 bodies; the treatment is sql/512's bodies.
- Both are created as TEMPORARY views under distinct names, the treatment `instrument_valuation` referencing the
  treatment `financial_periods_ttm`.
- The binding is asserted through `pg_depend`.
- The script exits non-zero on any failure below.

1. **Wiring.**
   - `financial_periods_ttm`: ordered columns and types equal the control plus `shareholders_equity_end` last.
   - `instrument_valuation`: ordered columns and types are unchanged.
   - The `instrument_id` sets are equal.
   - Every non-stock column of `financial_periods_ttm` is equal.
2. **Stock oracle.** Each treatment stock item and `shareholders_equity_end` equals an independent Python
   evaluation of the rule. That evaluation reads `financial_periods` rows separately, including the bound and the
   currency guard, and holds on every row.
   - The oracle lists every instrument whose latest quarter end has more than one row at its greatest `filed_date`
     (`DESC NULLS LAST`); only those rows are candidates.
   - Such an instrument passes, and is printed, only when its whole output equals one candidate row: all twelve
     items plus `shareholders_equity_end`. Any other outcome fails the run.
   - Today there are none [3]; test 14 exercises the policy.
3. **Valuation oracle.**
   - Every `instrument_valuation` column that reads a stock item is recomputed in Python from the treatment
     inputs: the displayed stock columns, `enterprise_value`, `pb_ratio`, `debt_equity_ratio`, `roa`, `roe`,
     `ev_revenue` and `ev_ebitda`.
   - The recomputation covers both arms, with positivity, finiteness, the USD guard, the 18-month bound and cap
     availability. NULL masks and values must match within relative 1e-12.
   - Every other column equals the control exactly.
4. **The A/B.** Every changed row of each view is listed with its cause: newer FY, fill or tie for stock items.
   For `pb_ratio`, the cause is an equity change, a refusal lifted by the removed `NOT EXISTS`, or the bound now
   measured from the FY end. A change without a cause fails.
5. **Named figures, dated evidence for the run's date.** NTNX, HRB, MCFT and PURR read their FY equity while
   their FY row is newer and usable. Cross-source: NTNX's FY2026 `StockholdersEquity` at 2026-07-31 equals SEC
   companyfacts (`data.sec.gov`, no account).
6. **Latency.** A bulk read and 50 per-instrument reads of each view; the treatment stays within 2× the control.
7. **Corpus evidence.**
   - The AAPL/GME/MSFT/JPM/HD panel figures are unchanged, or each change is attributed.
   - The migration itself is applied by the pre-push smoke on the dev DB.
   - After the first post-deploy `fair_value_band_refresh`, every A/B item 8 instrument has a
     `fair_value_band_current` row computed after the deploy. Its band and its qualifying status equal item 8's
     *full treatment arm*, or the difference is attributed to the run's later as-of price.
8. **`fair_value_band`, frozen inputs.**
   - Both query paths (`fair_value_band.py:936-958`, `:1300-1335`) are run with their view name rebound to the
     control, then the treatment, at one as-of date.
   - **Isolated arm.** The band is synthesised through the module's pure functions for every instrument whose own
     inputs differ, with the pass-1 cohort held at the control. Every band change must be on an instrument with a
     changed stock input, and is listed with that input.
   - **Full arms.** Control inputs with the control cohort, and treatment inputs with the treatment cohort, are
     run over every instrument the pass covers. That catches instruments that move only through their cohort
     peers.
   - Each full-arm change is attributed to an own-input change, a cohort change, or both.
   - Instruments that start or stop qualifying are listed separately.
   - Corpus item 7 compares post-deploy `_current` with the full treatment arm.

## Rollback

`sql/rollback/512_financial_periods_ttm_stock_items_combine.sql` restores sql/508's `financial_periods_ttm` and
sql/509's `instrument_valuation`.
- `CREATE OR REPLACE VIEW` cannot drop a column, so the rollback runs `DROP VIEW instrument_valuation;
  DROP VIEW financial_periods_ttm;` and recreates both in one transaction.
- Plain `DROP` (no `CASCADE`) is deliberate. It fails, and the whole transaction rolls back, if the target
  database has any other dependent. The rollback's first statement is a `DO` block that raises, naming each
  dependent, when `pg_depend` shows one beyond `instrument_valuation`. On dev, 2026-10-21, there is none. Test 13
  covers the clean path.
- **Metadata.** Both views are owned by the migration role (`postgres`), with `relacl` NULL, and no file under
  `sql/` grants on them. So recreating them as that role restores ownership and access exactly. The rollback
  re-adds both `COMMENT`s, and test 13 asserts all three.
- As with sql/508's rollback, a revert PR adds that body as the next `sql/NNN` migration.
- The revert restores `_WRITER_SYSTEM`'s P/B sentence and `_PROMPT_VERSION` v11 together. Theses written under v12
  keep their version label, and nothing is regenerated, as for v11 in the other direction.

## Not in scope

- The 18-month bound on the quarterly series and on the other columns: #3920.
- `be_x` construction: #3921.
- A final tie-break for the 5 quarter `filed_date` ties (sql/220's dedup).
- Instruments with FY rows and no normalized quarter rows. The view has no row for them today, because it groups
  quarters, and this does not add one.

## Checkpoint record

- **Round 1 (Codex, 2026-10-21): 15 findings (10 P2, 5 P3).** All applied in v2:
  - #1 quarterly expiry: departure 4 states it, it is linked to #3920, and tests pin it;
  - #2 clocks: departure 5, plus test 11;
  - #3 currency: departure 6 adds a guard, measured over every instrument with both series;
  - #4 population: departures 1–2;
  - #5 the narrower invariant: Construction 2;
  - #6 unreproduced numbers: the script now prints each one, and a figure it does not compute was removed;
  - #7 instrument-level partition: an instrument has one cause;
  - #8 readers: both `fair_value_band` paths;
  - #9 stored consumers: a section of its own;
  - #10 fixtures: tests 1–12;
  - #11 valuation oracle: A/B item 3;
  - #12 binding and the migration/rollback test: `pg_depend`, plus test 12;
  - #13 prompt wording: Construction 3;
  - #14 tie-break: the FY series has none by its primary key, and the quarter ties are in "Not in scope";
  - #15 rollback of the prompt text: in "Rollback".
- **Round 2 (Codex, 2026-10-21).** Verdicts on round 1: #1, #4, #5, #7, #8, #9, #11, #14 and #15 RESOLVED; #2,
  #3, #6, #10, #12 and #13 NOT RESOLVED. 7 NEW (16–22). All applied in v3:
  - R1 #2: departure 5's claim is limited to the two eligibility checks;
  - R1 #3: `usable` is two-valued, and Python requires both currencies non-NULL (test 8);
  - R1 #6: the script counts the 5 quarter ties [3];
  - R1 #10: tests 10 and 12;
  - R1 #12: test 13 compares definitions and behaviour;
  - R1 #13: the prompt sentence states the rule;
  - 16: readers by arm;
  - 17: departure 6 separates the rule from the dated measurement;
  - 18: A/B item 8, plus corpus item 7;
  - 19: tie handling in A/B item 2;
  - 20: the rollback's dependency guard;
  - 21: the month-end and next-day boundary, relative fixture dates, and named figures as dated evidence;
  - 22: script docstring and tie table.
- **Round 3 (Codex, 2026-10-21), the cap.** R1 #2, #3, #6, #10, #12 and R2 #16, #17, #20, #22 RESOLVED. NOT
  RESOLVED: R1 #13, R2 #18, #19, #21. 3 NEW P2 (R3-14, 15, 16). All applied in v4, without a fourth round:
  - R1 #13: the prompt sentence states the month-end bound;
  - R2 #18: the isolated and full FVB arms, with post-deploy compared to the full arm;
  - R2 #19: candidates limited to the winning `filed_date`, a whole-row match, an exit policy, and test 14;
  - R2 #21: the day-level bound tested with literal clocks, and the view tested at month granularity;
  - R3-14 and R3-15: test 7's supersession cases, plus the selection order in Construction 1;
  - R3-16: owner, ACL and comments measured, restored and tested.
- **Residual.** This is a view over the quarterly series that today has no 18-month bound (departure 4, #3920).
  Until #3920 lands, a stale quarter still supplies stock items.
