# #3850: the valuation view's count-based columns read the #3841 point-in-time row

**Ticket:** #3850 (refs #3841). **Status:** spec v4 + Amendment 1 (build). Codex checkpoint 1 ran three rounds, the cap: 36, 26 and 23
findings, each with an UNSOUND verdict. v4 applies the round-3 dispositions without a fourth round; "Checkpoint
record" gives every disposition.

**Depends on:** #3902, which must merge first (see "Sequencing"). **Follow-ups filed by this spec:**
- #3903: per-share columns summed across a split;
- #3904: the instrument page and the other price × count readers.

## Problem

Figures are printed by `PYTHONPATH=. uv run python scripts/measure_3850_premise.py` (dev DB 2026-10-19 14:44Z, code
`e8078de9`, PIT build 6). Its database reads share one REPEATABLE READ snapshot. Section 5 also runs the current
build reader against the current `submissions.zip`; the hash it prints equals build 6's. Section numbers refer to
that output.

"The session" of an instrument is its latest stored `price_daily` session with a positive close. sql/241's `priced`
uses that close unless a quote with a non-NULL derived price has a UTC date on or after it, or no positive close
exists. `current_price` and `price_as_of` are whichever price and timestamp it selected.

`instrument_valuation` (sql/241) multiplies `metric_price` by a count, in two arms:
- **complete-TTM arm:** the latest `financial_periods` row's `us-gaap:CommonStockSharesOutstanding`
  (`sql/220_ttm_strict_flow_sums.sql:155`);
- **legacy arm:** the latest `fundamentals_snapshot.shares_outstanding`.

`metric_price` is that selected price, divided by the curated ADS ratio for `ads_ratio` instruments outside the
dual-class set.

The count-based columns are `market_cap_live`, `enterprise_value`, `pb_ratio` (TTM arm), `price_sales`,
`p_fcf_ratio`, `fcf_yield`, `ev_revenue` and `ev_ebitda`. Their readers:
- `scoring.py`: `fcf_yield`;
- `ranking_pot.py`: the cap breakpoint and the `cap_unavailable` gate;
- `ai_trial_pack.py`: the < $2B shortlist;
- `thesis.py`;
- the ADS branch of `xbrl_derived_stats.resolve_market_cap_basis`.

eToro back-adjusts prices for splits, while XBRL counts are as reported (#3841 "Problem"). The count is dated at a
past instant, so:

1. **An unresolved price basis.** PIT refuses some instruments the view publishes a cap for (section 1):
   `basis_unresolved` 56, `anchor_contradicted` 1, `basis_stale` 9. Each is a refusal to pair the count with the
   stored close, not proof of a split.
   - MNST: the view publishes 979,490,000 × 46.38 = $45.4B. Its anchor window reads F̂ 2.0117 at 2026-06-30 and
     1.0017 at 2026-09-30 (section 4).
   - Its counts are pre-split under SAB Topic 4C's own interval. The balance-sheet count 979,490,000 (period
     2026-06-30) and the cover count 979,525,882 (context 2026-07-31, accepted 2026-08-07, step evidence) agree to
     0.004%. A split effective before acceptance would show in a restated or cover count near 1.96B, and none does.
   - So a re-basing of about 2× that eToro applied after the count's acceptance leaves today's figure at about half
     the company. The split's exact effective date is not established here.
   - APH reads 2.0114 → 1.0096.
2. **The view's count is not the #3841-selected count.** The #3841 selection is the cover count first, the
   balance-sheet count only when none is eligible, 15-month age, and the scale, turnover and discontinuity checks.
   - Among the 2,201 PIT-published rows with a positive view count, the view's count differs from PIT's effective
     count outside [0.9, 1.1] on 75, and below 0.55 or above 1.8 on 9 (section 2).
   - SPTX (legacy arm) publishes 2,595,342 × 17.75 = $46.1M. PIT selects the cover count 53,530,550 (context
     2026-07-27), giving $950.2M (section 4).
   - The differences are not attributed. Some may be balance-sheet counts legitimately restated under SAB Topic 4C
     (#3841 "count basis interval"). The claim is only that the view applies none of #3841's selection or checks.

## Source rule

| decision | rule | source |
|---|---|---|
| market cap | shares outstanding × price, on one share basis | CRSP/JKP `ME`; #3841 "Source rule" |
| which count, when public, which checks | #3841's selection, unchanged | #3841 "Source rule", "Selection and steps" |
| share basis | #3841's 13F-anchored factor, valid for the close of the session it is evaluated at; a count whose basis #3841 cannot establish is refused | #3841 "The factor for one count", "Build and publication" |
| the price paired with the count | the close of the same session, the only price whose basis #3841 checks | #3841 output definition ("put on the share basis of the stored close, × that close") |
| a column the view cannot compute truthfully | NULL | #1664 prevention entry; sql/201 |

No valuation rule is chosen by construction here. The latency bound in acceptance item 8 is.

## Construction

### The view

1. **Expose the session.** `priced` already selects the latest positive close (`pd`, with `p.close > 0`). The view
   carries `pd.price_date` as `session`.
2. **Read the PIT row at exactly the session.** This is a LATERAL equality lookup on
   `instrument_market_cap_pit (instrument_id, price_date = session)`. Steps are half-open intervals, and the PIT
   view joins each `price_date` to its covering step (#3841 "Selection and steps"), so equality does not need
   `valid_from = session`.
3. **`market_cap_live := pit.market_cap`.** This is PIT's own `close × shares_reported × split_factor`. It is
   already finite-checked (`invalid_input`), already NULL with a reason when refused, and already `basis_stale`
   when a price revision or series break lands (#3841 "Build and publication"). There is one owner, and the view
   computes no second product.
4. **Every other count-based column** is recomputed over `market_cap_live`, keeping its existing denominator guards:
   - `enterprise_value = market_cap_live + COALESCE(long_term_debt, 0) + COALESCE(short_term_debt, 0) - COALESCE(cash, 0)`;
   - `pb_ratio` (TTM arm) `= market_cap_live / shareholders_equity`;
   - `price_sales = market_cap_live / revenue_ttm`;
   - `p_fcf_ratio = market_cap_live / fcf_ttm`;
   - `fcf_yield = fcf_ttm / market_cap_live`;
   - `ev_revenue` and `ev_ebitda` over that `enterprise_value`.

   `ev_ebitda` divides by `ebitda_ttm`. In the legacy arm, `enterprise_value`, `price_sales`, `ev_revenue` and
   `ev_ebitda` stay structurally NULL as today. Its `p_fcf_ratio` and `fcf_yield` publish under their existing FCF
   guards. Section 3 counts 19 legacy-arm cap gains, without attributing their cause.

   **Inherited limitations.** These are unchanged, and the cap's validity does not cure them:
   - the non-cap operands (debt, cash, revenue, equity, FCF, EBITDA) are guarded for NULL and sign, not for
     finiteness;
   - EV treats missing debt or cash as 0;
   - FCF treats missing capex as 0;
   - EBITDA treats missing D&A as 0.

   These are sql/220 and sql/241 treatments, which this spec does not revisit. The NULL rule in "Source rule"
   governs the count term only.
5. **These columns are priced at the session close, not the quote.** A quote newer than the session can sit on a
   newer share basis: a split's first day, before the next `daily_candle_refresh` back-adjusts `price_daily` and
   writes the revision row that makes PIT `basis_stale`. #3841 checks the basis of the session close only.
   - Pairing the count with the quote would publish a wrong cap in that window with no refusal. Pairing it with the
     close does not.
   - The cost is staleness. Over the 2,765 rows the proposal publishes (section 8), today's `price_as_of` is on the
     session's own date for 1,485 rows. A Friday session sits against a Monday `price_as_of` for 1,261, and 19 rows
     have other pairs.
   - Of the 2,201 instruments published in both arms, 911 move by more than 0.5% (section 3, by threshold). For 526
     of them the two counts lie within 0.5% of each other, which section 3's threshold rule labels price-date-driven.
6. **Appended columns** (`CREATE OR REPLACE VIEW` appends only):
   - `session` and `valuation_price` (the PIT `close`);
   - `market_cap_reason` (below);
   - `market_cap_pit` (jsonb): the whole PIT row at the session, `to_jsonb` of every `instrument_market_cap_pit`
     column. That is the step's `valid_from`, `valid_to`, `shares_concept`, `accession`, `context_date`,
     `accepted_at`, `shares_reported`, `split_factor`, `evidence`, `build_id` and `step_revision_count`, plus PIT's own
     `reason`, `close` and `market_cap`. A consumer copies it in one statement.
7. **Final reason, in precedence order:**
   1. `dual_class`: the curated multiclass set, suppressed as today;
   2. `ads_ratio`;
   3. `fpi_adr`: the existing suppression, retained;
   4. `no_pit_row`;
   5. the PIT `reason`.

   Invariant: wherever the view has a row with `current_price > 0`, `market_cap_live IS NULL` if and only if
   `market_cap_reason IS NOT NULL`. When the reason is set, every column in item 4 is NULL.
8. **Unchanged**, with their residual exposure stated:
   - `current_price` and `price_as_of` (the selected price and its timestamp);
   - the per-share columns, TTM `pe_ratio` and `dividend_yield` and legacy `pe_ratio` and `pb_ratio`. They keep
     pairing `metric_price` with a filing-dated per-share figure, so they can still publish a wrong non-NULL value
     across a split (BKNG `pe_ratio` 1.23, evidence on #3903);
   - `shares_outstanding`, still exposed, and still an operand of legacy `debt_equity_ratio`
     (`fs.debt / (fs.book_value × fs.shares_outstanding)`), which has no price term and is not changed.

### ADS-ratio instruments are refused

The #3841 factor is measured on the quoted unit, because 13F reports ADS holdings for an ADS. The count is in
ordinary shares, and an ADS-ratio change re-bases the ADS price without changing that count. No ratio history
exists. Every `ads_ratio` instrument is therefore refused, and the refusal is the safety mechanism, not the snapshot.

The `ads_ratio` table holds 5 instruments, all `no_left_anchor` at the session (section 6). 4 of them (CRTO, ONC,
ZLAB, AKTX) have a cap today and lose it; TEVA has none today. The view never reads a ratio-divided PIT figure, so
it cannot double-divide whatever #3902 does to PIT's own ADS handling.

### The thesis consumer

`thesis.py:1236` reads the view into an explicit 18-field map (`_VALUATION_FIELDS`, `thesis.py:846`). Its prompt
(`thesis.py:1426`) re-rates from `valuation.current_price`. After this change, two kinds of multiple sit side by side
with different price bases:
- **count-based multiples** (`pb_ratio` in the TTM arm, `p_fcf_ratio`, `ev_revenue`, `ev_ebitda`) are priced at
  `valuation_price`, the session close;
- **per-share multiples** (`pe_ratio`, `dividend_yield`, legacy `pb_ratio`) stay on `current_price`.

The build changes three things:
1. it adds `session` and `valuation_price` to the query and the field map;
2. it adds a field-map test asserting both reach the rendered context with their types;
3. the prompt names the base per multiple: `valuation_price` for a count-based multiple, `current_price` for a
   per-share one.

Equity re-rating is then exact for P/B and P/FCF on the session base, and for P/E on the quote base. EV multiples
remain approximate, because EV is affine in price. That is an inherited prompt limitation.

**Residual, stated.** On a split's first trading day, before the next `daily_candle_refresh`, `valuation_price` is in
pre-split units while `current_price` is post-split. A count-based re-rating then yields a target in pre-split units.
This is bounded to about one session per split, and nothing detects the split before the refresh does. Today the
same window mis-prices the multiples themselves.

### Freshness, failed builds and job order

The PIT build runs daily at 08:20 UTC (`app/workers/scheduler.py:2920`), after `daily_candle_refresh`. Its steps end
at that day's `build_date`. That date is a coverage **boundary**, not a guarantee: an instrument can still have no
step at its session, as section 5 shows for 3 + 8 instruments.

- A failed build leaves the previous build active (#3841). Its rows stay valid for sessions it covers, unless a price
  revision or an unresolved `price_series_break` makes them `basis_stale`. Once the session passes the last
  successful `build_date`, the lookup returns `no_pit_row`.
- Between a refresh that revises a price and the next successful build, affected rows are `basis_stale`.

**Measured from retained records, not replayed.** `market_cap_pit_builds` is append-only, with build date, status and
finish time, and `job_runs` records each scorer run's start. Over the 30 days before the build PR, the PR reports for each scorer run
whether a successful build of that date had finished before the run started.

Refusal counts at run time are not reconstructable, because steps are replaced per build. They are measured going
forward: the acceptance script runs at the first scorer run after deploy.

### Provenance and frozen consumers

#3841 requires the first consumer that decides on its view to copy the whole row into its decision row in one
statement. `market_cap_pit` is that whole row.

- **Scoring** persists scores, not inputs. It is a ranking input, not a decision row, so it is unchanged.
- **`ranking_pot` v1 (decl 23) and the AI trial (decl 16).** Their modules are policy-hashed (`ranking_pot_policy`,
  `ai_trial_policy.POLICY_MODULES`, which includes `ai_trial_pack.py` and `ai_trial_pack_reader.py`). Editing one
  breaks its declaration (the #3574 incident), so neither is edited.
  - Both are halted. Decl 16 is `halted_operator` since 2026-10-04 (`ai_trial_state_events` event 3).
  - **The resumption boundary is the settled research-first rule**, not a dev-DB snapshot: no strategy opens
    positions until it has passed a historical backtest (`.claude/CLAUDE.md` "Objective and risk posture";
    2026-10-04). Neither declaration has, so either can trade again only under a new declaration, and a new
    declaration's reader is new code that copies `market_cap_pit`.
  - The build PR records the semantic change, with the merge SHA, on each declaration's issue. It runs
    `scripts.ai_trial_policy_guard` to confirm no hash moved.
- **`ranking_pot` v2** is not frozen. Before it freezes, its decision row must carry `market_cap_pit`. The build PR
  either adds it or posts that requirement on #3592 as a freeze precondition.

## Sequencing

Section 5 reconciles the 33 instruments with a view cap today and no PIT row at the session. These are what the
current reader observes, and their causes beyond these labels are not established:
- 11 are outside the validated universe;
- 11 are CIK-refused: `submissions_accession_conflict` 4 (AMN, AMZN, MORN, XPO) and `untimed_accession` 7, XOM
  among them. These are #3902;
- 3 have no steps;
- 8 have steps today that do not cover the session.

Built before #3902, this would blank AMZN and XOM, so the build starts after #3902 merges and its rebuild runs. The
build PR re-runs section 5 and traces each remaining row to a stated cause.

## Acceptance (corpus rung)

`scripts/measure_3850_acceptance.py`. The control is sql/241's body, and the treatment is the new body created as a
temporary view. Both are queried in one REPEATABLE READ transaction, so quotes, prices, revisions, steps and
fundamentals are one snapshot.

1. **Wiring, full population.**
   - **Row set.** The treatment has exactly the control's `instrument_id` set.
   - **Expected outcome, derived independently.** For each row, the script derives `session` from `price_daily`,
     then the final reason from the precedence list. It reads the inputs directly: the dual-class CIK set,
     `ads_ratio`, the FPI/ADR set, and `instrument_market_cap_pit` at the session. The view must agree on
     publish/refuse and on the exact reason.
   - **Published rows.** `market_cap_live` equals the PIT row's `market_cap`, and every item-4 column equals its
     formula recomputed in `Decimal` from the row's operands (`fcf_ttm`, `ebitda_ttm`, …), including the arm's
     structural NULLs and denominator guards.
   - **Refused rows.** Every item-4 column is NULL.
   - **Every row, published or refused.** Every item-8 column and every column not in item 4 equals the control.
2. **A/B of the view.** Per count-based column: kept, gained by arm, lost by reason, and changed by more than 0.5%.
   Each change is split into count-driven and price-date-only, using section 3's rule.
3. **Decision gates, both arms:**
   - `ranking_pot`'s NYSE breakpoint, and the members gained or lost through `cap_unavailable` and
     `cap_below_breakpoint`;
   - `ai_trial_pack`'s < $2B shortlist;
   - the scorer's `fcf_yield` gained, lost and changed;
   - value-family score changes;
   - full-population rank moves, attributed to the instrument's own cap input (count, price date or refusal) or to
     peers.

   An own-input move outside those causes fails.
4. **Independent values.** The boundary, stated: items 1–2 prove the view carries PIT's figure. They cannot detect a
   figure that is wrong inside PIT, such as a count on another class, issuance after the count, an estimated factor,
   or the offsetting and incomplete-adjustment cases #3841 lists. Those are #3841's residuals, and they reach every
   item-4 column. This item samples for them. The five-name panel (AAPL, GME, MSFT, JPM, HD) plus one published instrument per decile
   of published cap, against companiesmarketcap at the session, using #3841 slice 3's bracketing-Friday method.
   - A miss beyond ±5% blocks unless the PR traces it to a cause #3841 lists as a residual, such as a count on a
     different class or issuance after the count's date, with the filing that shows it.
   - #3841's own Gate 2 (`float_bound`) already applies at build. This spec adds no float check.
5. **Synthetic cases (DB tests).** These are frozen fixtures, independent of live data:
   - published with factor 1, 2 and 0.5;
   - PIT-refused;
   - a PIT row only on an earlier date (`no_pit_row` at the session);
   - `basis_stale` from a revision row;
   - `ads_ratio`;
   - `dual_class`;
   - a quote dated after the session (the cap uses the session close);
   - the NULL-iff-reason invariant.

   A pure test pins the precedence order of the final reason.
6. **Live expectations**, re-derived at build time from post-#3902 evidence and printed beside it:
   - MNST and APH refused while their anchor windows still read about 2 then about 1;
   - SPTX published at its then-selected count;
   - AMZN and XOM published.
7. **Corpus evidence** (CLAUDE.md "Corpus changes"):
   - smoke panel figures;
   - the item-4 cross-source check;
   - backfill: none, since this is a view;
   - live re-check after deploy.
8. **Latency.** Time the per-instrument query and `compute_rankings`'s full read before and after, and record the
   plans. A regression above 2× on either blocks; the 2× bound is chosen by construction. sql/241 records a past
   per-instrument regression to 1.7s.

## Rollback

sql/241 itself drops and recreates the view without `CASCADE` and issues no `GRANT`. The build PR re-checks both
facts at build time (`pg_depend` dependants, `information_schema.role_table_grants`).

The rollback is **one revert PR** containing both the rollback migration (sql/241's body in the same form, plus any
grant found) and the reverted `thesis.py`, so the thesis query never runs against a view without `valuation_price`.
A DB test applies the forward migration, then the rollback, and asserts the column list and a published row's value.
The thesis field-map test runs against each schema.

Scores and decisions produced while the change was live stay as recorded.

## Amendment 1 (build, 2026-10-19): the session is the latest close on or before the build's frontier

**Finding.** Construction item 2 reads the PIT row at the latest positive close. #3841's view refuses a bar later than
its step's `bars_through`, the last bar the build's snapshot held (`basis_stale`, sql/499). The build runs at 08:20Z.
The candle refresh writes session D's bar after the US close, so until the next build an instrument whose latest
positive close is D cannot publish a cap. An otherwise publishable covering step reads `basis_stale`; a step refusal
keeps its own reason (sql/499 checks it first); with no covering step there is no row.
- Measured on build 7 (2026-10-19 17:41Z): 3,562 instruments have `bars_through` 2026-10-16 and steps valid to
  2026-10-20. Each that receives a positive 2026-10-19 bar tonight loses its cap until the next build.
- `daily_candle_refresh` ran 77 times in the 7 days to 2026-10-19. The scheduled scorer (`morning_candidate_review`)
  ran at 03:01Z on 2026-10-19, after the 03:00Z refresh and before the 08:20Z build. `compute_rankings` has two more
  callers, `ranking_pot_job` and `ranking_pot_v2_job`, on their own schedules.

**Change.** The instrument's frontier is the `bars_through` its steps in the active build carry. One build writes one
value per instrument (its last stored bar in the build's snapshot), so the view reads it from the latest step. "The session" is the latest positive-close `price_daily` session on or before the frontier.
The view then reads the PIT row at exactly that session, as in item 2.
- **Frontier at or after the latest close:** the session is the latest positive close, as in item 2.
- **Frontier before it:** the session is the latest positive close dated on or before the frontier.
- **No frontier** (no steps, or a NULL `bars_through`): the session is the latest positive close. There is no step,
  or no publishable step, so the reason is `no_pit_row` or PIT's own.
- **No positive close on or before the frontier:** the session is the latest positive close, which is after the
  frontier, and PIT refuses it (`basis_stale` on an otherwise publishable step, else the step's refusal or
  `no_pit_row`).
- **A session with no covering step:** `no_pit_row`, as before.

**What this does not change.** The selection is by date only. Safety still comes from PIT's own checks at the
selected session. These are instrument-wide: a revision row, a backdated insert or an unresolved series break
refuses the earlier session too, as `basis_stale` on an otherwise publishable step. When a positive close exists on
or before a non-NULL frontier, a bar dated after the frontier is never the session. In the two fallback branches
above it can be, and PIT refuses it.
Item 5's argument for the session close over the quote is unchanged.

**Freshness, chosen.** The view imposes no age limit on the session. `session` carries the age to every consumer.
- The session is the latest positive close within the active build's snapshot frontier.
- In the ordinary case, a refresh appends one session after the build, and the session is one stored session older
  than the latest close until the next build. A refresh can append more than one session, or a session with no
  positive close, so the lag is bounded by the frontier, not fixed at one.
- After failed builds, it can be older still. #3841 keeps the previous build active, and build failure is a job-health
  signal (`market_cap_pit_rebuild` in `job_runs`). A correct cap at an older close is preferred to no cap. Every count
  and basis check still holds at that close.

**Scheduling is not the remedy here.** Ordering refresh → build → scorer would keep the latest close for one caller.
It would not cover the other two `compute_rankings` callers, the 77 refreshes a week, or a failed build. The fallback
covers every reader, and a later scheduling change can shorten the window without changing this rule.

**Inherited, not changed:** sql/499 refuses a NULL, NaN or non-positive close, but not a numeric `Infinity`. The dev
DB holds 0 such closes and 0 such counts (2026-10-19). This is filed against #3841, not handled here.

**Acceptance delta.**
- Item 1 derives the session the same way, from `instrument_share_count_pit.bars_through`.
- Item 5 gains these synthetic cases:
  - with a positive close on or before the frontier, a bar after the frontier is not the session, and the cap
    publishes at the earlier close;
  - on an otherwise publishable step, a revision row refuses that earlier session as `basis_stale`;
  - with no steps, the session is the latest close and the reason is `no_pit_row`;
  - with no positive close on or before the frontier, the session is the latest close, after the frontier, and PIT
    refuses it as `basis_stale`.

**Checkpoint record.** Codex checkpoint 1 ran 2 rounds on this amendment.
- Round 1 (12 findings, UNSOUND) confirmed the mechanism. Scheduling was evaluated and rejected, with the reasons
  above. The Infinity close is filed against #3841.
- Round 2 (3 findings, UNSOUND; no critical or high) raised three precision points, applied here without a third
  round: the fallback branches can select a bar after the frontier, a step refusal outranks `basis_stale`, and the
  lag is bounded by the frontier rather than fixed at one session.

## Not in scope

- The per-share columns: #3903.
- The instrument page cap and P/B, `fair_value_band` and historical `fcf_yield`: #3904.
- Curated dual-class issuers. The #1664 Python overlay still sums class counts × price, and PIT refuses
  multiclass.
- Coverage for `no_left_anchor`, meaning a left anchor status that is `thin`, `incoherent`, `no_close` or
  `anchor_ambiguous`. Refusing it is inherited from #3841, not re-decided. Its cost is 646 lost caps (section 3),
  measured against the decision gates in acceptance item 3. An alternative anchor is ticketed only if those gates
  show the coverage is needed.

## Checkpoint record

**Round 1 (36 findings, UNSOUND).** v2 addressed them. Round 2 confirmed these dispositions:
- 3 and 22: the exact-session population;
- 7–10: page work moved to #3904;
- 11 and 12: the ADS refusal;
- 13: the FPI suppression retained;
- 23–27: the risk argument withdrawn;
- 28: the claim that publication proves a split-free window is withdrawn;
- 32: the decision gates;
- 33: the reader inventory.

v3 resolves the rest as follows.

**Round 2 (26 findings, UNSOUND):**
- **1 and 4 (BLOCKING): the quote/basis window.** Fixed by pricing at the session close (item 5), with a synthetic
  quote-after-session test. This reverses v2's choice to keep the quote. v1's objection to the session close was
  the date contract and thesis; the date is now exposed (`session`, `valuation_price`) and thesis is adapted.
- **2 (BLOCKING): provenance.** "Provenance and frozen consumers" applies the obligation per consumer. The frozen,
  halted declarations are not edited (#3574) and the change is recorded on their issues. Round 3 found this
  incomplete; v4 completes it.
- **3 (BLOCKING): the refusal NULL set.** Fixed. It is limited to item-4 columns, and the item-8 columns are
  compared to control.
- **5:** an external miss blocks unless traced (acceptance item 4).
- **6:** non-finite operands. `market_cap_live` is PIT's own checked product.
- **7 and 8:** failed builds and `basis_stale` timing. Both are restated and both measured.
- **9:** legacy gains are stated (19).
- **10:** the `shares_outstanding` operand in legacy debt/equity is stated.
- **11:** the per-share residual is stated (item 8).
- **12:** the EV re-rating approximation is stated as inherited.
- **13:** one snapshot for both arms.
- **14:** synthetic factors of 2 and 0.5.
- **15:** the float check is removed; Gate 2 already applies.
- **16 and 17:** section 5 prints session coverage and the reader identities; causes are labelled as observations.
- **18 and 19:** anchor windows, arm, concept and context are printed. SPTX is corrected to the legacy arm.
- **20:** section 2 is restricted to published rows.
- **21:** the whole `ads_ratio` table is printed.
- **22:** comparisons use `Decimal`.
- **23:** frozen fixtures are separated from live expectations.
- **24:** the rollback is executable and tested.
- **25:** the price-selection wording is fixed.
- **26:** "may be".

**Round 3 (23 findings, UNSOUND; the last round under the cap).** Applied in v4 without a fourth round:
- **1 (BLOCKING): the thesis base.** The base is now chosen per multiple (count-based on `valuation_price`, per-share
  on `current_price`).
- **2 (BLOCKING): split-day units.** Stated as a bounded residual. No pre-refresh split detector exists, and today's
  multiples are wrong in the same window.
- **3 (BLOCKING): whole-row provenance.** `market_cap_pit` jsonb carries every PIT view column.
- **4 (BLOCKING): the resumption boundary.** Rested on the settled research-first rule; v2's freeze precondition is
  stated.
- **5 and 6:** the inherited operand treatments (finiteness, NULL-as-0) are stated.
- **7:** the acceptance boundary is stated in item 4.
- **8:** MNST is argued from its two counts' agreement under SAB 4C's interval. The refusals are no longer called
  splits.
- **9:** section 3 is labelled as threshold-based, with mixed changes counted as count-driven.
- **10:** "19 legacy-arm cap gains", with no cause claimed.
- **11:** section 6 prints today's cap per ADS row.
- **12:** BKNG cites #3903's evidence; the schedule cites `scheduler.py:2920`; the halt cites `ai_trial_state_events`.
- **13:** the build date is described as a boundary, and series breaks are added.
- **14:** freshness is measured from append-only build and job records, with refusal counts measured going forward.
- **15 and 16:** row-set preservation, an independently derived expected reason, and an all-row comparison of
  unchanged columns.
- **17:** the thesis field map and its test.
- **18:** rollback is one revert PR covering both schema and code.
- **19–23:** operand names, price-selection wording, the section-8 population, the "by construction" scope, and the
  dangling "28;".
