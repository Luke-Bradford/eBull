# #3903: the valuation view's dividend yield on the total/ME form

**Ticket:** #3903 (remainder item 1; refs #3850, #3841). **Status:** spec v4 + Amendment 1 (build). Codex checkpoint 1 ran three rounds,
the cap: round 1 raised 30 findings, round 2 re-graded them and added 24, round 3 re-graded all 54 and added 21, each
UNSOUND. v4 applies round 3 without a fourth round, as #3910 did; "Checkpoint record" gives every disposition.

`instrument_valuation.dividend_yield` divides a per-share dividend sum by a price. The sum crosses share bases and
derived quarters, and it misses most issuers that report dividends only as a cash total. This spec moves the column
onto the #3850 market cap and the TTM cash dividends paid, read only from a window whose four quarters are all
observed. It follows #3911's P/E (`docs/specs/fundamentals/2026-10-19-3903-pe-total-form.md`).

## Problem

Figures are printed by `PYTHONPATH=. uv run python scripts/measure_3903_dividend_premise.py` (dev DB, run
2026-10-19 21:55Z, code `bfad5805`, companyfacts.zip SHA-256 file `12bad36a…`). Its database reads share one
REPEATABLE READ snapshot; section numbers refer to its output. The published-cap population falls through the evening
(an earlier run at 21:03Z printed 2,555; this one 2,348): the candle refresh writes `price_daily_revision` rows, and
`instrument_market_cap_pit` refuses those steps as `basis_stale` until the next build
(`sql/499_market_cap_pit.sql:78-81`, by design). "Proposed" is the construction below evaluated over stored columns
(`proposed_yield`).

sql/506 computes `dividend_yield = dps_declared_ttm / metric_price * 100` when both are positive, in the complete-TTM
arm only (`sql/506_instrument_valuation_pe_total_form.sql:140-142`), and suppresses it for an ADR of unknown ratio
(`:253`). `dps_declared_ttm` sums the window's quarterly `dps_declared` over its present members
(`sql/220_ttm_strict_flow_sums.sql:141-143`). Each member is on its own filing's share basis; eToro's price is
back-adjusted for every split (#3841 "Problem").

1. **Defective per-share members** (section 11). EFC's DPS members are 390, 390, 390, 0.39: today's view publishes a
   yield of 10,046%, the proposal 13.66%. CEG's are 0.3878, −0.7756, 0.4265, 0.4265 (0.17% today, 0.57% proposed);
   PAYC's 0.375, −0.75, 0.375, 0.375.
2. **Payers with no per-share fact.** HD, KO, FAST and O have no `dps_declared` member, so today's view publishes no
   yield; their stored cash dividends paid are $9.221B, $10.971B, $1.056B and $2.996B TTM.
3. **Against an independent rebuild** (section 5). On the 526 rows publishing a positive yield both ways whose
   companyfacts common-only rebuild resolves, today's yield is within 5% of the rebuilt yield on 342 and within
   [0.8, 1.25] on 410; the proposed yield on 518 and 520. This is evidence on those 526 rows only.

**Populations** (section 1), all 3,941 complete-TTM rows (every one has a sql/220 window):

| today | proposed | rows |
|---|---|---|
| published | published | 572 |
| published | NULL: cap refused | 419 (`basis_stale` 183, `no_left_anchor` 146, `no_pit_row` 72, `basis_unresolved` 14, `dual_class` 4) |
| published | NULL: fewer than four observed members | 32 |
| published | NULL: no observed member | 39 |
| published | NULL: a negative member | 3 |
| NULL | published (positive) | 262 |
| NULL | published (zero) | 9 |
| NULL | NULL: cap refused | 1,174 |
| NULL | NULL: fewer than four observed members | 50 |
| NULL | NULL: no observed member | 1,376 |
| NULL | NULL: a negative member | 5 |

**Discrepancy inventory** (section 8). Over the 572 rows publishing a positive yield both ways, today ÷ proposed is
in [0.8, 1.25] for 446. The script asserts on every row that today ÷ proposed = (V/P) × (DPS·N/D), with V the session
close, P `current_price` (equal to `metric_price` wherever a cap publishes, since a curated ADS ratio refuses the
cap), N = `market_cap_live / valuation_price` and D the four members' sum. V/P lies in [0.936, 1.049] and outside
[0.95, 1.05] on 2 rows; it is printed per discrepant row. Each of the 126 rows outside the band is tagged with the
first observational predicate it matches (`inventory_cause`; a candidate, not a verified cause): shares spread
≥ 1.5× 5; a negative DPS member 11; a positive DPS member over 50× the smallest positive one 2; a quarter with a
positive paid member and a NULL DPS member 83; stored paid sum outside [0.95, 1.05] of the rebuild 4; span over
365 days 1; untagged 20 (listed).

## Source rule

| decision | rule | source |
|---|---|---|
| dividend yield | `div12m_me` = Σ over the last 12 months of (per-share distribution on the current share basis × shares) ÷ ME; NULL when the security has fewer than 12 months since its first observation | JKP code, `bkelly-lab/ReplicationCrisis` at `67174c7f170b`: `GlobalFactors/market_chars.sas:122-131` (`div_sum = %apply_to_lastn(x=div_tot*shares, _n=&n., func=sum); div&n.m_me = div_sum/me; if count < &n. then div&n.m_me = .`), `:86-95` (`count` = months since the security's first observation) |
| a period with no distribution | zero, a published value. CRSP's zero is arithmetic on observed returns (`project_macros.sas:423`, `div_tot = (ret-retx)*lag(prc)*(cfacshr/lag(cfacshr))`); Compustat's is **imputed** for a missing value (`:566-569`, "Dividend Adjustment (set to zero if missing)", `coalesce(div, 0)`) | as cited |
| the dividend total | cash dividends to common shareholders of the parent: us-gaap `PaymentsOfDividendsCommonStock`, "Amount of cash outflow in the form of ordinary dividends to common shareholders of the parent entity." **The construction reads the stored `dividends_paid` members, a proxy** (below) | companyfacts `description` (section 12, verbatim) |
| market equity | the #3850 `market_cap_live`, the #3841 PIT cap at the session | #3850 spec "Source rule" |
| a column the view cannot compute truthfully | NULL | #1664; sql/201 |

### Local rules, stated as departures

- **Four observed members.** JKP imputes zero for a missing Compustat dividend. We do not: our NULL member is
  sometimes a payment the normaliser failed to store. Section 3 classifies the 171 NULL members of the 1,464 active
  windows (a window with a nonzero member) against `financial_facts_raw`, USD, with the quarter's own boundaries
  (`classify_member`): 114 have no tracked fact ending at the quarter end, 4 derive to zero, **22 derive to a nonzero
  payment the normaliser did not store** (22 windows, 13 with a published cap; each amount printed, all positive),
  and 31 are unresolved. "No tracked fact" is absence from the tracked USD facts, not proof that nothing was paid.
  The construction therefore publishes only from four observed members (zero included) and leaves
  `dividends_paid_ttm`'s sporadic sum (sql/220 `:17-22`) unchanged for its other readers. This also refuses the 48
  windows (26 with a published cap) whose NULL members all have no fact, which may be correct.
- **No negative member** — a display convention. The payment concepts describe a cash outflow; this spec cites no
  rule that a negative quarter is always an error, and treats it as unusable. It refuses 8 rows (3 published today);
  20 windows have a negative member (section 2).
- **Zero publishes as zero**, from four observed members summing to zero, and means "no cash dividends recorded in
  the window", not "a non-payer": 9 rows, 7 of which the rebuild also puts at zero and 2 (JACK, RFIL) where it has
  an unresolved quarter (section 5).
- **No history test.** JKP's 12 months since first observation is not implemented and not measured: the view holds
  no first-observation date, and `price_daily`'s first bar is our ingestion horizon (107 proposed publications have
  a stored series under 365 days, section 9). Four filed quarters are required instead.
- **Quarter intervals.** sql/220's shape guard tests end dates only; it does not establish four consecutive valid
  quarters. 25 proposed publications have a member whose stored interval is NULL or outside 80–100 days (section 8;
  KR and ACI store 112-day first quarters, queried separately); 7 active windows span under 330 or over 371 days
  (FLXS 0, GOLD.US 456, IBKR 396, JRSH 546, LNN 730, OLP 638, YUMC 395), and FLXS publishes. This is the same
  window every TTM column of the view reads; the start-date metadata is filed with the normaliser ticket.

### The numerator is a proxy

The normaliser fills `dividends_paid` from the first tracked concept with a value, in the order of
`app/providers/implementations/sec_fundamentals.py:255-261`: `PaymentsOfDividends`, then
`PaymentsOfDividendsCommonStock`, then three partnership concepts. us-gaap describes `PaymentsOfDividends` as "Cash
outflow in the form of capital distributions and dividends to common shareholders, preferred shareholders and
noncontrolling interests" (section 12).

- **Reconstruction rule** (`quarter_value`). Per concept and quarter, boundaries matched within 3 days: the
  latest-filed dated fact spanning the quarter; else, per year-to-date start, the latest-filed fact ending at the
  quarter end less the latest same-start fact ending the day before the quarter filed no later than it. Undated
  facts make a quarter unresolved, never absent. 45 window quarters with a NULL `period_start_date` take the
  previous member's end + 1 day (31) or end − 90 days (14) (`bounds`). The rule does not establish that two facts
  share an accounting revision basis beyond that filing-date bound.
- **The rule's own check** (section 4). `priority_value` applies the normaliser's priority to companyfacts facts
  **filed on or before each stored member's `filed_date`**, so a later restatement is not reported against an older
  stored value. Of 5,721 stored non-NULL members of active windows and proposed publications, 5,557 agree within
  0.5%, 59 differ, 100 are unresolved and 5 have no fact. The matching candidate's concept is
  `PaymentsOfDividendsCommonStock` 3,263, `PaymentsOfDividends` 2,263, partnership 31; a match does not prove which
  concept the normaliser stored.
- **Composition** (section 5). `common_quarter` takes `PaymentsOfDividendsCommonStock`; else a partnership concept
  (a proxy: those concepts do not establish the recipient class); else `PaymentsOfDividends` less the filed
  `PaymentsOfDividendsPreferredStockAndPreferenceStock` and `PaymentsOfDividendsMinorityInterest`. A quarter is
  unresolved when a payment fact does not derive, a filed deduction is unresolved, preferred dividends are declared
  with no paid amount to deduct, or the only payment fact is `PaymentsOfOrdinaryDividends`; it is zero only when none
  of these concepts has a fact ending there. An unfiled deduction counts as zero, so the generic branch is "less
  filed deductions", not verified common-only. Of 843 proposed publications, 761 are compared (75 have an unresolved
  quarter, 7 are zero in both); stored ÷ rebuilt is in [0.95, 1.05] for 749. The verified common-only subset — all
  four quarters on `PaymentsOfDividendsCommonStock` — is 481 publications, 470 in [0.95, 1.05]. Every published
  window outside [0.95, 1.05] is listed with both amounts and its per-quarter branch, and the 82 publications without
  a resolved comparison are listed by name.
- **Special distributions.** JKP's `ret − retx` carries every distribution; `PaymentsOfDividendsCommonStock` is
  ordinary dividends. `PaymentsOfCapitalDistribution` ("excluding ordinary dividends. Includes special dividends")
  has a nonzero amount in a window quarter for 28 windows, 6 active and 0 proposed publications (section 6). Whether
  a stored generic amount includes a special distribution is not established.
- **Timing.** Cash paid by payment date over four fiscal quarters, against JKP's ex-date distributions over 12
  months. No stored ex-date source covers the population (3 of 834 positive publications have a stored
  `dividend_events` ex-date in the 365 days to today, section 10), so the difference is not measured.

### Input policy

The view cannot see filing facts, so it publishes by the construction alone. One rule applies to every row, whether
or not it is in an external sample:

- **A stored member that disagrees with the filing rule, as of its own filing date, is an input defect.** 34
  proposed publications have one (section 5); 12 of them publish no yield today (ASTH, CHDN, CWBC, EBF, EPM, EXP,
  HRB, ORCL, PFLT, RPM, TNET, VALU), so this change newly exposes those 12. They stay published and are filed as one
  normaliser ticket. The reason is the view's limit, not equivalence with today's form (which reads DPS, not these
  members): refusing them needs filing facts the view does not hold, and the normaliser fix repairs every reader of
  the column at once.
- **A construction error blocks.** A proposed publication further from the rebuilt yield than today's and outside
  [0.95, 1.05] must trace to a stored member that disagrees with the rule, or to the concept priority (the generic
  concept stored where the common-only concept is filed). An untraced row fails acceptance. At 21:55Z there are 3:
  PRI (stored Q4 168,936,000 against the rule's 33,288,000) and SSNC (stored Q4 3,900,000 against 65,500,000), member
  defects; XRX, whose members agree with the rule's generic concept while its per-quarter rebuild is on
  `PaymentsOfDividendsCommonStock` (stored 29,000,000 against 15,000,000), the concept priority. None is untraced.
- **Rows the rebuild cannot judge** — the 82 publications without a resolved comparison, the 9 zero publications
  among them where unresolved — are listed by name and covered by the member check alone, which runs on every
  publication including all-zero windows. A zero publication whose members disagree with the rule is an input defect
  like any other.

**Alternatives rejected.** Stored ex-dates (3 of 834 covered); Σ DPS × that quarter's count (the DPS member is the
defective operand); `dps_cash_paid` (per-share, so it carries the same share-basis problem, and is raw-only); restating
per-share DPS (no split register, #3841 "Problem").

## Construction

### Two views (sql/507)

1. **`financial_periods_ttm`** (sql/220's body) gains one column, appended last:
   `dividends_paid_ttm_observed` = `SUM(dividends_paid)` when the window passes sql/220's shape guard (`COUNT(*) =
   4`, end dates within 330 days), `COUNT(dividends_paid) = 4` and `MIN(dividends_paid) >= 0`; else NULL. Every
   existing column keeps its expression and position; `CREATE OR REPLACE VIEW` permits an appended column. The build
   greps every reader of the view for `SELECT *`.
2. **`instrument_valuation`** (sql/506's body):
   - `new_pipeline` drops its `dividend_yield` expression and carries `ttm.dividends_paid_ttm_observed` in its place;
     the legacy arm carries `NULL::numeric`. The final SELECT lists its outputs explicitly and does not emit it.
   - Final SELECT: `dividend_yield = dividends_paid_ttm_observed / market_cap_live * 100` when `is_complete_ttm`,
     `dividends_paid_ttm_observed < 'Infinity'` and `market_cap_live > 0`; else NULL. `dividends_paid` is
     `NUMERIC(20,4)` and cannot hold ±Infinity (#3911 Amendment 1, finding 2); `NaN` sorts above every number, so
     `MIN >= 0` passes it and the upper guard refuses the sum.
   - Output columns, their order and types are unchanged.
3. **Consequences.** `dividend_yield` becomes count-based under #3850's invariant: NULL whenever `market_cap_reason`
   is set. The FPI suppression (sql/506 `:253`) is subsumed by the `fpi_adr` reason and removed. It is priced at
   `valuation_price`. Every other column is unchanged.
4. **Comments.** sql/507's header and both `COMMENT ON VIEW`s state the rule; the valuation comment says the legacy
   arm's `pb_ratio` is the last per-share column.

### Expected movement

Kept 572; gained 262 positive and 9 zero; lost 419 to a refused cap, 71 to an unobserved member and 3 to a negative
member. Of the 74 lost with a published cap (section 7), 26 file `PaymentsOfOrdinaryDividends`, which is not
tracked, in a window quarter.

### Readers

- **Scoring.** No reader: `app/services/scoring.py` does not read `dividend_yield` (grep at `a5557f47`).
- **Thesis.** `_WRITER_SYSTEM` (`app/services/thesis.py:1344-1349`, `:1448-1457`) and the field comment
  (`:862-863`) price dividend yield at `current_price` and allow re-rating "the multiple's own price × (your target
  multiple / the context's current multiple)". The build moves dividend yield into the `valuation_price` set,
  defines it as TTM cash dividends paid ÷ market cap, and states: a yield re-rates inversely, price × (current yield ÷
  target yield), only when both are finite and positive; a zero yield means no cash dividends recorded in the window
  and is not a re-rating basis. `_PROMPT_VERSION` becomes v10. The thesis window is listed and repaired as in the P/E
  spec ("Deploy order and the thesis window"), with v9 in place of v8.
- **`ranking_pot` exposure.** `ranking_pot_rebalance` reads `dps_declared_ttm`, `dividend_yield` and `current_price`
  (`app/services/ranking_pot_rebalance.py:703-712`) into `NameFacts` (`app/services/ranking_pot.py:145-149`).
  `ranking_pot_exposure.dividend_yield` (`ranking_pot_exposure.py:103-119`, `_usable` at `:54-55`: a finite Decimal
  above zero) returns None for a NULL, non-finite or negative `dps_ttm` or an unusable close; 0 for a zero `dps_ttm`,
  whatever the view says; None for a positive `dps_ttm` when the view yield or `current_price` is not usable (a NULL,
  zero or non-finite yield included); else yield × `current_price` ÷ 100 ÷ the close on D. Under sql/506 that
  product is DPS × the curated ADS ratio (1 for a non-ADS row) ÷ close. Under sql/507 it is (TTM cash paid ÷ N) ×
  (`current_price` ÷ `valuation_price`) ÷ close: the cash-total form scaled by the quote's drift from the session
  close; curated-ratio ADS rows lose coverage (their cap refuses), and payers with no DPS stay uncovered. The module
  states "Descriptive only: nothing here enters a decision, a look or a gate" (`ranking_pot_exposure.py:3`); the
  three files are hashed (`ranking_pot_policy.py:55`, `:62`, `:69`; `ranking_pot_v2_policy.py:47`, `:49`, `:53`) and
  not edited. v1 is halted pre-trade; v2 is not frozen and must state its exposure definition before it freezes. The
  build posts sql/507's apply time on #2842 and #3592 as the forward boundary (and a rollback's apply time as the
  reverse one); a readout spanning a boundary states it, by the capture's own time if exposure rows record one, which
  the build checks.
- **Not readers.** The instrument page and `ranking_ablation_reader` read `instrument_dividend_summary.ttm_yield_pct`
  (`app/api/instruments.py:4352`, `app/services/ranking_ablation_reader.py:412-416`). The two yields can disagree
  until that surface moves; the build notes it on #3904.

## Acceptance (corpus rung)

`scripts/measure_3903_dividend_acceptance.py`, in the P/E acceptance's form, in one REPEATABLE READ transaction that
is rolled back. **Control:** sql/220's `financial_periods_ttm` and sql/506's `instrument_valuation`, as deployed.
**Treatment:** sql/507's two bodies created as temporary views, the valuation body rewritten to read the temporary
TTM view by name; the script asserts from `pg_depend` that each valuation arm depends on its own TTM arm. It runs
after the 08:20Z PIT build and before the evening candle refresh.

1. **Wiring, full population.** Ordered column names and types of both views (catalog) equal between arms, except
   the appended column. Row sets equal. Every treatment `dividend_yield` equals an independent evaluation from
   `financial_periods` (sql/220's window, shape guard and `is_complete_ttm` applied explicitly, then
   `proposed_yield`) within relative 1e-12, NULL decisions and causes included; every other column of both views
   equals the control exactly.
2. **A/B.** Section 1's table re-printed; differences from "Expected movement" explained by data that changed.
3. **Input policy, full population.** Section 5 over the treatment: the member check on every publication, the
   input defects listed (newly exposed ones marked) and filed; every publication further from the rebuild than today
   and outside [0.95, 1.05] traced, else FAIL; every publication without a resolved comparison listed.
4. **Independent values.**
   - **NULL and zero checks:** GME (no observed member) NULL; BJRI (fewer than four) NULL; every zero publication
     enumerated with its four members and member-check result.
   - **External yields**, positive only, against a published TTM dividend yield (stockanalysis, else
     companiesmarketcap), recording the external price, date and TTM DPS. Mandatory slots by category: panel payers
     MSFT, JPM, HD, AAPL; gained cash-only payers O, FAST, KO; a negative DPS member, CEG; a DPS scale outlier, EFC;
     and the three untagged section 8 rows with the largest |log(today ÷ proposed)|. A non-panel name whose cap is
     refused at the run is replaced by the largest proposed-published name of the same category. **Minimum
     coverage:** at least 10 of the 12 slots reconciled and every category with an available name covered; a refused
     panel name is reported with its reason and is not a failure. Below the minimum, acceptance reruns after the next
     08:20Z build.
   - **Reconciliation, an exact identity.** With external price P_x and TTM DPS E, N_f the filing's latest weighted
     basic count, and C the rebuilt common-only TTM: ours ÷ external = (P_x / V) × (N_f / N) × (D / C) ×
     (C / (N_f · E)). Asserted per row.
   - **Pass:** |ours ÷ external − 1| ≤ 15%. Otherwise only these are divided out, and the residual must be within
     ±15%: the count factor N_f / N, only when N = `shares_reported` × `split_factor` with `split_factor` = 1 and
     `shares_reported` equals the filed cover or balance-sheet count at its context date within 0.5% (#3911
     Amendment 1, rule 2); the composition factor D / C, only when every stored member agrees with the rule and every
     quarter of the rebuild resolves. C / (N_f · E) — timing, dividend changes and the external page's method — is
     never divided out, nor is price. A row whose D / C traces to a member that disagrees with the rule is an input
     defect under "Input policy". Anything else fails. ±15%, ±5% and 0.5% are chosen by construction.
5. **Readers.** Deterministic fixtures call `ranking_pot_exposure.dividend_yield` once per branch above (DPS NULL,
   non-finite, negative, zero, positive; view yield NULL, zero, non-finite, positive; close unusable) with expected
   outputs under each arm. The latest v1 and v2 snapshot universes are then reported per combination: names
   changed, gained or lost. Nothing reads it for a decision.
6. **DB tests (synthetic):** published; four observed zeros → 0; a NULL member among positive ones → NULL while
   `dividends_paid_ttm` stays the three-member sum; a negative member → NULL; `NaN` → NULL; a refused cap → NULL with
   its reason; `fpi_adr` NULL before and after; `dual_class` and `ads_ratio` rows whose sql/506 yield is positive →
   NULL with the reason; a legacy row → NULL in both; a split window: DPS 25.00, 1.00, 1.00, 1.00 with
   `dividends_paid` 100M each, close 50 and PIT count 100M, published by sql/506 at 28 / 50 × 100 = 56% and by
   sql/507 at 400M / 5,000M × 100 = 8%.
7. **Latency.** Per-instrument and bulk reads of both views, both arms; above 2× blocks (#3850's bound).
8. **Corpus evidence** (CLAUDE.md "Corpus changes"): the smoke panel; item 4 as the cross-source check; backfill
   none (views); live re-check after deploy; the thesis-window listing.

## Amendment 1 (build, 2026-10-19): the control's source, the external E, and the generic concept's unfiled deductions

Figures are from dry runs of `scripts/measure_3903_dividend_acceptance.py` at 2026-10-19 22:40–22:46Z (code
`03030946` plus this branch's working tree; the run's first line prints both). Those runs fell after the evening candle
refresh, so outside the acceptance window (the panel's caps were `basis_stale`). They justify the
change. The timed run after the next 08:20Z build is the acceptance.

**Finding 1: the control cannot be read "as deployed" at the timed run.** The pre-push hook's smoke test applies a new
`sql/NNN` to the dev database, so once the branch is pushed the deployed views are sql/507's.
- **Change:** the control is built from files, as two temporary views. They are sql/220's `financial_periods_ttm` body
  and sql/506's `instrument_valuation` body (`sql/rollback/507_…`, byte for byte), the latter rewritten to read the
  former by name. The treatment is built the same way from sql/507. `pg_depend` asserts that each valuation arm
  depends on its own TTM arm and on no other.
- **Evidence of equivalence:** while the deployed views lack the appended column, the script compares them with the
  file control on every row and every column. In the 22:40Z dry run, before any push, both views were equal: 4,564 TTM
  rows and 4,561 valuation rows. This is equality of the two snapshots' rows, not a proof that the SQL definitions are
  equivalent.
- After the push the comparison cannot run, and the script says so. The result is then a comparison against the file
  baseline that the dry run proved equal to the deployment.

**Finding 2: the external page's "Annual Dividend" is forward, not trailing.**
- stockanalysis shows MSFT's Annual Dividend at $3.92, four times the declared $0.98. Each of the four ex-dates in the
  trailing year paid $0.91.
- **Change:** the script stores every dividend-history row the page lists from 2025-09-01, as (ex-date, amount).
  `external_ttm` derives E from the amounts whose ex-date falls in the 365 days to the external close.
- Specials stay in E, as JKP's `ret − retx` carries them; FCBC's $1.00 on 2026-01-02 is one.
- No reference name split in the period, so the page's amounts and the close are on one share basis.
- This is a comparison convention: JKP sums monthly `div_tot × shares` over 12 observations, and a single count does
  not reproduce those weights. N_f / N compares two counts only; the difference between Σ(dividend_t × shares_t) and
  N_f × Σ dividend_t stays in the residual C / (N_f · E), which is never divided out.

**Finding 3: the rebuild cannot see a deduction the filer does not tag.** GM's case:
- GM's stored members are `PaymentsOfDividends`, $1.169B TTM, and they agree with the filing rule.
- GM files no `PaymentsOfDividendsCommonStock`, and no preferred or minority-interest payment fact. The rebuild's
  "generic less filed deductions" branch therefore returns the same $1.169B (D / C = 1.000), and section 3 cannot flag
  it.
- GM's filed `DividendsCommonStockCash` (common cash dividends declared) is $602M over the window.
- Against stockanalysis (P_x 82.75, E 0.69), ours is 1.587% and the external 0.834%, a ratio of 1.903. Today's
  per-share form is near the external here.

This is the composition that "The numerator is a proxy" names: `PaymentsOfDividends` includes preferred and NCI
payments. It is the same root cause as XRX, which "Input policy" already classes as an input defect: the normaliser
stores the generic concept. XRX's case is the concept order; GM's is that no common-paid fact is filed. The view
cannot tell them apart from a stored scalar, because `financial_periods` records no concept provenance (its columns
are `source` and `source_ref`, grep 2026-10-19).

**The population.** Of 843 publications, 344 have a member whose rule concept is `PaymentsOfDividends`.
- 75 of those file a four-quarter `DividendsCommonStockCash` TTM.
- For 7 of the 75, stored ÷ declared lies outside [0.95, 1.05]: XRX 2.231, GM 1.942, WTFC 1.272, BUSE 1.212 (newly
  exposed), EFC 1.121, SRBK 1.055 and OGN 0.900.
- The other 269 cannot be screened this way, so the error of the generic-concept population is not bounded by this
  evidence.
- Declared is not paid, so timing alone can move the ratio. The screen produces candidates, not verdicts.
- `DividendsCommonStock` is not used, because it includes stock and other noncash forms.

**Change:**
- **Section 3** lists the investigation candidates: stored ÷ declared outside [0.95, 1.05], or a positive amount paid
  against zero declared. Newly exposed ones are marked.
  - They are candidates, not confirmed defects. Under "Input policy" they stay published, because the view cannot see
    the concept.
  - The normaliser ticket's scope gains them, together with what it would take to give a generic-only filer a
    common-paid operand: concept provenance per stored member, and deductions the filer does not tag.
- **Item 4:** a reference miss that the count and composition factors do not account for is classified as a
  numerator proxy defect when all of the following hold:
  - all four stored members agree with the rule, and one is on `PaymentsOfDividends`;
  - stored D ÷ the filed `DividendsCommonStockCash` TTM C_d lies outside [0.95, 1.05];
  - |D − C_d| exceeds the largest declared amount over the quarter before the window and the window's four.
    Declared is not paid. With a payment lag of up to one quarter, the window pays the preceding quarter's declaration
    instead of its own last one, so timing alone moves D by at most the larger of the two. If the preceding quarter does
    not resolve, the row is not classified. GM: |1,169 − 602| = $567M, against a largest quarter of $164M;
  - the filing corroborates the external page: (P_x / V) × (N_f / N, unless the count factor was divided out) ×
    C_d / (N_f · E) lies within ±15%.

  The classification does not divide D out. It shows that D is the wrong operand by D / C_d, which is printed, and
  that the filing's common amount agrees with the external one. A proxy defect is never counted as reconciled and does
  not count towards the minimum coverage of 10 of 12. It is listed and filed like a member defect, whose size the
  policy does not bound either. Any other miss fails. GM: D / C_d 1.942, corroboration 0.960.

**Codex checkpoint 1 on this amendment, round 1 (UNSOUND, 8 findings).**

| # | finding | action |
|---|---|---|
| 1 | P1: finding 3 excuses an operand-selection error | Kept as an input defect under the existing policy (the XRX precedent; same root cause). The reason, the unscreenable 269 and the normaliser's widened scope are stated. Never counted as reconciled. |
| 2 | P1: dividing out D / C_d cancels D | Replaced: no divide-out. The classification requires D / C_d outside the band, and the filing's C_d corroborating E. |
| 3 | P2: `DividendsCommonStock` includes noncash | Dropped; `DividendsCommonStockCash` only. |
| 4 | P2: the exemption allowed unresolved members | Now requires all four members to `agree`. |
| 5 | P2: the residual re-multiplied a factor never applied | Computed afresh from the price factor, the count factor (unless divided out) and C_d / (N_f · E). |
| 6 | P2: zero declarations dropped | Zero declared counted; positive paid against zero declared is listed. |
| 7 | P2: file control ≠ deployed, unproved | Row-and-column equality with the deployment, checked while the deployment predates sql/507; passed in the dry run. |
| 8 | P2: E not auditable | Dated events stored; E derived by `external_ttm`; share basis stated. |

**Round 2 (UNSOUND; 7 of 8 resolved, finding 1 partial; 8 new).**

| # | finding | action |
|---|---|---|
| 1 | P1, partial: declared ≠ paid, so agreement does not show the excess is noncommon | Timing bound added (above). A proxy defect is still classified, not passed: this amendment deliberately keeps "Input policy"'s treatment for the generic concept's errors (the XRX precedent). The alternative is blocking until the normaliser carries concept provenance, and that keeps today's form, which publishes EFC at 10,046% and no yield for HD or KO, and agrees with the rebuild on 342 of 526 rows against 518. |
| 9 | P2: the count factor did not check N = `shares_reported` × `split_factor` | Required, within 0.5%. |
| 10 | P2: a member defect excused the whole miss | Quantified: the members are corrected to the rule's values and the factors re-applied, and the residual must be within ±15%. In section 3 a member trace needs the corrected total within [0.95, 1.05] of the rebuild. A concept trace needs every member resolved. PRI and SSNC pass on their corrected totals. |
| 11 | P2: zero cases escaped tracing | A resolved zero rebuild against positive stored, and a zero publication against a positive rebuild where today is positive, both need a trace. The dry run has 7 resolved zero rebuilds, all stored zero, and no zero regression. |
| 12 | P2: duplicate slots | Named names are reserved first. Replacements and the untagged slots exclude every taken name, and coverage counts distinct names. |
| 13 | P2: slot rules | A non-panel name is replaced only for a refused cap or a failed category predicate. Any other missing yield on a named name fails, panel included. |
| 14 | P2: N_f / N does not carry historical weighting | Wording corrected (finding 2): it stays in C / (N_f · E). |
| 15 | P3: outliers not all printed | Section 3 prints every publication outside [0.95, 1.05] with both totals and its quarter branches (12 in the dry run). |
| 16 | P3: timestamp | Corrected to the runs' own times. |

**Round 3, the cap (UNSOUND; 9, 11, 14, 15 and 16 resolved; 1, 10, 12 and 13 partial; 5 new).** As in v4, the round's
findings are applied without a fourth round. Dry run 22:53Z.

| # | finding | action |
|---|---|---|
| 1 | P1, partial: the timing bound ignored the quarter before the window | The bound takes the preceding quarter's declared amount too, and an unresolved preceding quarter is not classified. The residual disagreement stands and is recorded: Codex holds that only a reconciliation to common cash paid should make a miss non-blocking. This amendment keeps the "Input policy" treatment and states why (round 2, finding 1). |
| 10 | P2, partial: the concept trace did not measure its contribution | Quantified. Each concept quarter is moved to the rebuild's value, after member correction, and the adjusted total must lie within [0.95, 1.05] of the rebuild. PRI, SSNC and XRX each adjust exactly to their rebuilt totals. |
| 12, 18 | P2: one symbol could hold several slots, or resolve to another instrument | Pools and the untagged ranking hold one canonical instrument per symbol, so a slot resolves to the instrument that was assessed. |
| 13 | P2, partial: the predicate was checked only when the name published | The predicate is checked first, whatever the yield. |
| 17 | P2: a zero corrected total divided zero by zero | Guarded. |
| 19 | P3: candidates called defects | Called investigation candidates. |
| 20 | P3: Input policy's counts are stale | The script separates resolved zero rebuilds: in the 22:53Z dry run, 75 publications lack a resolved comparison and 7 have a resolved zero rebuild, all stored zero. The 9 zero publications include JACK and RFIL, whose zeros remain unresolved. v4's 82 counted both groups as unresolved. |
| 21 | P3: a resolved zero rebuild printed as "unresolved" | Printed as "resolved zero". |

## Rollback

A revert PR applies `sql/rollback/507_…sql`, which restores sql/506's `instrument_valuation` body, and changes
`thesis.py` to v11: dividend yield back to `current_price` and DPS ÷ price, while **keeping** the inverse yield
re-rating and its positive-only guard (that arithmetic is right under either definition). The thesis window applies
in reverse, as in the P/E spec: theses written with v10 between the rollback migration's apply time and the jobs
child's PID change are listed and regenerated. The rollback's apply time is posted on #2842 and #3592 as the reverse
exposure boundary. `financial_periods_ttm` keeps the appended column, unused: removing it needs
`DROP VIEW … CASCADE` over both views. A DB test applies forward then rollback in one transaction and asserts that
`instrument_valuation`'s ordered columns, types and every row equal sql/506's on a fixture covering each NULL cause
and each suppression.

## Not in scope

Filed by the build PR:
- **Normaliser** (a corpus change): derive the 22 missed members; the 34 publications whose stored member disagrees
  with the rule; track `PaymentsOfOrdinaryDividends` and `PaymentsOfCapitalDistribution`; read
  `PaymentsOfDividendsCommonStock` before `PaymentsOfDividends`; quarter start-date metadata (FLXS and the 7 spans).
  A quarter with no payment fact may be stored as an **imputed** zero only with its provenance marked; whether an
  imputed zero counts as observed for this construction is decided there, on filing evidence.
- **Non-payers.** JKP's yield for a non-payer is zero. The view publishes zero only from four observed zeros; a
  window with no observed member stays NULL.

These stay on #3903: legacy `pb_ratio` and the derived-Q4 per-share rule. The instrument page's yield is #3904.

## Checkpoint record

Rounds 1 and 2 (54 findings), with round 3's grade and v4's action where round 3 left one open. Findings 16–22 and
27, 28, 30 were graded resolved in round 2; round 3 marked them "unresolved" only because the v3 record carried no
text for them: 16 tag labels, 17 lost-yield causes, 18 witnesses printed, 19 tight-band list, 20 exhaustive
populations, 21 ex-date bound, 22 price attribution, 27 reader evidence, 28 inverse yield re-rating, 30 split
fixture. No change.

| # | finding | round 3 | v4 |
|---|---|---|---|
| 1, 2, 7, 8, 13, 31, 33, 34, 37–39, 43, 45, 46, 50–52 | | resolved | |
| 3, 4 | unresolved / generic as common-only | partial | ordinary-dividend-only and declared-preferred quarters unresolved; generic branch renamed "less filed deductions"; verified common-only subset reported (finding 55) |
| 5, 36 | intervals | partial | stated as the view's shared window limit; metadata filed (finding 63) |
| 6, 35 | vintage | partial | member check as of each member's `filed_date`; revision-basis limit stated (finding 58) |
| 9 | imputed zero | partial | imputed and observed zeros distinguished (finding 64) |
| 10, 40 | specials | partial | count is nonzero amounts; the unresolved denominator not separated, stated |
| 11 | timing | partial | stated as unmeasured |
| 12 | history | partial | restated as first observation, not listing |
| 14, 41 | selected population | partial | the 526-row comparison scoped to itself; unresolved publications listed (finding 60) |
| 15 | rule-check scope | partial | stated |
| 23, 42 | input policy | partial | justification corrected; newly exposed defects counted (findings 59, 60) |
| 24, 47 | factor division | partial | count factor needs `split_factor` 1; composition factor D / C needs a resolved rebuild (findings 61, 62) |
| 25 | coverage | partial | minimum coverage defined (finding 70) |
| 26, 48, 49 | pot reader | partial | full contract with `_usable`; fixtures per branch; ADS identity; forward and reverse boundaries (66–69) |
| 29 | rollback | partial | v11 keeps inverse re-rating (finding 75) |
| 32 | sign | partial | restated as a display convention |
| 44 | NaN | partial | the remaining comparisons check finiteness first (finding 65) |
| 53 | denominators | partial | member check covers all-zero windows (finding 59) |
| 54 | claims not emitted | partial | KR/ACI marked as queried separately; XRX's preferred attribution removed (finding 72) |

Round 3's new findings:

| # | finding | v4 |
|---|---|---|
| 55 | P1: uncertain fallback certified common-only | declared-preferred and ordinary-only quarters unresolved; branch renamed; verified subset separate |
| 56 | ignored payment fact read as no fact | `PaymentsOfOrdinaryDividends` alone makes the quarter unresolved |
| 57 | partnership recipient scope | labelled a proxy |
| 58 | vintage in the member check | facts filed by the member's `filed_date` only |
| 59 | P1: zero publications escape | member check on every publication; zero and unresolved publications listed |
| 60 | P1: false input-reuse premise | removed; newly exposed defects counted (12) |
| 61 | P1: count factor | requires `split_factor` 1 |
| 62 | payout factor bridge | only D / C on a resolved rebuild is divided out |
| 63 | impossible intervals | stated; filed |
| 64 | imputed zero called observed | distinguished; decided in the normaliser ticket |
| 65 | NaN elsewhere | finiteness checked first in every comparison |
| 66 | ADS factor in the old identity | stated |
| 67 | reader gates | full contract |
| 68 | reader combinations | deterministic fixtures |
| 69 | version boundary | apply times, forward and reverse |
| 70 | unavailable mandatory checks | minimum coverage and rerun rule |
| 71 | zero ≠ non-payer | reworded |
| 72 | claims not emitted | addressed |
| 73 | 8-K provenance | "stored `dividend_events`" |
| 74 | `dps_cash_paid` | rejected with reason |
| 75 | rollback restores wrong arithmetic | v11 keeps it |
