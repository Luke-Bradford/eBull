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

## Amendment 2 (acceptance, 2026-10-20): a window longer than a calendar year can hold a fifth payment

The timed acceptance at 2026-10-20 08:30Z (code `270ce02a`, after `market_cap_pit_rebuild` 08:20–08:22Z) failed
section 4: 9 of 12 slots reconciled. KO's and FAST's external figures come from that run of
`scripts/measure_3903_dividend_acceptance.py`, whose log is on PR #3917. The population figures are printed by
`PYTHONPATH=. uv run python -m scripts.measure_3903_dividend_window_span` (dev DB, 2026-10-20 ~09:00Z, deployed sql/507
views, one REPEATABLE READ snapshot). The hand classification and the earlier spec's figures are labelled where used.

**Finding 1: KO, a defect of the construction.**
- Ours 2.729%, stockanalysis 2.248%: ratio 1.214, residual 1.214 after the count factor (acceptance log).
- KO's window is Q2'25–Q1'26, 2025-03-29 to 2026-04-03: 371 days. Its stored members (script section 5):
  - Q1'25 (to 03-28) $89M; Q2'25 $2,194M; Q3'25 $2,108M; Q4'25 $4,388M; Q1'26 (to 2026-04-03) $2,281M.
  - FY2025 = $8,779M = Q1'25 + Q2'25 + Q3'25 + Q4'25.
  - The window replaces Q1'25's $89M with Q1'26's $2,281M, so D = $10,971M, 1.250 × FY2025.
  - KO's single-quarter amounts are $2.1–2.3B, so the total is consistent with one payment more than four. The
    payment dates are not stored, and nothing here establishes them. What is established independently is that the
    window's envelope exceeds a calendar year.
- The source rule (above) is Σ over the last 12 months of (per-share distribution × shares) ÷ ME, from monthly
  observations. The construction's proxy sums fiscal-quarter cash paid. A fiscal window longer than a calendar year
  is not the source's 12 months. Section 4 keeps timing in C / (N_f · E) and never divides it out, so the FAIL is the
  rule working.

**The population.** The column the change touches is `dividends_paid_ttm_observed`: 1,368 non-NULL TTM rows (the
script asserts this equals a direct count, one row per instrument, and that each window's member sum equals the
view's value). No window member sits on a tied sql/220 dedup key (instrument, end, `filed_date`), and no FY comparator
key is tied. 1,055 rows have a published yield, and 1,045 of those are positive. Span runs from the first member's
(`rn = 4`) `period_start_date` to the last member's `period_end_date`.
- **Spans of positive publications:** 1,005 at 334–365 days; 1 at 366 (TFX); 37 at 367–371; FLXS at 0, from an
  inverted interval; BHE NULL.
- **Screen:** D ÷ the normalized FY `dividends_paid` with the latest period end, then the latest filing, unless the
  window's two boundaries are the FY's. It filters candidates: it does not count payments, and a ratio inside [0.87, 1.15] does not establish
  correct timing. Growth, cuts and composition can offset timing.
  - Windows over one calendar year: 35 positive windows screened; 3 lie above 1.15 (KO 1.250, HNI 1.320, TILE
    1.673) and 1 below 0.87 (CRI 0.649). KO is read from its members, as above; HNI and TILE are candidates.
  - Retained windows: 956 screened; 65 lie above 1.15 and 50 below 0.87. The script lists all 115 with the FY used.
  - By hand, the 30 highest include an initiation (GEV), an annual special (PGR) and growth. That classification
    covers those 30 only. It does not clear the other 85, nor a timing error inside the band.
  - Timing inside a retained window stays in section 4's residual.
- **Ex-date input:** for each of the 44 refused rows with a valid window, no stored `dividend_events` ex-date falls
  inside the window. For BHE and FLXS, coverage cannot be assessed, because their windows are not valid. This
  establishes what is stored, not what is obtainable. The earlier measurement ("Alternatives rejected", v4) found
  stored ex-dates for 3 of 834 publications.

**Change: `dividends_paid_ttm_observed` gains three conditions (sql/508). The rest of sql/507's expression is
unchanged.**
1. **Valid intervals.** All four members have a non-NULL `period_start_date` that is not after their
   `period_end_date`. `period_end_date` is `NOT NULL` in the schema.
2. **No overlap.** No member starts on or before the previous member's `period_end_date`, so no stored interval is
   summed twice. A cumulative member stored as a quarter (Jan 1 to each quarter end) fails this.
   - It refuses CRM, HUN and WWW. Each has a derived Q4 stamped with a start a year early (457–460-day intervals on
     quarter-sized amounts). That is probably metadata, but it is not distinguishable from a cumulative period
     without provenance, which the view does not carry.
3. **Within one calendar year, for a positive total.** The last member's `period_end_date` < the first member's
   `period_start_date` + `INTERVAL '1 year'`. The first member is `rn = 4`, read as
   `MAX(period_start_date) FILTER (WHERE rn = 4)`.
   - A zero total from four valid, non-overlapping, non-negative members is zero over every period it spans, the
     12 months included, so it publishes whatever the span (VSTS, 368 days).
   - The conditions apply in order 1, 2, 3, and a refusal reports the first that fails.
   - This is calendar arithmetic, not 365 days. An end on the anniversary is refused. A start on 29 February adds a
     year to 28 February, so its window must end by 27 February.

**What it is: a fiscal cash-yield heuristic with measured limits, not the source rule.**
- It does not establish ex-date membership. It removes one measured failure: a fiscal window longer than a calendar
  year, which for a schedule on fixed calendar dates can hold the same payment date twice.
- It does not refuse five genuine distributions. A special dividend inside a window under a year stays, as JKP keeps
  it.
- **Remaining false inclusions:**
  - a schedule drifting by business days;
  - a first start that understates the span (cross-checked below);
  - **gaps between members.** 101 publications have one, up to 30 days, and none also overlaps. A gap or a short
    window may drop a payment; whether any does is undetermined.
- **Exclusions:** of the 35 positive windows refused over a year and screened, 31 have in-band ratios. Their correctness is
  undetermined.
- **Why accept the coverage cost:** the construction already prefers NULL to an operand it knows can be wrong ("four
  observed members"). Nothing reads the yield for a decision ("Readers").

**No lower bound and no gap rule.** Neither has a source rule. The snapshot does not support a value: 17 of the 20
positive publications under 364 days lie within [0.87, 1.15], and the other three are TDG 0.550, DIN 0.645 and FLXS
(condition 1). A day minimum chosen from that would be fitted to the sample. The start metadata is known to be noisy
(KO's derived Q4 is stamped 10-01 after a Q3 ending 09-26), so a continuity rule would refuse on metadata.

**End-anchored cross-check, an acceptance diagnostic, not enforcement.**
- The span runs from the day after the preceding stored quarter's end (`rn = 5`) to the last end. The test is
  `e < (e5 + 1) + INTERVAL '1 year'`.
- A kept window over a year by this test is classed:
  - "predecessor missing, start filed" when `financial_facts_raw` holds a fact whose context runs from the stored
    first start to the first member's end. The filing itself then dates the start. A plausible interval length
    would not, because a start can be understated by days and still look like a quarter;
  - "start unverified" otherwise;
  - "no predecessor stored" when there is no `rn = 5`.
- **Today:** 996 kept publications are within a year by end dates. 7 are "predecessor missing, start filed": EBF,
  ETD, EXP, ORCL, RPM, SRBK and VALU, each with a preceding stored end 3 or 6 months before its start. 10 are zero
  totals, to which condition 3 does not apply. None is "start unverified" or "no predecessor stored".
- The guard is the enforcement. The cross-check guards the start-date input the guard relies on, at each acceptance.

**Measured effect.**
- 46 of 1,368 TTM values are refused: 41 over one calendar year, 2 with invalid intervals (BHE, FLXS) and 3 with
  overlapping members.
- 42 of 1,055 publications are refused, all of them positive (42 of 1,045).
- Expectation, not measured: a window over a year comes from a 53-week year, or from a quarter-end calendar drifting
  past the anniversary. It leaves when its long quarter rolls out.

**Scope and migration.**
- Only `dividends_paid_ttm_observed` changes. sql/220's dedup, ranking and shape guard, and every other TTM column,
  are untouched. Their treatment of a long window is a separate corpus change, recorded on #3903.
- The change ships as sql/508, not as an edit to sql/507, because the pre-push smoke has already applied sql/507 to the
  dev DB.
  - sql/508 replaces `financial_periods_ttm` only. Columns, order and types are unchanged.
  - `instrument_valuation` keeps sql/507's body.
  - `sql/rollback/508_…` restores sql/507's TTM body. Rolling back the whole construction applies 508's rollback,
    then 507's.

**Acceptance changes.**
- **Arms.**
  - Control: the original baseline, sql/220 and sql/506 from files (Amendment 1).
  - Prior: sql/507's TTM and valuation bodies.
  - Treatment: sql/508's TTM body with sql/507's valuation body.
- **Section 1.**
  - The independent evaluation applies conditions 1–3 after the member conditions. The causes are "invalid member
    interval", "overlapping members" and "window over one calendar year".
  - Treatment versus prior, NULL-safe, on every TTM row and every valuation row. Key sets are equal and unique.
    - Every column is equal except `dividends_paid_ttm_observed` and `dividend_yield`.
    - Each of those two equals the independent expected result exactly: the prior value when conditions 1–3 pass, and
      NULL when one fails, with that condition's cause.
  - The increment's refusals are printed by cause, as TTM rows and as publications.
  - The end-anchored classes are printed. A positive treatment publication classed "start unverified" or "no
    predecessor stored" FAILs.
- **Section 2:** the A/B prints the three causes.
- **Section 4.**
  - Expected NULL with "window over one calendar year": KO, HNI and TILE, alongside GME and BJRI.
  - Expected NULL with "invalid member interval": FLXS. Expected NULL with "overlapping members": HUN.
  - **Slot replacement.**
    - A non-panel slot made NULL by a condition is replaced like a refused cap: by the largest proposed-published name
      of its category not already taken.
    - The original slot's outcome is printed separately from its replacement.
    - A replacement that is unavailable is printed, and counts as unreconciled.
  - The 12 slots are distinct instruments. NULL checks never count as reconciliations.
  - A panel name made NULL by a condition is reported with its cause, and is not a failure.
  - **Boundary category**, mandatory and outside the 10 of 12: the two largest-cap positive publications with a
    364-day window, 52/53-week filers the guard retains. At least one must reconcile under item 4's rules, or FAIL;
    the other may fail or be classified, as in every category (build note). It is a regression case, not a validation
    of timing in general; that stays in the residual.
  - The minimum stays 10 of 12.
- **DB tests (synthetic), added to item 6.** Each case carries a stated expected outcome:
  - Anniversary, for a window starting 2025-04-01: an end on 2026-03-31 publishes; an end on 2026-04-01 or
    2026-04-02 is NULL.
  - Leap year: a window from 2023-03-01 to 2024-02-29 (366 days) publishes. A window from 2024-02-29 to 2025-02-28
    is NULL; its anniversary is 2025-02-28.
  - NULL starts: a NULL start on the `rn = 4` member, with the other starts present, is NULL; so is a NULL start on
    another member.
  - Bad intervals: an inverted interval is NULL. A one-day overlap is NULL. Cumulative members (Jan 1 to each quarter
    end) are NULL.
  - Short windows: a gap between members, and a window of 334 days, both publish (the stated no-lower-bound decision).
  - A zero total over 368 days publishes zero; the same window with a positive total is NULL.
  - Multiple failures (an inverted interval inside an over-long window) report condition 1.
  - **Layout of each case.**
    - Every case states all four member intervals and amounts, and passes sql/220's shape guard.
    - Every case has a fifth, older quarter, which must not enter the sum.
    - A publishing case asserts the exact sum.
    - Valuation cases carry a published cap, so that no unrelated NULL can make a refusal pass.
  - **Rollback.**
    - Execute sql/508's file, then its rollback file. Compare the view definition, its ordered columns and types, and
      its rows, plus the dependent valuation rows, against sql/507's state.
    - A separate test runs the whole-construction sequence: 508's rollback, then 507's.
  - The end-anchored classifier is an acceptance diagnostic. It is exercised by the acceptance run, not by the view's
    DB tests.

**Finding 2: FAST, reference-data lag, no change.** Ours 1.892%, external 1.882%, ratio 1.005. The acceptance
script stops at `N_f unresolved` (08:30Z log). Our window ends 2026-09-30 (10-Q filed about 2026-10-14), and the local
`companyfacts.zip` (2026-10-17 09:34) does not carry that quarter. The rerun follows `sec_companyfacts_bulk_refresh`.
N_f stays required, because the identity is asserted per row (item 4).

**Codex checkpoint 1, round 1 (UNSOUND, 14 findings), applied in v2.** v3 supersedes the figures it quotes.

| # | finding | action |
|---|---|---|
| 1 | P1: FLXS's span of 0 passes; intervals and continuity unchecked | Condition 1, valid intervals, refuses FLXS. A continuity rule is rejected because the start metadata is noisy (101 gaps); the end-anchored cross-check replaces it. |
| 2 | P2: not the source rule | Stated as a local departure, with its false inclusions and exclusions. |
| 3 | P2: ratios promoted to diagnoses | KO is shown from its members; HNI and TILE are candidates; "sound yields" becomes "within the band". |
| 4 | P2: 30 of 65 explained, conclusion overreaches | Narrowed to the 30. All 116 retained outliers are listed; timing stays in the section 4 residual. |
| 5 | P2: ex-date coverage judged in aggregate | Per instrument: 0 ex-dates inside the window for each of the 45 refused rows. |
| 6 | P2: FY comparator not deterministic or boundary-checked | Normalized FY only, latest filed first; "that FY" means both boundaries equal; provenance printed. |
| 7 | P2: screened by day span, not by guard outcome | The screen and the listing are by guard outcome; every refused row is listed. |
| 8 | P2: claims not reproducible | The script prints every figure here; the hand classification is labelled and limited to the 30. |
| 9 | P2: impact measured on publications only | Measured on all 1,368 TTM values; acceptance compares treatment and prior on every TTM row. |
| 10 | P2: "continuous flows, 1.6%" unsupported | Removed; justified by scope instead. |
| 11 | P2: replacing KO weakens timing coverage | Original and replacement outcomes printed separately; a mandatory 52/53-week boundary slot added. |
| 12 | P2: boundary and NULL fixtures missing | Added to the DB tests; `rn = 4` start stated. |
| 13 | P2: baseline ambiguous | Three arms named: control (original baseline), prior (sql/507), treatment. |
| 14 | P3: recovery asserted | Labelled an expectation, not measured. |

**Codex checkpoint 1, round 2 (UNSOUND; round-1 findings 4, 7, 10, 13 and 14 resolved, the other nine partial; 14 new),
applied in v3.**

| # | finding | action |
|---|---|---|
| 1 | P2: JKP formula misstated | Restated from the Source rule table, with the proxy named separately. |
| 2 | P1: valid intervals can overlap; cumulative periods pass | Condition 2, no overlap, refuses CRM, HUN and WWW; cumulative-member fixture added. |
| 3 | P2: "conservative" claimed as soundness | Recast as a fiscal cash-yield heuristic with measured limits; five genuine distributions inside a window under a year are not refused. |
| 4 | P2: 60-day exception unsupported; snapshot is not enforcement | The 60-day rule is replaced by first-member-length classes; "no predecessor stored" is its own class; the check is stated as a diagnostic, and the guard as the enforcement. |
| 5 | P2: no lower bound rests on a sample | Kept, with the reason stated: no source rule, and a value fitted to the sample is what the finding warns against. The under-count risk is stated as undetermined; fixtures pin the decision. |
| 6 | P2: in-band called "false exclusions" | "In-band ratios, correctness undetermined." |
| 7 | P2: BHE and FLXS zero ex-dates are mechanical | Printed as unassessable; the others as "no stored ex-date", with instrument totals. |
| 8 | P2: NULL end premise | `period_end_date` is `NOT NULL` in the schema (information_schema, 2026-10-20). |
| 9 | P2: end-anchor leap arithmetic | The script uses `e < (e5 + 1) + INTERVAL '1 year'`; leap fixtures carry expected outcomes. |
| 10 | P2: ties; comparator provenance | Member sums equal the view's value on all 1,368 rows, so no tie mismatch occurs today. The dedup is sql/220's and is unchanged. FY provenance is printed for every listed row. |
| 11 | P2: inner joins may drop rows | Driven from the TTM rows with left joins; the count is asserted against a direct count and for uniqueness. |
| 12 | P2: figures not in the log | KO's members are printed; KO's and FAST's external figures cite the acceptance log; the hand classification is labelled. |
| 13 | P2: publication-level increment unchecked | Treatment versus prior, every valuation row and column, NULL-safe; distinct slots; an unavailable replacement counts as unreconciled. |
| 14 | P3: gap count conditioned on no overlap | Any-gap 101, any-overlap 3, both 0. |

**Codex checkpoint 1, round 3, the cap (UNSOUND; round-2 findings 1, 2, 3, 6, 7, 8, 9, 11, 13 and 14 resolved, 4,
10 and 12 partial, 5 unresolved; 10 new).** Applied without a fourth round, as in v4.

| # | finding | action |
|---|---|---|
| 1 | P1: conditions 1–3 do not establish 12-month coverage (one-day members pass) | **Recorded disagreement**, below. |
| 2 | P2: an interval length does not prove the start | The class needs a filed fact whose context starts at the stored start. All 7 kept cases have one. |
| 3 | P2: equal sums do not exclude ties | Ties measured: no window member on a tied dedup key, and no tied FY key. |
| 4 | P2: member-sum check printed, not asserted | Asserted. |
| 5 | P2: a zero total over a long window is still zero | Condition 3 applies to positive totals only. VSTS publishes zero; fixture added. |
| 6 | P2: KO's amounts are not a payment count | "Consistent with one payment more than four"; the envelope is the established fact. |
| 7 | P2: attribution and comparator provenance | Attribution split by source; CRM, HUN and WWW intervals printed; FY ordering stated; FY provenance printed for every comparator. |
| 8 | P3: one-way equality | Exact equality to the independent expected result; key sets equal and unique. |
| 9 | P2: fixtures under-specified | Every case states four intervals, amounts, a fifth quarter and exact sums; precedence fixture added. The end-anchor classifier is an acceptance diagnostic, not a view test. |
| 10 | P3: rollback test underspecified | The rollback file is executed and compared, and the whole-construction sequence has its own test. |

**Recorded disagreement (round 3, finding 1).** Codex holds that the Source rule's "last 12 months" requires each
window to be shown to cover twelve months, and that four one-day members pass all three conditions.
- **The amendment does not adopt a coverage rule.** The window is sql/220's four latest quarters, which v4 accepted
  through three rounds. This amendment removes one measured failure, a window longer than a calendar year, without
  re-deciding the window.
- **Coverage cannot be read from the stored metadata.**
  - The start dates are noisy: 101 publications have gaps of up to 30 days, and KO's derived Q4 is stamped 10-01.
  - A coverage rule would refuse on that noise. A day minimum would be fitted to this snapshot.
  - The one-day counterexample needs four stored intervals of a day. Today, 20 positive publications are under 364
    days, the shortest 334 days.
- **Ownership.** Coverage moves with the start-date metadata, which is the normaliser ticket's (#3916, "quarter
  start-date metadata"). The view's coverage rule is decided there.

**Amendment 2, build note (2026-10-20 ~09:30Z, dry runs of the acceptance script; no ckpt-1 round remained).**
- **The boundary category holds two names.** The single boundary slot drew NVDA, which failed.
  - Ours 0.129%, external 0.240%, a ratio of 0.537.
  - NVDA raised its dividend from $0.01 to $0.25 (ex-dates 2026-06-04 and 2026-09-10), and our window ends at its
    last filed quarter, 2026-07-26.
  - Item 4 keeps dividend changes in the residual, so one mandatory name fails on any recent change, whatever the
    boundary does. Two names with one reconciled is how every other category is covered. The selection rule (largest
    cap) is unchanged; the second name is COST.
- **A filing-lag classification**, on Amendment 1's pattern: classified, never reconciled, never counted.
  - E_w is the page's amounts with an ex-date in the 365 days to the window's own last end. It is computed only where
    the stored list covers that span.
  - A miss is classified when E_w ≠ E and ours × E ÷ E_w (count factor divided out where valid) lies within ±15%.
    NVDA: E_w $0.28 against E $0.52, residual 0.993.
  - It is self-limiting: with no dividend change E_w = E, and nothing is classified.
  - It records a property of every filings-based form, the control's included: a yield lags a dividend change by up
    to a quarter plus the filing delay.
- **N_f for a 12/12/12/16-week filer.** COST files a 52-week FY and a 36-week (252-day) year-to-date context, and no
  16-week one. The fiscal-Q4 rule's nine-month leg (260–285 days) misses it. The dividend acceptance widens that leg
  to 240–290 days, duration-weighted as before. The P/E acceptance's resolver is untouched.
- Dry run 09:40Z: 11 of 12 reconciled. COST reconciles (ratio 1.002). KO's replacement XOM reconciles (1.018).
  RESULT PASS.

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
