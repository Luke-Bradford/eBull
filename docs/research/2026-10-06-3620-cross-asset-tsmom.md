# #3620 — cross-asset trend (12-month TSMOM, long-or-cash, monthly) on the corpus ETF set

Status: spec, revised after Codex checkpoint 1 rounds 1–3 (§"Checkpoint log"). Slice 1 builds the pure rules and
a census on real data. The census inspects coverage, verdicts and distribution stamps only. It computes no signal,
slot, holding, return statistic or cross-source comparison, and prints no return value. No signal, holding or return
path is computed on real data until a declaration exists (§"Registration"). The declaration waits on the evidence-bar
decision posted on #3609 on 2026-10-06 02:37Z, which governs every study whose result could reach demo.
Programme: `docs/research/2026-10-04-strategy-research-sweep.md` §4 item 3. The baselines are defined in #3609 step 0
(`docs/research/2026-10-04-3609-step0-baselines.md`); this study reuses their weights and cost rules.

**Status of every result:** a **retrospective synthetic reference on a corpus-defined universe**. It is not a
deployable, execution or historical-implementability claim.
- Strategy capital is limited to US-listed eToro Stocks (`docs/settled-decisions.md`, universe contract). US ETFs
  may be CFD-only for UK clients (`quant/strategy-menu.md`). Deployment needs a per-instrument real-asset check or a
  recorded decision.
- From 2022 most funds' returns are NAV-based, and SPY and QQQ read proxy funds. Decisions taken on those inputs are
  **retrospective proxy decisions**: N-PORT values were not public at the decision date (§"Returns").
- Net figures deduct **modelled spreads only**; every figure is labelled by its cost scenario (gross, net, 2×).
  Costs are provisional stock-band assumptions applied to ETFs.
  Figures are pre-withholding, with fractional holdings and no minimum ticket. Live-like distribution accounting
  (`research-process.md` §Returns) is not modelled. That is this reference study's stated exception, and the
  declaration must keep it visible.

## Question

Does a monthly long-or-cash 12-month time-series-momentum book differ from passive and randomised versions of itself
(§"Controls")? It trades a fixed cross-asset ETF set and is measured net of modelled costs. How does it compare with
SPY, 60/40 and the Swensen mix over the same months?

The menu's prior (`quant/strategy-menu.md`) is that the gain is mostly drawdown reduction and diversification.
Hurst, Ooi & Pedersen simulate leveraged long/short futures. Their record and equity correlation do not transfer to an
unleveraged long-or-cash ETF book, so this backtest is the evidence, not a confirmation.

## Source rules and every deviation

The published rule is Moskowitz, Ooi & Pedersen 2012 (MOP), *Time series momentum*, JFE 104:
- **§2.1:** excess-return indices are built by compounding daily futures excess returns.
- **§2.4, eq. (1):** ex-ante volatility σ comes from daily returns, exponentially weighted with a centre of mass of
  60 days, and is annualised.
- **§3:** the signal is the sign of the past 12-month excess return, with lookback k = 12 months and holding period
  h = 1 month.
- **§4.1:** each position is sized at 40% / σ, signed by the signal, and averaged across available instruments.

| treatment | this study | deviation from MOP and why |
|---|---|---|
| signal | `s_t = Π_{m=t−11..t}(1 + r_m) − Π_{m=t−11..t}(1 + RF_m)`; hold iff `s_t > 0` | **Aggregation and basis.** MOP compound daily futures excess returns. This compares separately compounded monthly ETF total return and RF wealth. That is an ETF adaptation, and the two are not identical in general |
| short side | a zero or negative signal makes the slot cash | No shorts and no leverage (issue rule; settled no-leverage mandate) |
| sizing | slot `wᵢ = (1/σᵢ) / Σⱼ(1/σⱼ)` over **all** eligible funds, whatever their signal | Keeps MOP's inverse-volatility **functional form**. It does not keep MOP's relative positions, because the estimator differs, or MOP's 40% target, which needs leverage |
| volatility | `σᵢ`: sample standard deviation (n − 1) of the same 12 monthly total returns × √12 | It changes **frequency** (monthly, not daily), **weighting** (equal, not exponential), **horizon** (12 months, not a ~60-day centre of mass) and **basis** (the monthly panel). No single daily total-return basis exists: Intrader ETF `adj_close` loses distributions from 2022, and N-PORT is monthly. This is a constrained adaptation, not a validated substitute. It uses only 12 observations, and its estimation error relative to MOP's estimator has not been measured |
| timing | primary arm: decide on data through t−1, fill at t's close, so a signal through t first earns month t+2 | **One skipped month** relative to MOP, whose signal earns the immediately following month. The same-close arm keeps MOP's alignment under the market-on-close assumption |
| hurdle | French `RF`, `french_five_factor_monthly`, latest accepted snapshot; `decimal_return` asserted | — |
| cash remuneration | **research convention:** 0% in the primary arm; the sensitivity arm credits cash held over month m with `RF_m` | Hurdle and cash return are different quantities. eToro interest on an uninvested USD balance is not verified for the account |
| costs | `cost_model.py` bands, half the round trip per side, band fixed at entry by the raw close | #3609 step 0 §Costs unchanged, including its ETF exception |

**What the slot rule equalises.** At formation, each slot has equal **estimated** standalone volatility if invested
(`wᵢσᵢ` is constant). It does not equalise risk contributions, which would need correlations. Realised risks also
differ, an off slot holds cash, and weights drift until the next formation.

## Returns

**Source.** `etf_total_return_reader.load_etf_total_return_panel(conn, symbols)` (#3619 slice 2b), with price-return
extensions **off**.
- To 2021-12, each fund reads Intrader `adj_close` month-end returns.
- From 2022-01, funds with an N-PORT class read Form N-PORT Item B.5.a monthly NAV total returns. SPY reads IVV and
  QQQ reads QQQM as declared proxies.
- The commodity pools GLD, IAU, SLV and USO stay on Intrader through 2024-08.

**Decision-time availability.** At month-end t a trader could observe the market total return. To 2021-12 the panel
is that quantity; from 2022 it is a NAV proxy that was not public at t. Every decision whose 12-month window contains
a 2022+ month is therefore a retrospective proxy decision. The report prints each decision window's source
composition, months by source, so the transition formations (2022-01 through 2022-11) are identified.

**Proxy fidelity (slice 3, behind the gate).** This is an input diagnostic, run inside the declared run before any
outcome, because it computes signals.
- **Population:** the 30 strategy funds (not comparator-only VTI) that have an N-PORT class. Pairs are obtained
  through the reader's own selection: Intrader month-end returns (`total_return_reader.load_month_ends` /
  `monthly_returns`), and N-PORT months from the reader's class resolution and `resolve_nport_months`. No second
  N-PORT selection policy is written.
- **Months:** overlap months up to 2021-05.
- **Per fund it prints:** the paired count, median, 95th percentile and maximum absolute monthly difference, and
  the number of **12-consecutive-paired-month** windows. For each such window it gives the signal and the slot
  weight under each basis, slots computed over the funds that have a window ending in the same month.
- **Summary:** the signal-disagreement count, and the median and maximum absolute slot-weight difference.
- **Thin data:** no window prints "no window"; no common population prints "no common population".

This is descriptive: there is no acceptance threshold. It covers the audited period only and does not validate the
post-2021 NAV inputs.

**Distribution capture is unverified (slice 1 census).** Intrader's ETF distribution stamps have gaps. Measured
2026-10-06: SHY has 9 in 2019, against 12 in every other full year 2005–2023, and DBC has none in 2022 (#3676).
- **Event:** a `research_price_daily` row of the fund's Intrader series with `dividend` non-zero. Its date is that
  row's `bar_date`, the ex-date as stored.
- **Years:** every year from the fund's first Intrader bar to the last Intrader month the panel uses. A **full**
  year has bars in both its January and its December; any other year is **partial** and is printed separately, never
  flagged.
- **Reference:** the mode of the full-year counts. Fewer than 3 full years prints "insufficient history"; a tied mode
  prints "multimodal" and flags nothing.
- **Flag:** a full year whose count differs from the mode.

This is a screen, not validation: no independent distribution dates or amounts are checked. Results carry unverified
distribution capture, and the declaration must record that as an input-quality exception.

**DBC is excluded.** Its corpus total return is known to be incomplete (#3676), and no reconstruction source is
wired. The exclusion is made on input quality, before any outcome. Commodity exposure comes through USO, GLD, IAU and
SLV.

**Month-end anchors.** A panel month is a calendar month. An Intrader month-end is the last bar the reader assigns to
that month; it must fall within 7 calendar days of the calendar month-end (step 0's `B3_STALE_DAYS`), or the run
refuses. An N-PORT month is the reporting month as filed and has no bar date. Every execution uses the Intrader raw
close of the month-end bar. A missing, non-positive or non-finite raw close refuses the run; it is never replaced by
another bar.

## Universe and eligibility

**Funds (30):** #3620's 31 minus DBC.

**Classes:** the house partition, which is also the primary segmentation. The six sets are disjoint and their union
is the 30 funds (asserted in code):
- US equity: SPY, QQQ, IWM, XLB, XLC, XLE, XLF, XLI, XLK, XLP, XLU, XLV, XLY;
- non-US equity: EFA, EEM, VGK, EWJ;
- Treasuries: TLT, IEF, SHY;
- credit and inflation-linked: AGG, LQD, HYG, TIP;
- commodities and precious metals: GLD, IAU, SLV, USO;
- real estate: VNQ, XLRE.

No published TSMOM rule groups ETFs this way.

- **Inception eligibility.** A fund's history starts at its **first panel month** f. Months before f are
  pre-history: the fund is not yet eligible, and nothing refuses. The fund is eligible at formation t when t ≥ f + 11,
  so its 12 lookback months are all at or after f. Every month from f to E must be present and valid, or the run refuses
  (§"Validity").
  - Identity is the reader's: the Intrader symbol, plus the N-PORT identity gate where N-PORT is used.
  - The census prints each fund's first corpus bar beside its first eligible formation.
  - Historical existence and identity are corpus-defined and are not independently verified.
- **Start.** The start is the first month-end at which each of the six classes has at least one eligible fund. It is
  computed, never hand-set. On the 2026-10-06 panel, commodities qualify last (GLD), giving 2005-11. The census prints
  each class's eligible constituents at the start and each December.
- **End E.** E = min(2024-08, the earliest last panel month across the 30 funds and the comparators).
  - 2024-08 is the chosen coverage cap. It keeps every execution price on Intrader and every pool on its accepted
    Intrader months. It certifies nothing about ETF archive completeness: these funds sit outside the stock reader's
    `survivorship_free` selection.
  - E is fixed in the register entry before any outcome; a later E needs a new entry.
  - Source rows after E are untouched; only the evaluation path stops.
- **Refusals.** The census prints a status per fund and refuses the run on each of these:
  - no panel rows;
  - first eligible formation after E − 2;
  - a missing month between the fund's first panel month and E (comparators: between the start and E);
  - a class that never qualifies;
  - **E < S + 2**, where S is the start. The lagged arm needs at least one held month.
- **Overlap is deliberate.** GLD and IAU both hold gold, and the equity funds overlap. Per-class invested share is
  printed.
- **Survivor-conditioned fund selection.** The list is today's familiar funds. No corpus field identifies terminated
  ETFs, and our SEC fund directory is current, not historical. So no closed or merged fund is added. The study does
  not meet the survivorship-free element of the 2026-08-23 bar, and the declaration carries that label.

The census (`PYTHONPATH=. uv run python -m scripts.report_3620_tsmom --census`) covers the 30 funds and the
comparators (SPY, AGG, VTI, EFA, EEM, VNQ, IEF, TIP). It prints the verdicts, refusal statuses, first corpus bar, first
and last panel months, first eligible formation, the start, E and its limiting fund(s), and the stamp audit. Slice 1
adds no other entry point on real data.

**Hold-out inspection, stated.** To decide coverage and E, the census loads every panel row through E, W2 months
included. It checks their presence, source and validity, and prints no return value or statistic. That is input
inspection, not outcome access. The slice 3 run records the census run (timestamp and git SHA) beside its
`strategy_holdout_accesses` rows, so the history shows it.

## Validity

Three cases are kept apart:
1. **Pre-history** (before a fund's first panel month): ineligibility, never a refusal.
2. **A missing month** between a fund's first panel month and E: refusal.
3. **An existing invalid value:** refusal.

A value is invalid when it is non-finite. Domains differ:
- **Fund returns and RF**, which are compounded as wealth, must also be > −100%.
- **Factor and AQR observations** are long/short or excess returns. They need only be finite, with unit
  `decimal_return` asserted.
- **Execution closes** must be positive.

Validity applies to every consumed value: lookback months, held months, comparator months, C4's prefix, RF months,
execution closes, and the factor and AQR observations used. A volatility for an eligible fund that is ≤ 0 or
non-finite refuses; it is checked after eligibility. Weights must be finite and non-negative, and must sum to 1 with
cash to within 1e-12. No cap or floor is applied to any slot.

## Path

Valuation and return attribution are monthly. Daily bars supply only the month-end execution anchors and the census
metadata. Each arm runs one continuous self-financing path with fractional units, no leverage and no negative cash.
The path is written in `app/services/tsmom_etf.py` rather than reusing step 0's `simulate`, which charges costs at
pre-cost targets, has no cash slot, and is part of step 0's recorded construction.

- **Initialisation and reported months.** Capital is 1.0, in cash, at S's close **before** any trade. The reported
  months are S+1..E for every arm and both timings (225 on the current panel).
  - `R_{S+1} = V_{S+1} / 1.0 − 1` and `R_m = V_m / V_{m−1} − 1` for m > S+1. Each V_m is the post-trade NAV at m's
    close, or the post-liquidation NAV at E.
  - So a fill's cost at m's close sits in month m's return. The same-close arm's opening cost at S's close is carried
    into R_{S+1}.
  - The lagged arm holds cash through S+1, buys at S+1's close, and first earns risky returns in S+2.
  - Asserted: `Π(1 + R_m) = V_E` (post-liquidation) to within 1e-12.

- **Timing, primary ("lagged").** Decisions use data through month-end t−1 and fill at month-end t's close. The old
  book earns month t; the new book starts earning in month t+1. The first decision uses the start month's data and
  fills one month later.
- **Timing, idealised ("same-close").** Decisions use data through t and fill at t's close: the market-on-close
  assumption.
- **Sizing.** Both arms apply target weights to the post-cost NAV at the fill close. That presumes a closing order
  sized in weights. It is an idealised sizing assumption, stated for both arms.
- **Cost solve.** At a fill, with pre-trade risky values vᵢ, cash c, NAV V = Σvᵢ + c and targets wᵢ, the cost is
  `C = Σᵢ hᵢ · |wᵢ(V − C) − vᵢ|`. hᵢ is the half-spread of the position's band, or of the entry band for a new
  position. The solve works on V-normalised values:
  - iterate `C_{k+1} = f(C_k)` from `C_0 = 0`;
  - the map contracts with factor ≤ maxᵢ hᵢ < 1;
  - stop when `|C_{k+1} − C_k| ≤ 1e-14` (relative to V);
  - refuse after 100 iterations;
  - assert afterwards that the charged cost equals Σ h·|traded| to within 1e-12.
- **Position lifecycle.** A zero target sells the position in full and clears its band. A re-entry takes a fresh
  band from its fill raw close. A held position keeps its band through adds and trims. Cash is not a position.
- **Cash.** Cash earns 0% (primary) or `RF_m` over each held month m (sensitivity). It carries no cost.
- **End.** The path liquidates at E's close in both arms, with the sale charged. In the lagged arm, the decision taken
  at E−1 is not filled; the liquidation replaces it. Sub-windows slice the continuous path, with no invented trades.
- **Turnover.** Turnover counts risky traded notional only: (buys + sells) / 2 over pre-trade NAV, per calendar year.
  It excludes cash movements, and exactly two events per book: the **book's first fill**, even if that fill buys
  nothing, and the liquidation at E. Both excluded events are still charged. Every later entry counts, including a
  newly eligible fund's first purchase.
- **Trade size (diagnostic).** For a $10,000 starting sleeve, the report prints the distribution of trade notionals
  and the count below eToro's $10 minimum. Sub-$10 trades execute in the model and pay the ordinary spread. No
  minimum-ticket fee or rejection is modelled.

## Controls and baselines

Every control and baseline uses the same months, path engine, costs, timing arm and cash arm.

- **Evaluated formations.** These are the formations whose fills produce at least one held month: in the lagged arm,
  the start through E−2; in the same-close arm, the start through E−1. Controls are defined over exactly this set.
- **C1 — always invested.** Every eligible fund is held at its slot every month. C1 shows the total effect of the
  timing overlay, not timing skill alone.
- **C1x — notional-exposure-matched.** Weights are `wᵢ · e_t` for every eligible fund, with cash `1 − e_t`. e_t is
  TSMOM's invested share at formation t. C1x is conditional on TSMOM's exposure schedule, and risk can still differ at
  equal notional.
- **C2 — timing surrogate (retrospective diagnostic).**
  - For each fund, its on/off sequence over the evaluated formations where it is eligible (n of them) is circularly
    shifted by an offset drawn uniformly from 1..n−1. A fund with n ≤ 1 is not shifted and is counted as degenerate.
  - The shift keeps each fund's on-count exactly and keeps run lengths except across the wrap. It breaks alignment
    with returns, except where the sequence is unchanged by the shift; those cases are counted. It breaks cross-fund
    co-movement as well.
  - The report prints C2's achieved invested-share difference against TSMOM, per fund and in aggregate, and its
    turnover change.
  - **Joint dependence:** per formation, the share of eligible fund pairs that are both on, for TSMOM and the C2
    draws' mean. Conclusions are limited to this surrogate distribution.
- **C3 — random basket from the same universe (the evidence bar's control).**
  - At each evaluated formation, draw kₜ funds uniformly without replacement from the eligible set, where kₜ is
    TSMOM's held count. Each selected fund takes its full-universe slot wᵢ; unselected slots are cash.
  - The report prints C3's achieved invested-share difference, so a result is not attributed to selection alone.
- **Samplers (exact):**
  - Draw ids run 0..999. Book is `all` or a class name; timing is `lagged` or `same_close`.
  - **C2:** `rng = random.Random(f"3620:C2:{timing}:{book}:{draw}")`. For each fund in symbol order: if n ≤ 1, offset
    = 0 and **no RNG call** is made; else `offset = rng.randrange(1, n)`. Then `shifted[i] = seq[(i − offset) % n]`.
  - **C3:** `rng = random.Random(f"3620:C3:{timing}:{book}:{draw}")`. At each evaluated formation in ascending order:
    `chosen = rng.sample(sorted(eligible_symbols), k_t)`.
  - Offsets and baskets are persisted. The same draws serve every cost and cash scenario.
- **C4 — historical-mean diagnostic** (Huang, Li, Wang & Zhou 2020, eq. 17 and Table 9).
  - **Rule:** hold iff `Σ(r_m − RF_m) ≥ 0`, summed over the fund's contiguous months from its first panel month to the
    decision month. "Non-negative" follows Huang et al.'s Table 9 boundary, unlike TSMOM's strict `> 0`.
  - Same slots and the same 12-month eligibility as TSMOM.
  - **Reading:** an alternative allocation diagnostic, not a clean isolation of horizon. C4 differs from TSMOM in
    aggregation (an arithmetic sum, not compounded wealth), horizon and boundary, and both keep 12-month volatility
    sizing. The report prints the source composition of each C4 prefix.
- **Baselines.** B1 is SPY, B2a is 60/40 (SPY/AGG) and B2b is Swensen (VTI, EFA, EEM, VNQ, IEF, TIP), with step 0's
  weights. Each is bought at the first fill and rebalanced to target at each December fill. Step 0's own paths start
  in 2010 and are not sliced.
- **Class sub-books.** Each class is simulated as a **standalone counterfactual book**: its eligible slots are
  renormalised over all of that class's eligible funds, off slots are cash, and it starts with its own NAV 1.0 and
  its own costs and lifecycle. Each class gets the matching C1 and C3 (k = the sub-book's held count). A class with no
  eligible fund at a formation is all cash.
- **Percentiles.** C2 and C3 report the 5th, 50th and 95th percentiles of each statistic, nearest-rank.
  - **Valid draws only.** A draw whose statistic is undefined is excluded. The valid count is printed, and zero valid
    draws prints "n/a".
  - Medians of different statistics need not come from one draw.
  - **TSMOM's position** within the valid draws is `(count below + ½ · count equal) / valid draws`. It is a numerical
    rank, printed with the statistic's favourable direction: higher is favourable for return, lower for volatility,
    drawdown, turnover and cost.
  - Drawdown is reported as a positive loss fraction.

## Windows

- **W1:** S+1 to min(2021-05, E), before the house `HOLDOUT_BOUNDARY` (2021-06-29).
- **W2a:** 2021-06 to 2021-12. **W2b:** 2022-01 to E. Both are reused validation, split at the source transition.
  W2b's first eleven formations mix sources. W2 = W2a ∪ W2b is supplementary.
- **Episodes:** calendar 2008, 2020-02 to 2020-04 and calendar 2022.
- **Full window:** S+1..E, supplementary.
- **Clipping.** Every window is intersected with the reported months S+1..E, and nothing is read outside them. The
  report prints each window's actual bounds and month count. An empty intersection prints "empty". Length thresholds
  apply after clipping.
- **Statistics by clipped length, the same for whole-book and class outputs:**
  - **1 month or more:** cumulative return and maximum drawdown (descriptive).
  - **12 months or more:** the full statistics.
  - **60 months or more:** the FF5 + momentum regression and the Newey–West t-values. This is an operational
    reporting minimum and does not establish inferential sufficiency.

## Outputs (`scripts/report_3620_tsmom.py`, run path in slice 3)

**Scenario matrix.** Cost {gross, net, 2× stress} × cash {0%, RF} × timing {lagged (primary), same-close}. Each
comparison uses the same scenario on both sides.

For each arm (TSMOM, C1, C1x, C2 and C3 percentiles, C4, B1, B2a, B2b) and each class sub-book, per window, the report
prints:
- annualised net return (geometric), annualised volatility, and maximum drawdown from monthly observations;
- against B1: active return, tracking error, IR, beta and maximum relative drawdown. Sharpe is not printed;
- turnover, cost drag and the cost reconciliation;
- **paired differences**, TSMOM minus each of C1, C1x, C4, B1, B2a and B2b: the monthly mean difference with its
  Newey–West t (lag ⌊4(T/100)^(2/9)⌋), and the differences in annualised return, volatility and maximum drawdown;
- TSMOM's position within C2 and within C3. Inference is exploratory until a declaration names the primary
  comparison;
- the FF5 + momentum regression (step 0's rule), exploratory;
- an **unaligned descriptive comparison with AQR** (`aqr_tsmom_monthly`, latest accepted snapshot, unit
  `decimal_return` asserted):
  - **Comparisons** (correlation and tracking error):
    - TSMOM's monthly excess return `R_m − RF_m` (net, primary scenario) against `TSMOM`;
    - an **equity sub-book**, a standalone book over US equity ∪ non-US equity under the class-sub-book rules,
      against `TSMOM^EQ`;
    - the Treasuries sub-book against `TSMOM^FI`;
    - the commodities sub-book against `TSMOM^CM`.
  - AQR's series are already excess returns, so RF is not subtracted from them.
  - **Join:** by calendar month, common months only. Fewer than 60 common months prints "n<60".
  - **Not covered:** credit and real estate have no AQR leg; FX has no ETF here.
  - AQR's series are leveraged long/short futures with no costs, so this is not the Track B fidelity test. It is
    reported as description.

**Undefined values print "n/a" with the reason:**
- zero variance, which leaves Newey–West t, beta, correlation and IR undefined;
- an all-cash formation, which leaves risky-sleeve concentration undefined. Total risky exposure is still printed as
  0.

**Diagnostics.**
- Per formation: slot weights, actual risky weights (targets and month-end drifted), the largest risky weight, the
  top-3 share of the risky sleeve, invested share, per-class invested share, and the decision window's source
  composition.
- Per fund: in-market fraction.
- Per trade: fund, band, raw price and notional.
- Cost disclosure: `COST_MODEL_ID`, the band values and each band's `sample_size`, the calibration limits and the ETF
  exception.

## Registration

**Nothing is declared or run in slice 1.** The gated run path is built in slice 3, against the declaration that
slice 2 defines. The planning context:

**Why it waits.** The result matters only if it can lead to demo, and the bar for that is the open question on
#3609. Planning arithmetic, using `trial_register.power_check` with one configuration, Track B and 80% power, under an
**IID planning scenario** of 18.75 years (2005-12..2024-08, 225 months; no dependence model yet, and dependence can
move effective information either way): the smallest detectable margin is ≈ 0.5742 IR, and a margin of 0.5 needs 24.7
years. Reproduce:
`PYTHONPATH=. uv run python -c "from app.services.trial_register import *; c=power_check(TrialDesign(EvidenceTrack.ADOPTION,0.575,'x',225/12,'x'),trials=1); print(c.required_years, c.power)"`.
That figure is feasibility, not a margin.

**The declaration (slice 2) must:**
- pick the estimand, the primary comparison and the primary window. The comparison can be against SPY, against
  C1x/C3, or as a complement to the core. The last needs a frozen sleeve allocation, funding asset, combined rebalance
  rule and required improvement;
- pick the track:
  - **Track B** needs a post-publication prior and a fidelity bridge for this unleveraged long-or-cash ETF adaptation;
  - **Track A** needs a sealed, access-audited confirmation sample and its analysis rule. W2 is reused validation and
    is not one, so without new data the exercise stays exploratory;
- set the margin from an economic rationale, then test feasibility under a stated dependence treatment. Bind power to
  the primary window, the reference's uncertainty and the forward-monitoring horizon, with a minimum information
  requirement and the #2500 sequential boundary;
- record the search history: this spec's one primary configuration, the programme-level family selection
  (`research-process.md` PBO/CSCV or a nested hold-out where it applies), and a rule that no arm other than the
  declared primary can be promoted.

**The slice 3 gate must:**
- refuse unless exactly one `TRIAL_REGISTER` entry matches. That entry must be frozen and feasible under
  `power_check`, use the `3620-tsmom-v` trial-id prefix, and name this spec's sha256, the construction-versions digest
  and the **input digest**. The input digest is a canonical sha256 over every consumed input: panel rows with source
  keys and accessions, raw execution closes with bar dates, RF, factor and AQR observations, verdicts and identity
  mappings, the census, E and the class partition;
- **bind the run to the declaration** through structured fields compared exactly: primary claim, primary window,
  primary scenario, track, and the `trials` value that `power_check` uses. Each field gets a refusal fixture;
- **define the canonical form** of every digest: a versioned schema (`3620-canonical-v1`); JSON with sorted keys;
  ISO-8601 dates; floats as `repr`; rows sorted by their natural key, with duplicates refused. Unhashed canonical
  records are persisted beside their digests.
  - **Construction-versions digest:** the code hashes of `tsmom_etf.py`, `report_3620_tsmom.py`,
    `etf_total_return_reader.py`, `total_return_reader.py`, `cost_model.py` and `trial_register.py`, plus
    `ETF_TOTAL_RETURN_VERSION`, `TOTAL_RETURN_SPLICE_VERSION`, the quarantine rule set and `COST_MODEL_ID`.
  - Fixed digest fixtures are tested;
- **append an `evaluation_began` row to the ledger file and fsync it before the first outcome is computed**, outside
  any DB transaction. The row carries the sha256 of the **complete canonical register entry**, the input digest and
  the snapshot's start time. Completion or failure is a separate appended row. On every run, each earlier row's entry
  is checked against its hash, so an altered or vanished past attempt refuses. The ledger is committed to
  `docs/research/3620-ledger.jsonl`;
- **adopt step 0's statistic formulas** (`report_3609_baselines.window_stats`: geometric annualised return,
  n − 1 volatility × √12, monthly drawdown, active = 12·mean, TE = √12·sd, beta by OLS).
  - **Paired-mean Newey–West t:** an OLS intercept on a constant, Bartlett kernel, lag ⌊4(T/100)^(2/9)⌋, with no
    small-sample correction (step 0's `ols_newey_west`).
  - A deterministic expected-value fixture is tested;
- record `strategy_holdout_accesses` for W2a and W2b before reading them;
- persist and hash the artefacts in `var/research/3620/<UTC>/manifest.json`. These are the inputs above, signals,
  volatilities, slots, holdings, trades, monthly returns per arm and scenario, and C2 offsets and C3 baskets with
  draw-level paths. The manifest also records the git SHA (a dirty checkout refuses), the Python and numpy versions,
  the sampler (`random.Random`, version-2 string seeding) and fixed iteration orders (funds by symbol, months
  ascending, draws ascending), all under one repeatable-read snapshot.

## Slices

1. **This change.**
   - `app/services/tsmom_etf.py` holds the pure rules: class partition, eligibility, start, E and refusals, signal,
     volatility, slots, validity, the exact cost solve, the monthly path with lifecycle and both timings, the
     C1/C1x/C2/C3/C4, baseline and class-sub-book weight builders, samplers, percentile and n/a conventions.
   - `scripts/report_3620_tsmom.py` adds `--census` only: coverage, verdicts, refusals, start, E and the stamp audit.
   - Fixture tests cover every rule, each revert-probed. Codex checkpoint 2 runs on the diff.
2. **Declaration,** after the #3609 answer: claim, track, margin, search record and register entry, with checkpoint 1
   on them.
3. **Gated run path, then the declared run** from clean `main`. This slice includes the proxy-fidelity diagnostic and the
   AQR comparison. The verdict, ledger and manifest are posted on #3620.

## Known limits

- Volatility uses 12 monthly returns, not MOP's daily EWMA. The slots keep MOP's functional form, not its positions or
  its volatility target.
- Fund selection is survivor-conditioned, with DBC excluded, overlapping funds and corpus-defined identity and
  inception.
- ETF costs are stock bands (step 0's exception), calibrated on nine summer dates with no stressed regime. Fills are
  at month-end closes, with sizing in weights, no slippage, fees or FX, fractional holdings and the minimum ticket
  ignored.
- From 2022, NAV and proxy returns stand in for market total returns, and those decisions are retrospective.
  Distribution capture is unverified.
- Cash at 0% is a convention. Figures are pre-withholding, with no live-like distribution accounting.
- This is one history. C2 and C3 dispersion is not a confidence interval, and C2 is retrospective.

## Checkpoint log

Raw outputs: `var/research/3620/ckpt1_round{1,2,3}.txt` in the loop worktree.

**Round 1 (52 findings).**
- **Corrections:** 4, 6–8, 15–17, 21–24, 26, 31, 32, 34, 35, 37, 44, 45, 47–52.
- **Accepted limitations** (disclosed, not corrected): 12, 13, 19, 25.
- **Declaration-stage:** 33, 38–43.

**Round 2 (21 open, 28 new).** Round 3 rated 36 of the 49 resolved; the 13 still open are handled under round 3 below.
- **Corrections:**
  - open items 1, 2, 10, 20, 28, 30, 31;
  - new items 53–68, 71, 72, 74–77, 79, 80. The daily bridge was removed, so 57–61 no longer apply.
- **Accepted limitations:** 9, 14, 36.

**Round 3 (13 open, 20 new).**
- **Corrections:**
  - 11, 34, 81, 85: the proxy-fidelity diagnostic (signals) moved behind the slice 3 gate; consecutive-window rule and
    a defined population; census hold-out inspection stated;
  - 29: a joint co-on diagnostic;
  - 39: equity sub-book defined, AQR excess units, join and minimum;
  - 63: E ≥ S + 2;
  - 69, 93: windows clipped, one length ladder;
  - 70: C4 prefix composition;
  - 73: valid-draw rule;
  - 82: initialisation and reported months;
  - 83, 84: pre-history vs gap vs invalid;
  - 86: stamp-audit population;
  - 87: exact samplers;
  - 88, 89: C4 boundary and reading;
  - 90: timing deviation;
  - 91: sub-$10 trades pay spread;
  - 92: turnover exclusions;
  - 94: validity domains;
  - 98–100: wording.
- **Slice 3 contract:** 44, 95, 96, 97, 48. The gate binds structured fields, adopts step 0's formulas, appends and
  fsyncs the ledger before any outcome, and fixes the canonical form with the construction inventory.
- **Accepted limitation:** 5 (distribution capture unverified; the declaration must record it as an exception).
- **Declaration-stage:** 43.
- **Still open, not claimed resolved:** 78. This log replaces the blanket statement; whether its dispositions hold is
  for the next review.
