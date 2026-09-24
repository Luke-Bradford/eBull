# #2901 — quality arm: declaration spec (simulator, identity gate, statistics, verdict, capital)

PR B of `docs/proposals/ta/2026-09-24-2901-quality-arm.md` (the construction spec; "Carried to the declaration
spec", lines 177-203). Refs #2899, #2908, #3362, #2829, #2364. Security: none (research harness; no broker, auth or
order path).

**Status: draft, Codex ckpt-1 pending.** No return, factor spread or census outcome has been read. The construction
artefact is not yet published (PR A = #3377 + #3378, open); nothing here depends on its counts.

## What this spec fixes, and what it produces
It fixes every rule the run applies to the construction artefact: the simulator, the identity gate, the statistics,
the verdict and the capital boundary. Its code (PR B) is a new module `app/services/r6_monthly_trial.py` plus a sealed
runner `scripts/run_2901_quality_trial.py`. The **frozen declaration** is a separate, hashed document written after
PR B merges and before the run. As #2908's did, it pins the artefact manifest, the implementation sha256s and every
input digest (`2026-08-24-r6-exclusion-preregistration.md`, "Frozen evidence identity"). PR C is the one run.

`r6_exclusion_trial.py` is **not edited**. Its sha256 is in #2908's result identity and in #3362's
`termination_identity`. The new module imports its public primitives (`TerminationPolicy`, `PROGRAMME_POLICIES`,
`ZERO_RECOVERY`, `SeriesEvidence`, `load_series_evidence`, `read_price_series`, `binding_policy`, `evidence_sha256`,
`termination_identity`, `HALF_SPREAD`, `WINDOW_END`) and re-implements nothing that module already does.

## Portfolios (all read from the one artefact; identical process, costs and termination)
At each formation D (the 12 Junes 2013–2024), with X(D) the artefact's `x_date`:
- **A(D) arm**: rows with `in_arm` (top GP/A decile of E(D) plus boundary ties).
- **C(D) control, the headline comparator**: all of E(D) (`rung = eligible`). This is the #2908 lesson: the identical
  construction without the signal.
- **C′(D) complete-case diagnostic control**: rows with `executable` true, a recorded SIC that is valid and outside
  6000–6999, `fields.fpi` ≠ `true`, and neither a resolved `Assets` ≤ 0 nor a resolved `StockholdersEquity` < 0. The
  economic screens are applied wherever the artefact resolved them, and incompleteness is not screened: an issuer
  missing a period or a component tag is admitted. `no_companyfacts_entry` and integrity-excluded rows carry no
  recorded SIC and are out, so C′ tests the component-tag condition, not companyfacts presence. C′(D) ⊇ E(D), by
  construction; the runner asserts it.
- **D₀(D)**: E(D) rows with `decile = 0`. Used only by the identity gate.
- **Secondary comparators, reported and never gating:** (a) a literal buy-and-hold of C(2013) from X(2013), never
  rebalanced; (b) `SPY` bought at the X(2013) open and sold at the window-end close.

Weighting is equal (D1). Every portfolio buys equal-dollar targets at the X(D) **adjusted open**. The first formation
X(2013) starts from cash 1.0. Every later formation rebalances to its new targets.

## Simulator (`r6_monthly_trial.simulate_monthly`)
Sessions are the NYSE calendar (`market_calendar`). Three kinds of valuation session occur, and each has one rule:

| session | when | a held symbol with a bar | missing bar, **gap** | missing bar, **terminated** | missing bar, **alive at capture** |
| --- | --- | --- | --- | --- | --- |
| **rebalance** | X(D) open | adjusted open | `policy.gap_fraction` × last close, sold | policy terminal fraction × last close, sold | n/a (before window end) |
| **month-end mark** | the last session of each month (close) | adjusted close | **last close, held (fraction 1 under every policy)** | policy terminal fraction × last close, **realised to cash**, half-spread charged | n/a |
| **final** | `WINDOW_END` 2024-09-27 close | adjusted close, sold | `policy.gap_fraction` × last close, sold | terminal fraction × last close, sold | last close, sold (#3362 rule) |

- **Status** is #3362's evidence-bound rule, unchanged. **Terminated** means stored `last_bar` < session and not
  alive at capture. **Alive at capture** means stored `last_bar` > `INTRADER_CAPTURE_DATE − ALIVE_CUT_DAYS`. Anything
  else is a **gap**. A bar on the session is always an ordinary price, and termination never overrides it.
- **Why a gap holds at a mark but takes the bound at a rebalance.** A month-end mark trades nothing. Pricing a halted
  holding at zero there would book a −100% month and then a recovery, which is a timing artefact of the mark and not
  an outcome. A rebalance and the final sale must trade the holding, so #2908's non-executability bound applies
  there, and it applies identically to every portfolio.
- **Recognition.** A terminated holding is realised at the **first valuation session strictly after its `last_bar`**.
  If `last_bar` is itself a month-end session, it is priced normally there and realised at the next session. The
  realisation is booked in the month containing that session. It enters cash at `fraction × last close × (1 − h)`:
  the recovery is sold and pays the half-spread once, as every exit does. `realised_value` is logged before the
  spread, as in #3362.
- **Cash** earns 0% and is **not reinvested mid-year**. It rejoins at the next X(D) rebalance, identically for every
  portfolio. For the buy-and-hold comparator it stays in cash to the end.
- **Costs.** `h = HALF_SPREAD` (0.725%) on every traded dollar: initial purchase, rebalance trades (solved exactly
  as `_target_value` solves them, so cash is conserved), termination realisations, and the final sale. `SPY` is
  charged the same `h` (one cost basis, no per-name exception); its effect on SPY's total return is ≤ 2h, and that
  bound is reported.
- **Wealth ≤ 0** is unreachable, because fractions lie in [0, 1] and h < 1. The simulator raises if it happens, and
  the run is recorded as `FAIL(simulator_invariant)`.
- **Monthly return** r_m = W(mark_m) / W(mark_{m−1}) − 1. W is cash plus holdings at the mark, after that month's
  costs. W before 2013-07 is the starting cash, 1.0, so July 2013 includes the initial purchase cost. The months are
  2013-07 … 2024-08 (**134 full months**, `month_pairs((2013, 7), (2024, 8))`). The partial month 2024-09 (to the
  09-27 final sale) enters the **total return** and the haircut bars only, never the monthly statistics.
- **Turnover**, one-way, per holding year. It is (rebalance traded notional at X(D), excluding the first purchase)
  plus (the termination realisations in the preceding holding year), all divided by 2 and by the pre-cost wealth at
  X(D). It is reported for A, C and C′. Novy-Marx/Velikov's 50%/month (`strategy-evidence.md`) is reported beside it.
- **Result per (portfolio, policy)**: the monthly series, the total return (net and gross; gross = the same path at
  h = 0), events, realisations (#3362's `Realisation`), `realisation_census`, and turnover.

### Reconciliation tests (fixtures, beyond telescoping)
1. ∏(1 + r_m) over all months, the partial month included, equals 1 + total return (telescoping).
2. At every event, pre-cost wealth = traded-at-target + cost (cash conservation). At every mark,
   W = cash + Σ holdings.
3. Σ over realisations of `realised_value × (1 − h)` equals the cash credited by realisations.
4. With h = 0, no missing bars and one formation, the result equals a hand-computed equal-weight fixture.
5. The gap/terminated/alive-at-capture table: one fixture per cell, under all three programme policies. It includes
   a termination whose `last_bar` is a month-end, a gap across a mark that recovers, a gap across a rebalance, and a
   termination in the partial final month.
6. The same `TerminationPolicy` object is passed to every portfolio in one call. As in #3362,
   `simulate_under_policies`-style validation checks unique labels, that `ZERO_RECOVERY` is present, evidence for
   every priced symbol, and that loaded bars equal the stored bounds.
7. Every A(D) and C′(D) target has a valid bar on `x_date`, which is asserted rather than inferred: the artefact's
   executability says so, and a violation means artefact or mirror drift.

## Identity gate (runs first; arm outcomes stay sealed until it passes)
Source: global-q `prof_monthly_2025/portf_gpa_monthly_2025.csv` in the pinned zip (sha256 `18dfced9…f963c`), header
exactly `year,month,rank_GPA,nstocks,ret_vw`, returns in percent (÷ 100).
- **Ours**: the gross (h = 0) monthly spread r(A) − r(D₀) from `simulate_monthly`. It uses the same marks, rebalance
  and termination treatment as the arm, which is "a spread matching the arm's process". **Reference**: `ret_vw` of
  rank 10 minus rank 1 (VW vs our EW, D1; HXZ NYSE breakpoints vs ours over E(D), D2).
- **Calendar alignment is exact**. Both key sets must equal the 134 months above; a missing, extra or duplicate key in
  either refuses the run. `validate_factor`'s key intersection is **not** used. Its lead/lag computation (the
  positional shift) is used only after the equality assertion, when positions and months coincide.
- **Unit checks**: every reference rank 1 and 10 value is finite with |ret| < 1 after ÷ 100 (a percent/fraction
  confusion fails this). Both series must have non-zero variance, and `_pearson` already raises on zero.
- **Pass**: #2908's frozen rule (correlation ≥ +0.20, OLS beta > 0, |corr| ≥ max(|lag|, |lead|)) under **every**
  programme policy (a conjunct: it can only refuse).
- **What the gate publishes**: months, correlation, beta, lag and lead correlations, and pass/fail per policy. It does
  **not** publish either leg's mean, the alpha or any cumulative figure. Those carry arm information, and a failed
  gate must not leak it into a correction.
- **A failed gate** is a code or data alignment finding, not a market finding. Arm outcomes stay sealed. One
  correction is allowed. It must be a written, hashed correction declaration frozen before the gate re-runs, and it
  counts **+1 trial** in the deflation family. A second failure closes #2901 as `GATE_FAIL` with no arm result opened.

## Statistics
All on the 134 monthly **net active** returns a_m = r_m(A) − r_m(C) (and a′_m = r_m(A) − r_m(C′) for the diagnostic),
per policy.

1. **HAC t (headline).** mean(a) divided by its Newey–West standard error: the Bartlett kernel, autocovariances with
   1/T normalisation, and lag L = ⌊4(T/100)^{2/9}⌋ (the Newey & West 1994 rule of thumb, *Review of Economic Studies*
   61(4):631-653, as implemented by R `sandwich::NeweyWest` "NW1994"). For T = 134 this gives L = 4, which the runner
   computes rather than reads from here. Reference: N(0, 1), bar **t > 3** (Harvey/Liu/Zhu).
2. **Finite-sample check over holding-year cohorts.** Monthly HAC with 134 observations still rests on 12 annual
   rebalance decisions. Holding-year cohorts c are July(D) … June(D + 1), and the 2013–2023 formations give 11
   complete cohorts. The cohort active return is ∏(1 + r(A)) − ∏(1 + r(C)) over the cohort's months. The cohort t is
   mean / (sd / √11), with ddof = 1, referred to **Student t with 10 df**. This is the few-clusters rule: t(G − 1)
   reference with G fixed (Bester, Conley & Hansen 2011, *J. Econometrics* 165(2); Cameron & Miller 2015, *J. Human
   Resources* 50(2), §VI). The bar is the t₁₀ quantile at the **same two-sided tail probability as |z| = 3**, i.e.
   p = 2(1 − Φ(3)). Both p and the quantile are computed at run time: the Student-t CDF for integer ν (Abramowitz &
   Stegun 26.7.3/26.7.4 closed forms) inverted by bisection. Tests pin the published t₁₀ quantiles 2.228 (0.975) and
   3.169 (0.995). The 2024 cohort (July–August, plus the partial September) is reported, never tested. **This is a
   conjunct**: it can only refuse.
3. **Deflated Sharpe** (Bailey & López de Prado 2014, reusing `deflated_sharpe.deflated_sharpe` and its equations;
   the axis is ours and is recorded as `quality-2901-monthly-dsr-v1`, distinct from `c6-deflated-sharpe-v1`, which
   is trade-axis):
   - SR, y3 and y4 come from a_m (per month, never annualised; `trade_moments` on a_m in percent).
   - **T = the HAC effective size** T·γ̂₀/Ω̂, capped at T. It comes from the same estimate as step 1, so dependence is
     not counted twice or ignored.
   - **Trials M = 5 + corrections.** #2908's three arms (dilution, filing-risk, union) plus #2901's headline and
     C′ diagnostic. Each identity-gate or construction correction adds 1. Termination policies and haircut cells are
     not searches: the verdict takes their minimum. Secondary comparators are not searches either.
   - **ρ = 0, so N = M**, declared. #2908's series cover 27 months (2022-07 … 2024-09) against these 134. A
     cross-trial correlation matrix over different windows is not defined, and two same-window series (headline and
     C′, which share the arm) cannot stand for a five-trial family. ρ = 0 is the largest N available for a given M,
     and so the highest SR₀.
   - **V[{SR_n}] = max(1/(T − 1), the ddof = 1 variance of the two same-window measured SRs).** The first term is the
     sampling variance of an SR estimate under H0 (SR = 0 with normal moments), which is equation (2)'s own
     denominator at SR = 0. It is a floor, so a near-identical pair of measured trials cannot collapse SR₀ toward
     zero.
   - Bar: **DSR > 0.95**, strict (`DSR_PROMOTION_POLICY_VERSION = "c6-dsr-threshold-0.95-v1"`, #2364). A DSR of
     `None` (any of the module's four states) is a **refusal, never a pass**.
4. **Haircut bars** (#2908's rule, restated against the control). For each policy and d ∈ {0.15, 0.58}, on total
   returns over the whole window:
   - edge = A_gross − C_gross;
   - adjusted = C_gross + (edge > 0 ? edge·(1 − d) : edge) − (A_gross − A_net);
   - this is `haircut_net_return(strategy_gross=A_gross, strategy_net=A_net, buy_hold_gross=C_gross, haircut=d)`;
   - **pass(d) ⇔ adjusted > 0 and adjusted > C_net**.

### Composition with termination policies
Every statistic is computed under each of `PROGRAMME_POLICIES` (zero_recovery governs; classified_worst and
classified_best). For each bar, the **binding value is the least favourable across the policies**: the minimum t, the
minimum cohort t, the minimum DSR, and pass(d) under all policies. `binding_policy` reports which policy bound. A
policy can therefore only turn a pass into a fail. The gate is evaluated the same way.

## Verdict (evaluated in this order; the first matching line is the verdict)
1. `GATE_FAIL`: the identity gate fails after at most one correction. No arm figure is published.
2. `FAIL(simulator_invariant)`: any raise inside the run after the gate passed. The run's partial output is kept as
   evidence and is not re-run without a correction declaration (+1 trial).
3. `PASS_ROBUST` requires all of the following under the binding policy:
   - HAC t > 3;
   - cohort t > the t₁₀ bar;
   - DSR > 0.95;
   - pass(0.58);
   - the **complete-case diagnostic**: mean(a′) > 0, and the haircut rule at 0.58 **against C′** (C′ in place of C
     above).
4. `PASS_CONTINGENT`: every item in line 3 except the 0.58 cells, which pass at 0.15 only, for the headline or the
   diagnostic or both. **No capital.**
5. Otherwise the result is **not a pass**, and it is labelled one of three ways:
   - `UNDERPERFORMS_CONTROL` if the binding HAC t < −3;
   - `FAIL_CONTROL` if mean(a) ≤ 0 under the binding policy;
   - otherwise `UNDETERMINED_AT_THIS_POWER`. It is never labelled "no edge".

**Complete-case diagnostic, its purpose and its consequence.** E(D) admits only issuers that tagged all four
components. If that selection handicaps the control (for example by keeping distressed late filers), the arm can beat
C(D) while not beating the executable, non-financial population a live sleeve could simply hold. The diagnostic
cannot remove a collider inside E(D). It bounds the one consequence that matters for capital: **an arm that does not
beat C′ gets no capital**. Line 3 makes it a conjunct, and it cannot rescue a failed headline. The
`GrossProfit`-where-COGS-is-missing sensitivity named as an example in the construction spec is **not run**: it would
need a construction amendment after the freeze (the artefact records `gp_tag` as a status, never as an operand), and
it would add a trial to the family while only being able to downgrade.

**Descriptive readouts, outcome-free and never gating.** Per holding-year cohort (all 12); the 2013–2018 vs 2019–2024
cohorts (the coverage shift in the construction spec, split on formation year only); SPY trailing 12-month total
return sign at D (known at D); the realisation census by status × class per portfolio; the literal buy-and-hold; SPY.
They are never used to choose a sub-period or rescue a verdict.

## Power, stated before the look (not a verdict prediction)
`uv run python -m scripts.measure_2901_power --zip <prof_monthly_2025.zip> --sha256 18dfced977d9412e2e6ec0582646f0c61b596db3a4ac88dbcdd9dae53fef963c`
(re-run 2026-09-25; reads pre-window months only) prints the proxy tracking error and the annual excess needed for
50% / 80% power at t > 3 over 134 months. The declaration copies that output verbatim, and the run re-executes the
script and refuses on a mismatch. The limits of the proxy: it is IID, value-weight decile 10 against the decile
average, 1967-07 … 2013-06, and from the 2025 release. This book is equal-weight against an E(D) control, with
unknown TE and dependence. The expected readout at this power is **low**. `UNDETERMINED_AT_THIS_POWER` is the modal
outcome and is published as such.

## Minimum notional and broker reachability, "at formation"
Neither is observable point-in-time. The broker's `minPositionAmount` (`etoro_broker.py`, `min_position_amount`) and
its instrument list are current values, and the historical run cannot apply them. The run **reports** the per-name
notional that a £50,000 sleeve implies at each formation (£50,000 / |A(D)|) and gates nothing on it. Both checks
bind at the **live** formation, below.

## Capital boundary (restating #2908's, against this arm's control)
- ISA allocation is **£0** (#2915). The sleeve maximum is **£50,000**, ordinary account, long/x1, USD. There is one
  FX conversion each way, with the 0.70% sensitivity reported.
- `PASS_ROBUST` opens **only a paper-deployment ticket**, not capital. That ticket requires:
  - a fresh-universe rebuild at the live June formation;
  - ≥ 95% of target weight mapping uniquely to currently validated, BUY-enabled eToro instruments;
  - every target at or above its broker `min_position_amount` at the declared weights;
  - paper reconciliation through the #2844 sandbox, the execution guard and the kill switch.
  Any operational failure leaves an evidence-backed **£0 sleeve**. `PASS_CONTINGENT` and every non-pass get £0.
- **Turn-off rules** (before the next order), if ever deployed: PIT or hash failure; mapping drift below 95%; a
  kill-switch or execution-guard refusal; a missed annual rebuild; a realised spread worse than h; or a rolling
  preregistered live readout that loses positive-absolute status **or loses to its own live C(D) control**.
  Underperformance alone between annual decisions does not authorise discretionary abandonment.
- #2908's "quality-overlap gate" (pointing at this ticket) is **moot**: #2908 failed its own control and has no
  survivor to overlap.

## Declared residuals and deviations
The declaration repeats R1–R9, D1–D8 and the PIT-registry statement of the construction spec (lines 205-218)
verbatim. This spec adds:
- **R10**: gaps at month-end marks are held at the last close. Monthly marks therefore understate the volatility of
  halted names, identically for every portfolio.
- **R11**: recoveries sit in cash until the next X(D), identically for every portfolio.
- **R12**: `SPY` is charged the programme h.
- **R13**: ρ = 0 and the V floor in the DSR are constructions, not measurements.
- **R14**: minimum notional and broker reachability are unobservable historically, and are enforced only live.

## Delivery
- **This PR**: this spec.
- **PR B**: `app/services/r6_monthly_trial.py` (the simulator, gate, statistics and verdict, all pure), its fixtures
  (reconciliation tests 1–7, the gate's alignment and unit refusals, the HAC and cohort-t reference values, the DSR
  `None` refusal, the verdict table with one fixture per line), and `scripts/run_2901_quality_trial.py`. The runner
  refuses to open any arm figure before the gate passes, and records a holdout-access row before loading prices, as
  #2908's final governance correction did.
- **Then**: the frozen declaration (hashes; #2829 register entry), then **PR C**: one run, the result document, and
  #2901 closed.

Rung: judgement artefact (spec), so Codex ckpt-1 before merge.
