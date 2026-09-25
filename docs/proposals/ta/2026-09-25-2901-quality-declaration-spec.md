# #2901 — quality arm: declaration spec (simulator, identity gate, statistics, verdict, capital)

PR B of `docs/proposals/ta/2026-09-24-2901-quality-arm.md` (the construction spec; "Carried to the declaration
spec", lines 177-207). Refs #2899, #2908, #3362, #2829, #2364. Security: none (research harness; no broker, auth or
order path).

**Status: ckpt-1 rounds 1 (60 findings) and 2 (57) folded in, 2026-09-25; round 3 pending.** No return, factor
spread or census outcome has been read. The construction artefact is not yet published (PR A2 = #3378, open);
nothing here depends on its counts.

## What this spec fixes, and what it produces
It fixes every rule the run applies to the construction artefact: the simulator, the identity gate, the statistics,
the verdict and the capital boundary. Its code (PR B) is a new module `app/services/r6_monthly_trial.py` plus a sealed
runner `scripts/run_2901_quality_trial.py`. The **frozen declaration** is a separate, hashed document written after
PR B merges and before the run. As #2908's did, it pins the artefact manifest, the implementation sha256s and every
input digest (`2026-08-24-r6-exclusion-preregistration.md`, "Frozen evidence identity"). PR C is the one run.

`r6_exclusion_trial.py` is **not edited**. Its sha256 is in #2908's result identity and in #3362's
`termination_identity`. The new module imports from it rather than re-implementing: `TerminationPolicy`,
`PROGRAMME_POLICIES`, `ZERO_RECOVERY`, `SeriesEvidence`, `load_series_evidence`, `read_price_series`,
`binding_policy`, `evidence_sha256`, `termination_identity`, `month_pairs`, `HALF_SPREAD`, `WINDOW_END`,
`haircut_net_return`, `_pearson`, and the private solvers `_target_value` and `_holding_value`, whose bytes the
module's pinned sha256 already freezes. What is new: month-end marks, realisation into cash between rebalances, and
the monthly series.

**Policies.** Only `PROGRAMME_POLICIES` are used, and the runner asserts each has `terminal_fractions` (so
`_holding_value`'s legacy `unsplit` branch never runs) and that `termination_identity` equals the value #3362
recorded, which pins every policy's recovery matrix by hash rather than by numbers restated here.

**Parity with the frozen simulator (terminal wealth only).** Holding a gap at a mark trades nothing, and realising a
termination at a mark rather than at the next rebalance moves the same `fraction × last close` through the same single
half-spread: at that rebalance `pre_cost` is lower by `h·v` and `traded` by `v`, so `_target_value` returns the same
target. Hence the monthly simulator's **terminal wealth equals `simulate_portfolio`'s** on the same schedule and
policy. The runner asserts, for A, C, C′ and D₀ under every programme policy and before any statistic, that
`simulate_portfolio`'s event dates equal the artefact's `x_date`s, and that the terminal wealths agree within
rel. 1e-9 **and** abs. 1e-12. Parity cannot see *when* a return was booked. The monthly timing is pinned by the
fixtures (test 8). A mismatch is `REFUSED_PRE_GATE`.

## Portfolios (all read from the one artefact; identical process, costs and termination)
At each formation D (the 12 Junes 2013–2024), with X(D) the artefact's `x_date`:
- **A(D) arm**: rows with `in_arm` (top GP/A decile of E(D) plus boundary ties).
- **C(D) control, the headline comparator**: all of E(D) (`rung = eligible`). This is the #2908 lesson: the identical
  construction without the signal.
- **C′(D) complete-case diagnostic control**: rows with `executable` true, a recorded SIC that is valid and outside
  6000–6999, `fields.fpi` not `true`, and neither a resolved `Assets` ≤ 0 nor a resolved `StockholdersEquity` < 0.
  The screens bind only where the artefact resolved them; an issuer missing a period or a component tag is admitted,
  and `fpi = not_evaluated` is admitted. `no_companyfacts_entry` and integrity-excluded rows carry no recorded SIC and
  are out.
  - **What C′ is:** the executable, non-financial, **not-known-FPI** issuers with a companyfacts entry and a recorded
    SIC, without the component-tag condition. It is not "the whole population a live sleeve could hold".
  - **Declared confound (R15):** an unresolved distressed issuer can enter C′ while an otherwise identical resolved one
    cannot. The census tabulates, per formation, every (period found, assets resolved/screened, equity
    resolved/screened, FPI true/false/not_evaluated) combination in C′, and the count excluded by each reason (no
    companyfacts entry, integrity-excluded, SIC missing, SIC financial, FPI, assets ≤ 0, equity < 0, not executable).
    Weights are equal, so counts are weights.
- **D₀(D)**: E(D) rows with `decile = 0`: ranks 0 … ⌊n/10⌋ − 1 in the construction's (GP/A, CIK) order, with ties at
  the decile-0 boundary split by CIK (rule 7 keeps ties whole only at the decile-9 boundary). The count tied at that
  boundary is reported. Used only by the identity gate.
- **Secondary comparators, reported and never gating:** (a) a literal buy-and-hold of C(2013) from X(2013), never
  rebalanced; (b) `SPY` bought at the X(2013) open and sold at the window-end close.

**Schedule assertions — census-time, from the artefact, before any price is read.** Exactly the 12 formations,
strictly increasing and unique. Each X(D) is an NYSE session, is strictly after D, is the first session of July of
year D (so every July return includes exactly one outgoing-book session), and precedes the next formation and
`WINDOW_END`. Each target set is non-empty with unique symbols. A(D) ⊆ E(D), D₀(D) ⊆ E(D), E(D) ⊆ C′(D), and
A(D) ∩ D₀(D) = ∅. The construction already refuses n < 100, so D₀ has ≥ 10 names, and an overlap would need ties
spanning 80% of E(D). If any assertion fails, it is recorded in the frozen declaration before any price is read, and
the run does not start. It is a construction fact, not an outcome.

The run-time checks are #3362's `simulate_under_policies` validation: unique policy labels, `ZERO_RECOVERY`
present, evidence for every priced symbol, and loaded bars equal to the stored bounds.

Weighting is equal (D1). Every portfolio buys equal-dollar targets at the X(D) **adjusted open**. The first formation
X(2013) starts from cash 1.0. Every later formation rebalances to its new targets.

## Simulator (`r6_monthly_trial.simulate_monthly`)
This is an **ex-post scenario simulator**, not a point-in-time executable process. A missing bar is classified gap or
terminated from the *stored* series bounds (#3362's evidence-bound rule), which a trader on that session could not
know. The classification rule is identical for every portfolio, but the portfolios' exposures to it differ, and the
recovery policies do not bound what that knowledge is worth (R16). The stale-mark and realisation censuses report the
exposure.

**Valuation sessions** (NYSE calendar, `market_calendar`) come in three kinds:
- the X(D) **rebalance**, at the open;
- the **month-end mark**, at the close of each month's last session;
- the **final**, at the `WINDOW_END` 2024-09-27 close.

When a rebalance session is also a month-end session, the open rebalance is processed first and the close mark
second. The final session is not also processed as a mark.

| session | a held symbol with a bar | missing bar, **gap** | missing bar, **terminated** | missing bar, **alive at capture** |
| --- | --- | --- | --- | --- |
| **rebalance** | adjusted open | `policy.gap_fraction` × last close, sold | policy terminal fraction × last close, sold | treated as **gap** |
| **month-end mark** | adjusted close | **last close, held (no trade, fraction 1 under every policy)** | policy terminal fraction × last close, **realised to cash**, half-spread charged | treated as **gap** |
| **final** | adjusted close, sold | `policy.gap_fraction` × last close, sold | terminal fraction × last close, sold | last close, sold (#3362 rule) |

- **Status** follows `_holding_value`'s rule, unchanged and imported.
  - **Alive at capture**: stored `last_bar` > `INTRADER_CAPTURE_DATE − ALIVE_CUT_DAYS`. Such a series missing a bar
    before the final session is a gap.
  - **Terminated**: not alive at capture, and stored `last_bar` < session.
  - Anything else is a gap.
  - A bar on the session is always an ordinary price. Termination never overrides it.
- **Why a gap holds at a mark but takes the bound at a rebalance.** A mark trades nothing. Pricing a halted holding at
  zero there would book a −100% month and then a recovery, which is a timing artefact of the mark. A rebalance and the
  final sale must trade the holding, so #2908's non-executability bound applies there, identically for every
  portfolio.
- **Recognition.** A terminated holding is realised at the **first valuation session strictly after its `last_bar`**:
  a rebalance, a mark or the final. If `last_bar` is itself a valuation session, the holding is priced normally there
  and realised at the next valuation session. The realisation is booked in the month containing that session.
  - The holding's shares are **removed** at realisation, and cash is credited `f × last close × shares × (1 − h)`
    exactly once. Relative to the holding's previous mark value u, this revalues it by −(1 − f)·u and then charges
    the spread h·f·u. A terminated symbol never reappears.
  - A gap **sale** at a rebalance or the final also emits a `Realisation` (status `gap`). That symbol may be bought
    again at a later formation.
  - `realised_value` is logged before the spread, as in #3362.
- **Timing (R17).** The recovery is credited at recognition, not at a dated payment. Cash is not reinvested mid-year,
  so this cannot change terminal wealth. It does move *when* the loss lands in the monthly series, which feeds the
  HAC, cohort and DSR statistics. That effect is declared, not bounded, and its exposure is in the realisation
  census.
- **Cash** earns 0% and is **not reinvested mid-year**. It rejoins at the next X(D) rebalance, identically for every
  portfolio. For the buy-and-hold comparator it stays in cash to the end.
- **Final wealth** = cash + Σ holdings sold × (1 − h). The final spread is charged on holdings sold, never on cash.
- **Costs.** `h = HALF_SPREAD` (0.725%) applies to every traded dollar:
  - the initial purchase;
  - rebalance trades (`_target_value`);
  - termination realisations;
  - the final sale.

  `SPY` is charged the same `h`: one cost basis, no per-name exception. With gross wealth multiple G, its net multiple
  is G(1 − h)/(1 + h), and the report states the exact drag 2hG/(1 + h).
- **Finiteness.** Every price used is checked finite and > 0. That includes the derived adjusted open
  `raw_open × adjusted_close / raw_close`, which can overflow or underflow even when its operands are valid. Every
  wealth, target, cost and return is checked finite, and any violation raises.
- **Zero wealth, by convention.** W < 0 is impossible (fractions ∈ [0, 1], h < 1) and raises. W = 0 is reachable when
  every holding takes a zero-valued bound (termination or gap) at a valuation session.
  - A ruined book holds nothing. The month it reaches 0 has r = −1.
  - Every later month has **r_m = 0 by definition** (W(mark_{m−1}) = 0). Every ratio stays defined, and telescoping
    still holds with terminal return −1.
  - `_target_value` at `pre_cost = 0` returns 0, so parity holds.
  - No special verdict applies. The statistics treat the series as it is, and the report flags the ruin month.
- **Monthly return.** r_m = W(mark_m) / W(mark_{m−1}) − 1, where W = cash + holdings at the mark, after that month's
  costs.
  - W(mark_{2013-06}) is the starting cash, 1.0. July 2013 therefore includes the initial purchase cost.
  - The statistics use the **134 full months**, `month_pairs((2013, 7), (2024, 8))`.
  - The partial month 2024-09 (mark 2024-08 → final) is returned **separately** as `partial_return`.
  - Total return = ∏(1 + r_m) × (1 + partial_return) − 1.
  - Every portfolio × policy × cost series is asserted to carry exactly the 134 month keys before any arithmetic.
- **Turnover (a descriptive statistic of our own definition).** Annual one-way turnover, reported for A, C and C′ at
  each of X(2014) … X(2024). Each value is (rebalance sales + purchases at X(D)) / 2 / pre-cost W at X(D), plus
  (termination realisations recognised strictly after the previous X and strictly before this one) / pre-cost W at
  X(D).
  - A realisation recognised *at* an X is rebalance traded notional, never counted twice.
  - The terminal interval (X(2024), final] is reported as termination realisations / W at X(2024), with its length
    in months stated. The final liquidation is not turnover.
  - Novy-Marx/Velikov's bar is **monthly** one-way turnover on contemporaneous NAV (`strategy-evidence.md`). The
    report shows annual ÷ 12 beside it, labelled as an approximation for an annually rebalanced book, not their
    measure.
- **Result per (portfolio, policy)**:
  - the 134-month series and `partial_return`;
  - the total return, net and gross (gross is the same path at h = 0);
  - events and realisations (#3362's `Realisation`);
  - `realisation_census`, turnover, and the stale-mark census.

**Stale-mark census (full population; R10).** For every portfolio × policy, it counts the held-symbol ×
valuation-session cells in each state:
- bar;
- gap held at a mark;
- gap bounded at a rebalance or the final;
- terminated and realised;
- alive at capture.

It also reports the exposure (share of W) in gap-held cells at each mark, and the distribution of gap durations in
sessions. Per held series, it reports `read_price_series`'s `invalid_rows` (an invalid interior row becomes a missing
bar, and so a gap) and the number of bars dated off the NYSE calendar. Off-calendar bars are neither dropped nor
refused. The annual simulator, #3362's evidence and this simulator all read the same `read_price_series` output, so
the treatment is consistent. The census is descriptive and never gates.

### Reconciliation tests (fixtures)
1. ∏(1 + r_m) × (1 + partial_return) = 1 + total return (telescoping).
2. At every rebalance, pre-cost W = n·target + cost. At every mark, W = cash + Σ holdings.
3. Σ `realised_value × (1 − h)` over terminal realisations equals the cash they credit. No terminated symbol is held
   after its realisation. A gap-sold symbol can be re-bought.
4. With h = 0, no missing bars and one formation, the result equals a hand-computed equal-weight fixture.
5. The gap/terminated/alive-at-capture table: one fixture per cell, under all three programme policies. It includes:
   - a termination whose `last_bar` is a month-end;
   - a gap across a mark that recovers;
   - a gap across a rebalance;
   - an alive-at-capture series with an early gap;
   - a termination in the partial final month;
   - a rebalance on a month-end session.
6. **Parity:** terminal wealth equals `simulate_portfolio`'s under every programme policy, on every fixture above.
7. Zero wealth: every holding terminates at zero. r = −1 in the ruin month and 0 after, telescoping holds, and parity
   holds.
8. **Timing:** the individual monthly wealths are pinned by hand in fixtures 5 and 7, with h > 0 and cash carried
   across marks. Parity cannot catch a mis-dated return; this test can.
9. Every A(D), C′(D) and D₀(D) target has a valid bar, with a finite derived adjusted open, on `x_date`. This is
   asserted, not inferred; a violation is artefact or mirror drift (`REFUSED_PRE_GATE`).

## Identity gate (runs first; arm outcomes stay sealed until it passes)
Source: global-q `prof_monthly_2025/portf_gpa_monthly_2025.csv` in the pinned zip (sha256 `18dfced9…f963c`), header
exactly `year,month,rank_GPA,nstocks,ret_vw`, returns in percent (÷ 100).
- **Ours**: the gross (h = 0) monthly spread r(A) − r(D₀) from `simulate_monthly`, under each programme policy.
  **Reference**: `ret_vw` of rank 10 minus rank 1 (VW vs our EW, D1; HXZ NYSE breakpoints vs ours over E(D), D2).
- **Reference parsing.** Raw rows are read before any dictionary is built.
  - A duplicate (year, month, rank) raw row refuses.
  - After filtering to ranks 1 and 10 and the 134 months, each used row must have integer `nstocks` ≥ 1 and a finite
    `ret_vw` > −100 (a long-only leg cannot lose more than everything; a missing-value sentinel fails one of these).
  - Rank 1 and rank 10 must each cover the 134 months exactly.
- **Calendar alignment is exact.** Our key set and the reference key set must both equal the 134 months; a missing,
  extra or duplicate key refuses. `validate_factor`'s key intersection is **not** used. The lead/lag correlations
  (positional shifts) run only after the equality assertion, on the 133 overlapping months. That unequal window is
  #2908's frozen treatment and is kept.
- **Units.** Correlation, the sign of the OLS beta and the lead/lag correlations are all invariant to a positive
  rescaling of either series, and the file is sha-pinned. A unit error therefore cannot move the gate, and no
  magnitude bound is imposed.
- **Refusals inside the gate.** A non-finite value or a `_pearson` raise is a **gate failure** for that policy, with
  its reason. `_pearson` raises on zero variance, in the full series or in a shifted array.
- **Pass**: #2908's frozen rule (correlation ≥ +0.20, OLS beta > 0, |corr| ≥ max(|lag|, |lead|)) under **every**
  programme policy. This is an inherited heuristic, not a calibrated identity test: it is #2908's declared rule, and
  its constants are not re-derived here.
- **What the gate publishes**: months, correlation, beta sign, lag and lead correlations, and pass/fail per policy.
  It does **not** publish either leg's mean, the beta's magnitude, the alpha or any cumulative figure.
- **A failed gate** means the identity is **unvalidated**. The cause can be a defect, or it can be the deliberate
  differences (EW vs VW, universe, breakpoints, termination). It is not a market finding. Arm outcomes stay sealed.

## Corrections — one policy for every stage
A correction must meet all of these conditions:
- **Grounds**: an **independently evidenced implementation or data defect**, such as a failing fixture, a mismatched
  input or a mis-keyed month. It is never a change of construction, threshold or policy made to improve a result.
- **Declaration**: a written, hashed correction declaration frozen before the re-run.
- **Ledger**: every run attempt, including refused ones, is recorded in the holdout-access ledger, together with the
  flags it exposed.
- **Trial count**: a correction adds **one trial per comparator configuration whose outcome-dependent information was
  visible** before it. Counted: a gate statistic, an arm or control figure, or any flag derived from comparing
  portfolios (a parity failure compares two implementations of *one* portfolio, and does not count).
- **Limit**: at most **one** correction per stage (pre-gate, gate, post-gate). A second failure at a stage is final:
  `GATE_FAIL` at the gate, and `FAIL(simulator_invariant)` after it.

## Statistics
All are computed per policy on the 134 monthly **net active** returns a_m = r_m(A) − r_m(C), and on
a′_m = r_m(A) − r_m(C′) for the diagnostic. T = 134 is the nominal size.

**One refusal rule for every predicate.** Each statistic returns a value or a refusal with a reason. A refusal is
checked before any division or square root, and a predicate over a refused value is **false**, never skipped. The
refusal cases are:
- fewer than 2 observations;
- any non-finite a_m;
- γ̂₀ = 0;
- a non-positive or non-finite HAC variance;
- T_eff ≤ 1;
- a cohort sd of 0;
- fewer than 2 valid measured Sharpe ratios.

Which predicates gate is fixed in the Verdict. A diagnostic statistic that does not gate (the HAC t and cohort t on
a′) is reported, and its refusal vetoes nothing.

1. **HAC t (headline).** mean(a) divided by its Newey–West standard error. The estimator uses the Bartlett kernel,
   1/T autocovariances, no prewhitening and no finite-sample adjustment. The lag is L = ⌊4(T/100)^{2/9}⌋, the
   rule-of-thumb truncation from Newey & West 1994 (*Review of Economic Studies* 61(4):631-653). It is the initial lag
   of their Bartlett plug-in procedure, **not** the full automatic bandwidth (so not R
   `sandwich::NeweyWest(lag = NULL)`). L = 4 at T = 134, and the runner computes it. The reference distribution is
   N(0, 1), and the bar is **t > 3** (Harvey/Liu/Zhu). The headline claims no robustness to dependence beyond lag L.
   Test 2 is a separate guard at the annual horizon.
2. **Holding-year cohort test (a separate conjunct).** Cohorts are **calendar holding years**, July(D) … June(D + 1),
   and the 2013–2023 formations give 11 complete cohorts. July's return includes one outgoing-book session (asserted
   above), identically for A and C.
   - The cohort active return is ∏(1 + r(A)) − ∏(1 + r(C)) over the cohort's months. This is a compounded-wealth
     estimand, different from mean monthly a.
   - The cohort t is mean / (sd / √11), ddof = 1, referred to **Student t with 10 df**. This **assumes** cohort active
     returns are independent and approximately normal across years. It is the few-clusters reference t(G − 1)
     (Bester, Conley & Hansen 2011, *J. Econometrics* 165(2); Cameron & Miller 2015, *J. Human Resources* 50(2), §VI).
     It is used only as an additional refusal, and no conservativeness beyond that is claimed.
   - The bar is q = F⁻¹_t10(Φ(3)), upper tail. It is computed from the closed-form t CDF for integer ν (Abramowitz &
     Stegun 26.7.3/26.7.4) by bisection on [0, 100] to 1e-12. Tests pin 2.228 (0.975), 3.169 (0.995) and the operating
     q ≈ 3.957, which the test cross-checks by independent numerical integration of the t₁₀ density.
   - The 2024 cohort is reported, never tested.
3. **Deflated Sharpe** (Bailey & López de Prado 2014 equation (2), via `deflated_sharpe.deflated_sharpe`). The axis is
   recorded as `quality-2901-monthly-dsr-v1`, distinct from the trade-axis `c6-deflated-sharpe-v1`.
   - SR, y3 and y4 come from a_m, per month and never annualised (`trade_moments` on a_m in percent).
   - **T_eff for the Sharpe: Lo (2002)** (*Financial Analysts Journal* 58(4), "The Statistics of Sharpe Ratios",
     non-IID case). The asymptotic variance of SR is the delta method applied to the HAC (Bartlett, the same L)
     long-run covariance of (a − μ, (a − μ)² − σ²). T_eff solves `(1 − y3·SR + (y4 − 1)/4·SR²) / (T_eff − 1)` =
     that variance, so equation (2)'s standard error equals Lo's. It is capped at T. It covers dependence in both
     returns and squared returns.
   - **Trials M, computed and never hand-written**, from a table in the frozen declaration of every
     **(comparator, configuration)** whose outcome was visible.
     - A configuration is identified by the set of frozen documents that change a computed cell. #2908's
       correction 2 (JSON encoding) and correction 4 (audit logging) change none, so they are not new configurations.
       Its correction 1 froze before any return cell and adds nothing. Its correction 3 changed the resolver after an
       outcome was emitted, so it is a new configuration.
     - Each row lists which arms that configuration actually emitted, taken from its outcome document and
       reconciled against the holdout-access ledger where one exists. No arms × versions product is assumed.
     - M = #2908's rows + 2 (#2901's headline and C′) + the rows added by corrections (above).
     - Not trials: the premise coverage runs r1–r6 and the census (no return read); termination policies and haircut
       cells (every policy must pass); and the secondary comparators.
     - The call passes `declared_trials = M` and `measured_trials` = the number of **valid** same-window SRs
       (headline and C′). If fewer than 2 are valid, that is a refusal. `trial_register_version` is the #2829 register
       row written by the frozen declaration.
   - **ρ = 0, so N = M**, declared (R13). The correlation with #2908's trials could be estimated over the 27 common
     months, though imprecisely. ρ = 0 maximises N within the module's non-negative-correlation interpolation. It is
     not a bound under arbitrary dependence.
   - **V[{SR_n}] is a declared construction, not a bound (R13).** It is the larger of two quantities:
     - the ddof = 1 variance of the valid measured SRs;
     - the largest per-trial IID-Gaussian null sampling variance of a Sharpe estimate, (T_k − 1) / (T_k (T_k − 3)).
       This is the variance of t_{T−1}/√T, and it gives 0.0401 at T_k = 27. It is evaluated at T_k = 27 for #2908's
       trials and at each current trial's T_eff.

     It is chosen so that V is never below the noisiest trial's own null sampling variance. It is not proven to bound
     the family's expected maximum (dependence and non-normality are not covered), and the resulting figure is
     reported as a DSR under this construction.
   - The bar is **DSR > 0.95**, strict (`DSR_PROMOTION_POLICY_VERSION = "c6-dsr-threshold-0.95-v1"`, #2364).
4. **Haircut bars** — #2908's rule and constants, inherited as **policy scenarios** and restated against the control.
   d ∈ {0.15, 0.58}. 0.58 is McLean & Pontiff's (2016) post-publication return decay (`strategy-evidence.md` §2.6),
   and 0.15 is #2908's lighter scenario. Applying a period-level decay to a cumulative window edge is **not** a
   published formulation, so the result holds only under these scenarios. For each policy and d, on total returns
   over the whole window:
   - edge = A_gross − C_gross;
   - adjusted = C_gross + (edge > 0 ? edge·(1 − d) : edge) − (A_gross − A_net), i.e.
     `haircut_net_return(strategy_gross=A_gross, strategy_net=A_net, buy_hold_gross=C_gross, haircut=d)`;
   - pass(d) holds iff margin₁ = adjusted > 0 and margin₂ = adjusted − C_net > 0. Each margin's minimum across
     policies is reported separately.
   - Implications, tested: with edge ≤ 0 the rule reduces to A_net > 0 and A_net > C_net, which an arm can meet only
     through lower costs than the control. The discount acts on the compounded window edge, so its size depends on
     the window's length.

### Composition with termination policies
Every statistic is computed under each of `PROGRAMME_POLICIES`: zero_recovery (which governs), classified_worst and
classified_best. Each promotion condition is an **all-policy conjunction**, and a refusal under any policy makes it
false. For each condition, the report records its least favourable value and the policy that produced it, using
`binding_policy` separately per condition. There is no single binding policy for the verdict. A policy can only turn
a pass into a fail. The gate composes the same way.

## Verdict (evaluated in this order; the first matching line is the verdict)
Notation: `base` is all of the following, each holding under every policy:
- HAC t > 3;
- cohort t > q;
- DSR > 0.95;
- mean(a′) > 0.

`P58` means pass(0.58) under every policy for C, and also with C′ in place of C. `P15` is the same at 0.15.
1. `REFUSED_PRE_GATE`: a run-time refusal before the gate (schedule, parity, data, finiteness). Only the stage and
   the exception type are published, and the exception text must not carry a figure. The correction policy applies.
2. `GATE_FAIL`: the identity gate fails and the correction policy is exhausted. No arm figure is published.
3. `FAIL(simulator_invariant)`: a raise after the gate passed, with the correction policy exhausted. The partial output
   is **sealed** in the holdout area. Only the stage and the exception type are published.
4. `PASS_ROBUST`: `base ∧ P58`.
5. `PASS_CONTINGENT`: `base ∧ P15 ∧ ¬P58`. **No capital.**
6. Otherwise the result is **not a pass**. It is labelled on the monthly mean (the compounded comparison is reported
   beside it, and can differ):
   - `UNDERPERFORMS_CONTROL_MONTHLY` if HAC t < −3 under **every** policy;
   - `FAIL_CONTROL_MONTHLY` if mean(a) ≤ 0 under every policy;
   - otherwise `UNDETERMINED_AT_THIS_POWER`, never "no edge".

   The report lists which conditions failed, and under which policies.

**Complete-case diagnostic, its purpose and its limit.** E(D) admits only issuers that tagged all four components. If
that selection handicaps the control (for example by keeping distressed late filers), the arm can beat C(D) without
beating the broader executable set C′. The diagnostic cannot remove a collider inside E(D). Beating C′ also does not
show the complete-case selection is harmless. The diagnostic bounds one consequence for capital: **an arm that does
not beat C′ gets no capital**. It is a conjunct and cannot rescue a failed headline.

The `GrossProfit` sensitivity is **withdrawn**, and the construction spec is amended in this PR, before the freeze.
The artefact records `gp_tag` as a status, never as an operand, so the sensitivity needs a construction change. It
could only downgrade the result while adding a trial.

**Descriptive readouts.** Their definitions are outcome-independent and frozen here. They are published only after
the gate passes, never gate anything, and are never used to choose a sub-period, a policy or a rescue. A later use of
them to select anything would be a new trial. The readouts are:
- per holding-year cohort (all 12);
- the 2013–2018 vs 2019–2024 cohorts, split on formation year only;
- the SPY regime at D: the sign of SPY's adjusted-close return from the last session on or before D − 365 days to the
  last session on or before D, or `unavailable` if either bar is missing;
- the realisation and stale-mark censuses;
- the literal buy-and-hold;
- SPY.

## Power, stated before the look (no verdict predicted)
`uv run python -m scripts.measure_2901_power --zip <prof_monthly_2025.zip> --sha256 18dfced977d9412e2e6ec0582646f0c61b596db3a4ac88dbcdd9dae53fef963c`
(re-run 2026-09-25; reads pre-window months only) prints the proxy tracking error and the annual excess needed for
50% / 80% power at t > 3 over 134 months. The declaration copies that output verbatim, and the run re-executes the
script and refuses on a mismatch.

The proxy has limits:
- it is IID;
- it compares value-weight decile 10 against the decile average;
- it covers 1967-07 … 2013-06, from the 2025 release;
- it covers the HAC t only, not the cohort, DSR, haircut or diagnostic conjuncts.

This book is equal-weight against an E(D) control, with unknown TE and dependence. No verdict is predicted from the
proxy (construction spec, "Power").

## Minimum notional and broker reachability, "at formation"
Neither is observable point-in-time. The broker's `minPositionAmount` (`etoro_broker.py`, `min_position_amount`) and
its instrument list are current values, and the historical run cannot apply them. The run **reports** £50,000 / |A(D)|
per formation, as a nominal illustration that gates nothing. Both checks bind only live (below).

## Capital boundary (restating #2908's, against this arm's control)
- ISA allocation is **£0** (#2915). The sleeve maximum is **£50,000**, in an ordinary account, long/x1, USD.
  - FX sensitivity: 0.70% of the converted amount on each of the two conversions, applied multiplicatively. The GBP
    net multiple is therefore the USD net multiple × (1 − 0.007)². This covers the fee only; the GBP/USD rate move is
    not modelled. Verdict figures are USD strategy returns.
- `PASS_ROBUST` opens **only a paper-deployment ticket**, not capital. `PASS_CONTINGENT` and every non-pass get £0.
  That ticket needs its **own spec, with ckpt-1**, frozen before its first paper order. The spec must fix each of the
  following:
  - a **forward-available** selection and execution procedure (the historical executability predicate reads the
    execution day's close), and its reconciliation to the tested one;
  - a fresh-universe rebuild at the live June formation;
  - an **injective** mapping of targets to currently validated, BUY-enabled eToro instruments covering ≥ 95% of target
    weight, checked over the full target list. Unmapped weight, and mapped-but-undersized orders below
    `min_position_amount` at their actual post-cost order value, are **held as cash**. The mapped-plus-cash portfolio
    is reconciled against its comparator;
  - global admissibility: `R6RankingIdentity.QUALITY` and the shared datasets admissible in the PIT registry, and no
    target with a failing cell (construction spec, PIT-registry statement);
  - paper reconciliation through the #2844 sandbox, the execution guard and the kill switch;
  - the turn-off rules, each with its action. Every trigger blocks new risk. Risk-reducing exits stay allowed, and
    liquidation is decided per trigger. The triggers are:
    - a PIT or hash failure;
    - mapping drift below 95%;
    - a kill-switch or execution-guard refusal;
    - a missed annual rebuild;
    - realised execution cost above h, measured as side-aware signed implementation shortfall with fees, weighted by
      value, with a stated treatment of partial and no fills;
    - the live readout failing at an **annual rebalance**. Its statistic, threshold, window, comparator (its own live
      C(D)), cost basis and missing-data rule are frozen in that spec.

    Underperformance between annual decisions does not authorise abandonment.

  Any operational failure leaves an evidence-backed **£0 sleeve**.
- #2908's "quality-overlap gate" (which points at this ticket) is **moot**: #2908 failed its own control and has no
  survivor to overlap.

## Declared residuals and deviations
The declaration repeats R1–R9, D1–D8 and the PIT-registry statement of the construction spec (lines 208-222)
verbatim. This spec adds:
- **R10**: gaps at month-end marks are held at the last close. Stale marks relocate losses in time and can bias
  volatility, autocorrelation and covariance, and not necessarily equally across portfolios. The stale-mark census
  reports their exposure and durations per portfolio.
- **R11**: recoveries sit in cash until the next X(D), identically for every portfolio.
- **R12**: `SPY` is charged the programme h.
- **R13**: ρ = 0 and the V construction in the DSR are declared choices, not measurements or proven bounds.
- **R14**: minimum notional and broker reachability are unobservable historically, and are enforced only live.
- **R15**: C′'s screens bind only where they resolved (the missingness confound above).
- **R16**: gap/terminated status is ex-post, read from the stored series bounds. Portfolios' exposures to it differ.
- **R17**: recoveries are credited at recognition, not at a dated payment. This affects monthly timing, not terminal
  wealth.

## Delivery
- **This PR**: this spec, plus the construction-spec amendment withdrawing the `GrossProfit` sensitivity.
- **PR B**: `app/services/r6_monthly_trial.py` (the simulator, gate, statistics and verdict, all pure) with its
  fixtures, and `scripts/run_2901_quality_trial.py`.
  - The fixtures cover reconciliation tests 1–9, the gate's alignment, parsing and refusal cases, the HAC/cohort-t
    reference values including q, Lo's T_eff against a hand-computed case, the V construction, the refusal rule, and
    the verdict table with one fixture per line.
  - The runner refuses to open any arm figure before the gate passes. It records a holdout-access row before loading
    prices, as #2908's final governance correction did.
  - Part 1 (#3380) is amended to this spec's statistics and verdict before it merges.
- **Then**: the frozen declaration (hashes, the M table, the #2829 register entry), then **PR C**: one run, the
  result document, and #2901 closed.

Rung: judgement artefact (spec), so Codex ckpt-1 before merge.
