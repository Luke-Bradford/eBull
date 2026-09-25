# #2901 — quality arm: declaration spec (simulator, identity gate, statistics, verdict, capital)

PR B of `docs/proposals/ta/2026-09-24-2901-quality-arm.md` (the construction spec; "Carried to the declaration
spec", lines 177-203). Refs #2899, #2908, #3362, #2829, #2364. Security: none (research harness; no broker, auth or
order path).

**Status: ckpt-1 round 1 folded in (60 findings, 2026-09-25); round 2 pending.** No return, factor spread or census
outcome has been read. The construction artefact is not yet published (PR A2 = #3378, open); nothing here depends on
its counts.

## What this spec fixes, and what it produces
It fixes every rule the run applies to the construction artefact: the simulator, the identity gate, the statistics,
the verdict and the capital boundary. Its code (PR B) is a new module `app/services/r6_monthly_trial.py` plus a sealed
runner `scripts/run_2901_quality_trial.py`. The **frozen declaration** is a separate, hashed document written after
PR B merges and before the run. As #2908's did, it pins the artefact manifest, the implementation sha256s and every
input digest (`2026-08-24-r6-exclusion-preregistration.md`, "Frozen evidence identity"). PR C is the one run.

`r6_exclusion_trial.py` is **not edited**. Its sha256 is in #2908's result identity and in #3362's
`termination_identity`. The new module imports from it rather than re-implementing: the public primitives
(`TerminationPolicy`, `PROGRAMME_POLICIES`, `ZERO_RECOVERY`, `SeriesEvidence`, `load_series_evidence`,
`read_price_series`, `binding_policy`, `evidence_sha256`, `termination_identity`, `month_pairs`, `HALF_SPREAD`,
`WINDOW_END`, `haircut_net_return`, `_pearson`) **and** the private solvers `_target_value` and `_holding_value`,
whose bytes the module's pinned sha256 already freezes. What is new is only what the annual simulator does not have:
month-end marks, realisation into cash between rebalances, and the monthly series.

**Parity with the frozen simulator (the strongest check available).** Holding a gap at a mark trades nothing, and
realising a termination at a mark instead of at the next rebalance moves the same `fraction × last close` through the
same single half-spread (algebra: at the next rebalance, `pre_cost` falls by `h·v` and `traded` by `v`, so
`_target_value` returns the same target). The monthly simulator's **total return therefore equals
`simulate_portfolio`'s** on the same schedule and policy. The runner asserts this (rel. tol 1e-9) for A, C, C′ and D₀
under every programme policy, over the full run, before any statistic is computed; a fixture asserts it too. A
mismatch is `REFUSED_PRE_GATE` (below).

## Portfolios (all read from the one artefact; identical process, costs and termination)
At each formation D (the 12 Junes 2013–2024), with X(D) the artefact's `x_date`:
- **A(D) arm**: rows with `in_arm` (top GP/A decile of E(D) plus boundary ties).
- **C(D) control, the headline comparator**: all of E(D) (`rung = eligible`). This is the #2908 lesson: the identical
  construction without the signal.
- **C′(D) complete-case diagnostic control**: rows with `executable` true, a recorded SIC that is valid and outside
  6000–6999, `fields.fpi` ≠ `true`, and neither a resolved `Assets` ≤ 0 nor a resolved `StockholdersEquity` < 0. The
  economic screens are applied wherever the artefact resolved them, and incompleteness is not screened: an issuer
  missing a period or a component tag is admitted. `no_companyfacts_entry` and integrity-excluded rows carry no
  recorded SIC and are out.
  - **What C′ is, narrowly:** the executable, non-financial, non-FPI issuers *with a companyfacts entry and a
    recorded SIC*, without the component-tag condition. It is not "the whole population a live sleeve could hold".
  - **Declared confound (R15):** because screens bind only where resolved, an unresolved distressed issuer can enter
    C′ while an otherwise identical resolved one cannot. The census tabulates, per formation, every
    (period found?, assets resolved/screened, equity resolved/screened, FPI) combination in C′, and the counts
    excluded from C′ by each reason (no companyfacts entry, integrity-excluded, SIC missing, SIC financial, FPI,
    assets ≤ 0, equity < 0, not executable). Weights are equal, so counts are the weights.
- **D₀(D)**: E(D) rows with `decile = 0` (ranks 0 … ⌊n/10⌋ − 1 under the construction's (GP/A, CIK) order; ties at the
  decile-0 boundary are split by CIK as rule 7 orders them, and the count at that boundary is reported). Used only by
  the identity gate.
- **Secondary comparators, reported and never gating:** (a) a literal buy-and-hold of C(2013) from X(2013), never
  rebalanced; (b) `SPY` bought at the X(2013) open and sold at the window-end close.

**Schedule assertions (refuse before any simulation):** exactly the 12 formations, strictly increasing and unique;
one `x_date` per formation shared by every portfolio; every target set non-empty with unique symbols; A(D) ⊆ E(D),
D₀(D) ⊆ E(D), E(D) ⊆ C′(D); A(D) ∩ D₀(D) = ∅; and #3380's `simulate_under_policies`-equivalent checks (unique policy
labels, `ZERO_RECOVERY` present, evidence for every priced symbol, loaded bars equal to the stored bounds).
`termination_identity` is asserted equal to the value #3362 recorded, which pins the recovery matrix of every
programme policy by hash rather than by numbers restated here.

Weighting is equal (D1). Every portfolio buys equal-dollar targets at the X(D) **adjusted open**. The first formation
X(2013) starts from cash 1.0. Every later formation rebalances to its new targets.

## Simulator (`r6_monthly_trial.simulate_monthly`)
This is an **ex-post scenario simulator**, not a point-in-time executable process: a missing bar is classified gap
or terminated from the *stored* series bounds (#3362's evidence-bound rule), which a trader on that session could not
know. The programme policies bracket what that knowledge is worth; the classification is identical for every
portfolio (R16).

**Valuation sessions** (NYSE calendar, `market_calendar`) are of three kinds: the X(D) **rebalance** (at the open), the
**month-end mark** (the close of each month's last session) and the **final** (`WINDOW_END` 2024-09-27 close). When a
rebalance session is also a month-end session, the open rebalance is processed first and the close mark second. The
final session is not also processed as a mark.

| session | a held symbol with a bar | missing bar, **gap** | missing bar, **terminated** | missing bar, **alive at capture** |
| --- | --- | --- | --- | --- |
| **rebalance** | adjusted open | `policy.gap_fraction` × last close, sold | policy terminal fraction × last close, sold | treated as **gap** |
| **month-end mark** | adjusted close | **last close, held (no trade, fraction 1 under every policy)** | policy terminal fraction × last close, **realised to cash**, half-spread charged | treated as **gap** |
| **final** | adjusted close, sold | `policy.gap_fraction` × last close, sold | terminal fraction × last close, sold | last close, sold (#3362 rule) |

- **Status** is `_holding_value`'s rule, unchanged and imported. **Alive at capture** means stored `last_bar` >
  `INTRADER_CAPTURE_DATE − ALIVE_CUT_DAYS`; such a series missing a bar before the final session is a **gap**.
  **Terminated** means not alive at capture and stored `last_bar` < session. Anything else is a gap. A bar on the
  session is always an ordinary price, and termination never overrides it.
- **Why a gap holds at a mark but takes the bound at a rebalance.** A mark trades nothing. Pricing a halted holding
  at zero there would book a −100% month and then a recovery, a timing artefact of the mark. A rebalance and the final
  sale must trade the holding, so #2908's non-executability bound applies there, identically for every portfolio.
- **Recognition.** A terminated holding is realised at the **first valuation session strictly after its `last_bar`**
  (a rebalance, a mark or the final). If `last_bar` is itself a valuation session it is priced normally there and
  realised at the next valuation session. The realisation is booked in the month containing that session.
  - State transition: the holding's shares are **removed** at realisation, and cash is credited
    `fraction × last close × shares × (1 − h)` exactly once; no later session prices or sells it again.
  - `realised_value` is logged before the spread, as in #3362.
  - **Timing (R17):** the recovery is credited at recognition, not at a dated payment. It cannot compound before the
    next X(D), because cash is not reinvested mid-year (below), so the assumption can only move money forward to a
    rebalance, identically for every portfolio.
- **Cash** earns 0% and is **not reinvested mid-year**. It rejoins at the next X(D) rebalance, identically for every
  portfolio. For the buy-and-hold comparator it stays in cash to the end.
- **Final wealth** = cash + Σ holdings sold × (1 − h). The final spread is charged on holdings sold, never on cash.
- **Costs.** `h = HALF_SPREAD` (0.725%) on every traded dollar: initial purchase, rebalance trades (`_target_value`),
  termination realisations, and the final sale. `SPY` is charged the same `h` (one cost basis, no per-name
  exception); with gross wealth multiple G its net multiple is G(1 − h)/(1 + h), and the report states the drag
  2hG/(1 + h) exactly.
- **Wealth.** W < 0 is impossible (fractions ∈ [0, 1], h < 1) and raises. W = 0 is reachable only if every holding
  terminates at zero recovery; then the portfolio's later monthly returns are undefined, and the run is labelled
  `DEGENERATE_WEALTH` (not a pass, arm figures published) rather than an invariant failure.
- **Monthly return** r_m = W(mark_m) / W(mark_{m−1}) − 1, W = cash + holdings at the mark after that month's costs.
  W(mark_{2013-06}) is defined as the starting cash 1.0, and the runner asserts the first event is X(2013) in July
  2013, so July 2013 includes the initial purchase cost. The statistics use the **134 full months**
  `month_pairs((2013, 7), (2024, 8))`. The partial month 2024-09 (mark 2024-08 → final) is returned **separately** as
  `partial_return`; total return = ∏(1 + r_m) × (1 + partial_return) − 1.
- **Turnover**, one-way, reported for A, C and C′ over 12 intervals. For interval k = X(2014) … X(2024): (rebalance
  traded notional at X(D)) plus (termination realisations since the previous X), divided by 2 and by the pre-cost
  wealth at X(D). The first purchase is excluded. The terminal interval (X(2024) → final) reports termination
  realisations only; the final liquidation is not turnover. Novy-Marx/Velikov's bar is **monthly** one-way turnover
  (`strategy-evidence.md`), so the report shows annual turnover and annual ÷ 12 beside the 50%/month figure, stating
  the conversion.
- **Result per (portfolio, policy)**: the 134-month series, `partial_return`, the total return (net and gross; gross
  = the same path at h = 0), events, realisations (#3362's `Realisation`), `realisation_census`, turnover, and the
  **stale-mark census** below.

**Stale-mark census (full population, R10).** For every portfolio × policy: the count of held-symbol × valuation-
session cells in each state (bar, gap held at a mark, gap bounded at a rebalance/final, terminated realised, alive at
capture), the exposure (share of W) in gap-held cells per mark, and the distribution of gap durations in sessions.
Also per held series: `read_price_series`'s `invalid_rows` (an invalid interior row becomes a missing bar, so a gap)
and bars dated off the NYSE calendar. Descriptive, never gating.

### Reconciliation tests (fixtures, beyond telescoping)
1. ∏(1 + r_m) × (1 + partial_return) = 1 + total return (telescoping).
2. At every rebalance, pre-cost W = n·target + cost (`_target_value`'s conservation, asserted as the annual simulator
   does). At every mark, W = cash + Σ holdings. At every realisation, ΔW = −h × realised_value.
3. Σ over realisations of `realised_value × (1 − h)` equals the cash credited by realisations; each realised symbol is
   absent from every later session's holdings.
4. With h = 0, no missing bars and one formation, the result equals a hand-computed equal-weight fixture.
5. The gap/terminated/alive-at-capture table: one fixture per cell, under all three programme policies, including a
   termination whose `last_bar` is a month-end, a gap across a mark that recovers, a gap across a rebalance, an
   alive-at-capture series with an early gap, a termination in the partial final month, and a rebalance that falls on
   a month-end session.
6. **Parity:** total return equals `simulate_portfolio`'s under every programme policy on every fixture above.
7. Every A(D), C′(D) and D₀(D) target has a valid bar on `x_date` — asserted: the artefact's executability says so,
   and a violation means artefact or mirror drift (`REFUSED_PRE_GATE`).

## Identity gate (runs first; arm outcomes stay sealed until it passes)
Source: global-q `prof_monthly_2025/portf_gpa_monthly_2025.csv` in the pinned zip (sha256 `18dfced9…f963c`), header
exactly `year,month,rank_GPA,nstocks,ret_vw`, returns in percent (÷ 100).
- **Ours**: the gross (h = 0) monthly spread r(A) − r(D₀) from `simulate_monthly`, under each programme policy.
  **Reference**: `ret_vw` of rank 10 minus rank 1 (VW vs our EW, D1; HXZ NYSE breakpoints vs ours over E(D), D2).
- **Reference parsing.** Raw rows are read before any dictionary is built: a duplicate (year, month, rank) raw row
  refuses. Rows are then filtered to ranks 1 and 10 and to the 134 months. Each of rank 1 and rank 10 must cover all
  134 months exactly, separately.
- **Calendar alignment is exact.** Our key set and the filtered reference key set must both equal the 134 months; a
  missing, extra or duplicate key refuses. `validate_factor`'s key intersection is **not** used. The lead/lag
  computation (a positional shift) runs only after the equality assertion.
- **Units.** Every gate statistic — correlation, the sign of the OLS beta, lead/lag correlations — is invariant to a
  positive rescaling of either series, and the file is sha-pinned. So a unit error cannot move the gate, and no
  magnitude bound is imposed (a bound like |ret| < 1 would also reject legitimate ≥ 100% months). Values must be
  finite.
- **Refusals inside the gate.** Any non-finite value, or a `_pearson` raise (zero variance in the full series or in a
  truncated lead/lag array), is a **gate failure** for that policy, recorded with its reason.
- **Pass**: #2908's frozen rule (correlation ≥ +0.20, OLS beta > 0, |corr| ≥ max(|lag|, |lead|)) under **every**
  programme policy (a conjunct: it can only refuse).
- **What the gate publishes**: months, correlation, beta sign, lag and lead correlations, and pass/fail per policy.
  It does **not** publish either leg's mean, the beta's magnitude, the alpha or any cumulative figure.
- **A failed gate** means the identity is **unvalidated**. That can be a code or data defect, or the deliberate
  differences (EW vs VW, universe, breakpoints, termination) — it is not a market finding either way. Arm outcomes stay
  sealed. One correction is allowed, and only for an **independently evidenced implementation or data defect** (a
  failing fixture, a mismatched input, a mis-keyed month) — never a change of construction, threshold or policy to
  improve the gate. It is a written, hashed correction declaration frozen before the gate re-runs. It counts **+1
  trial per configuration whose gate result is inspected**. A second failure closes #2901 as `GATE_FAIL` with no arm
  result opened.

## Statistics
All on the 134 monthly **net active** returns a_m = r_m(A) − r_m(C) (and a′_m = r_m(A) − r_m(C′) for the diagnostic),
per policy. T = 134 is the nominal size; T_eff is defined in 3.

**Refusal states (each a `None`, never a pass):** fewer than 2 observations; any non-finite a_m; γ̂₀ = 0 (constant
series); a non-positive or non-finite HAC long-run variance Ω̂; T_eff ≤ 1; a cohort sd of 0. Each is checked before
any division or square root, and carries its reason.

1. **HAC t (headline).** mean(a) / NW standard error: Bartlett kernel, autocovariances with 1/T normalisation, no
   prewhitening, no finite-sample adjustment, lag L = ⌊4(T/100)^{2/9}⌋. This L is the rule-of-thumb truncation from
   Newey & West 1994 (*Review of Economic Studies* 61(4):631-653) — the initial lag of their Bartlett plug-in
   procedure, **not** the full automatic bandwidth selection (so this is not R `sandwich::NeweyWest(lag = NULL)`).
   L = 4 at T = 134; the runner computes it. Reference N(0, 1), bar **t > 3** (Harvey/Liu/Zhu).
   - **Dependence sensitivity (reported, never gating):** the same t at L = 11, one holding year less one month,
     because annual rebalancing can carry dependence past lag 4. The headline does not claim robustness to annual
     dependence; test 2 is the gating guard for it, as a separate conjunct.
2. **Holding-year cohort test (a separate conjunct, not a calibration of 1).** Cohorts c are **calendar holding
   years** July(D) … June(D + 1); the 2013–2023 formations give 11 complete cohorts. July's return includes the
   outgoing book's close-to-open move into X(D), which is one session and is identical in construction for A and C.
   The cohort active return is ∏(1 + r(A)) − ∏(1 + r(C)) over the cohort's months, which is a compounded-wealth
   estimand, different from mean monthly a. The cohort t is mean / (sd / √11), ddof = 1, referred to **Student t with
   10 df**. Assumptions, stated: cohort active returns are approximately independent across years and normal; the
   t(G − 1) reference with G fixed is the few-clusters rule (Bester, Conley & Hansen 2011, *J. Econometrics* 165(2);
   Cameron & Miller 2015, *J. Human Resources* 50(2), §VI), used here as a conservative refusal, never as the
   headline. The bar is the t₁₀ quantile at the same two-sided tail probability as |z| = 3: q = F⁻¹_t10(Φ(3)), upper
   tail, computed at run time from the closed-form t CDF for integer ν (Abramowitz & Stegun 26.7.3/26.7.4) by
   bisection on [0, 100] to 1e-12. Tests pin 2.228 (0.975), 3.169 (0.995) **and the operating value** q ≈ 3.957,
   cross-checked in the test by independent numerical integration of the t₁₀ density. The 2024 cohort is reported,
   never tested.
3. **Deflated Sharpe** (Bailey & López de Prado 2014 equation (2), via `deflated_sharpe.deflated_sharpe`; axis
   recorded as `quality-2901-monthly-dsr-v1`, distinct from the trade-axis `c6-deflated-sharpe-v1`):
   - SR, y3 and y4 from a_m (per month, never annualised; `trade_moments` on a_m in percent).
   - **T_eff = T·γ̂₀/Ω̂**, capped at T, from the same estimate as 1. ⚠ This is the effective size for the **mean**;
     using it for the Sharpe ignores dependence in squared returns. It is a declared approximation (R13), not a
     validated Sharpe standard error.
   - **Trials M: every outcome-emitting configuration in the family, computed, never hand-written.** M = (#2908's
     three arms — dilution, filing-risk, union — × the number of **distinct outcome SHA-256s** its result chain
     emitted: the predecessor outcomes named in its corrections 3 and 4 and the final result, de-duplicated by value)
     + 2 (#2901's headline and C′) + one per #2901 correction configuration whose gate or arm figure was inspected.
     #2908's corrections 1–2 froze before any return cell existed and add nothing; correction 3 followed an emitted
     outcome, so it can. The frozen declaration computes M from those documents and states the command. Not trials:
     the premise coverage runs r1–r6 and the census (no return read), termination policies and haircut cells (the
     verdict takes each condition's least-favourable value), and the secondary comparators. The call passes
     `declared_trials = M` and `measured_trials` = the same-window measured SRs (2: headline and C′; the module's
     `MIN_MEASURED_TRIALS` is 2); the declared M is never substituted for it. `trial_register_version` is the #2829
     register row written by the frozen declaration.
   - **ρ = 0, so N = M**, declared. The correlation between these trials and #2908's could be estimated on their
     27 common months, but only imprecisely; ρ = 0 maximises N within the module's non-negative-correlation
     interpolation (it is not a bound for arbitrary dependence).
   - **V[{SR_n}]: a conservative null bound, not the published estimator (R13).** Under H0 each trial's SR estimate
     is ≈ N(0, 1/(T_k − 1)), and the trials have different lengths: T_k = 27 for #2908's three, T_eff for #2901's two.
     For independent centred normals with variances σ_k² ≤ σ²_max, Sudakov–Fernique gives E[max_k σ_k Z_k] ≤ σ_max
     E[max_k Z_k] (Adler & Taylor 2007, *Random Fields and Geometry*, Thm 2.2.3). So V = max(max_k 1/(T_k − 1), the
     ddof = 1 variance of the two measured SRs) — here 1/26 unless the measured term is larger — makes SR₀ an upper
     bound on Bailey–López de Prado's expected maximum. It can only raise the bar.
   - Bar: **DSR > 0.95**, strict (`DSR_PROMOTION_POLICY_VERSION = "c6-dsr-threshold-0.95-v1"`, #2364). A `None` from
     any policy is a **refusal**; it is aggregated before any minimum is taken and keeps its reason.
4. **Haircut bars** (#2908's rule and constants, inherited, restated against the control). d ∈ {0.15, 0.58} are
   #2908's declared scenarios; 0.58 is McLean & Pontiff's (2016) post-publication decay (`strategy-evidence.md`
   §2.6), 0.15 is #2908's lighter scenario. No calibration of either to *cumulative* gross active return is published;
   they are inherited policy scenarios. For each policy and d, on total returns over the whole window:
   - edge = A_gross − C_gross;
   - adjusted = C_gross + (edge > 0 ? edge·(1 − d) : edge) − (A_gross − A_net), i.e.
     `haircut_net_return(strategy_gross=A_gross, strategy_net=A_net, buy_hold_gross=C_gross, haircut=d)`;
   - **pass(d) ⇔ adjusted > 0 and adjusted > C_net**.
   - Implications, tested: with edge ≤ 0 the rule reduces to A_net > 0 and A_net > C_net (no discount; an arm can
     pass only through lower costs than the control); the discount acts on the compounded window edge, so its size
     depends on window length.

### Composition with termination policies
Every statistic is computed under each of `PROGRAMME_POLICIES` (zero_recovery governs; classified_worst,
classified_best). Each promotion condition is an **all-policy conjunction**: HAC t > 3 under every policy, cohort t >
q under every policy, DSR > 0.95 under every policy, pass(d) under every policy. For each condition the report
records its own least-favourable value and the policy that gave it (`binding_policy` per condition; there is no single
binding policy for the verdict). A policy can only turn a pass into a fail. The gate composes the same way.

## Verdict (evaluated in this order; the first matching line is the verdict)
1. `REFUSED_PRE_GATE`: a schedule, parity, data or simulator refusal before the gate. Nothing is published but the
   stage and the exception type; the exception text must not carry a figure. It is fixed and re-run **without** a
   trial increment only if no gate or arm figure was inspected.
2. `GATE_FAIL`: the identity gate fails after at most one correction. No arm figure is published.
3. `FAIL(simulator_invariant)`: any raise after the gate passed. The partial output is **sealed** (written to the
   holdout area, not published); only the stage and exception type are published. A re-run needs a correction
   declaration and counts +1 trial.
4. `DEGENERATE_WEALTH`: any portfolio reaches W = 0 (see Simulator). Not a pass.
5. `PASS_ROBUST` requires every one of these as an all-policy conjunction:
   - HAC t > 3;
   - cohort t > q;
   - DSR > 0.95;
   - pass(0.58);
   - the **complete-case diagnostic**: mean(a′) > 0, and pass(0.58) with C′ in place of C.
6. `PASS_CONTINGENT`: every item in line 5 except that pass(0.58) fails and pass(0.15) holds, for the headline or the
   diagnostic or both. **No capital.**
7. Otherwise **not a pass**, labelled:
   - `UNDERPERFORMS_CONTROL` if HAC t < −3 under **every** policy (robust underperformance);
   - `FAIL_CONTROL` if mean(a) ≤ 0 under every policy;
   - otherwise `UNDETERMINED_AT_THIS_POWER`, never "no edge". The report states which conditions failed and under
     which policies.

**Complete-case diagnostic, its purpose and its limit.** E(D) admits only issuers that tagged all four components. If
that selection handicaps the control (for example by keeping distressed late filers), the arm can beat C(D) while not
beating the broader executable set C′. The diagnostic cannot remove a collider inside E(D), and beating C′ does not
show the complete-case selection is harmless. It bounds one consequence for capital: **an arm that does not beat C′
gets no capital**. It is a conjunct and cannot rescue a failed headline. The construction checklist's
`GrossProfit`-where-COGS-is-missing sensitivity is **resolved as not run**: it needs a construction amendment after the
freeze (the artefact records `gp_tag` as a status, never as an operand), and it would add a trial while only being
able to downgrade.

**Descriptive readouts, outcome-free and never gating.** Per holding-year cohort (all 12); the 2013–2018 vs 2019–2024
cohorts (split on formation year only); the SPY regime at D — the sign of SPY's adjusted-close return from the last
session on or before D − 365 days to the last session on or before D, `unavailable` if either bar is missing (known
at D); the realisation and stale-mark censuses; the literal buy-and-hold; SPY. They are never used to choose a
sub-period or rescue a verdict.

## Power, stated before the look (no verdict predicted)
`uv run python -m scripts.measure_2901_power --zip <prof_monthly_2025.zip> --sha256 18dfced977d9412e2e6ec0582646f0c61b596db3a4ac88dbcdd9dae53fef963c`
(re-run 2026-09-25; reads pre-window months only) prints the proxy tracking error and the annual excess needed for
50% / 80% power at t > 3 over 134 months. The declaration copies that output verbatim, and the run re-executes the
script and refuses on a mismatch. Its limits: it is IID, value-weight decile 10 against the decile average,
1967-07 … 2013-06, from the 2025 release, and covers the HAC t only — not the cohort, DSR, haircut or diagnostic
conjuncts. This book is equal-weight against an E(D) control, with unknown TE and dependence. No verdict is predicted
from it (construction spec, "Power").

## Minimum notional and broker reachability, "at formation"
Neither is observable point-in-time. The broker's `minPositionAmount` (`etoro_broker.py`, `min_position_amount`) and
its instrument list are current values, and the historical run cannot apply them. The run **reports** £50,000 / |A(D)|
per formation as a nominal illustration only, and gates nothing on it. Both checks bind at the **live** formation,
computed from actual post-cost targets in the broker's currency (below).

## Capital boundary (restating #2908's, against this arm's control)
- ISA allocation is **£0** (#2915). The sleeve maximum is **£50,000**, ordinary account, long/x1, USD.
  - FX sensitivity: 0.70% of the converted amount on each of the two conversions, applied multiplicatively, so GBP
    net multiple = USD net multiple × (1 − 0.007)². It covers the conversion fee only; the GBP/USD rate move is not
    modelled, and all verdict figures are USD strategy returns.
- `PASS_ROBUST` opens **only a paper-deployment ticket**, not capital. That ticket requires:
  - a fresh-universe rebuild at the live June formation;
  - ≥ 95% of target weight mapping uniquely to currently validated, BUY-enabled eToro instruments, checked over the
    **full** target list; unmapped weight is **held as cash**, never redistributed;
  - every target's actual post-cost USD order value at or above its broker `min_position_amount`;
  - no target whose PIT-registry cell is failing (construction spec's PIT-registry statement) — a documented
    admissibility check before any order;
  - paper reconciliation through the #2844 sandbox, the execution guard and the kill switch.
  Any operational failure leaves an evidence-backed **£0 sleeve**. `PASS_CONTINGENT` and every non-pass get £0.
- **Turn-off rules** (before the next order), if ever deployed: PIT or hash failure; mapping drift below 95%; a
  kill-switch or execution-guard refusal; a missed annual rebuild; a realised half-spread above h, measured per
  rebalance as the value-weighted |fill − quote mid at order time| / mid over all fills, fees included; or the
  live readout failing at an **annual rebalance**. The paper ticket must freeze the live readout's window, comparator
  implementation (its own live C(D)), cost basis and action before its first order. Underperformance between annual
  decisions does not authorise abandonment.
- #2908's "quality-overlap gate" (pointing at this ticket) is **moot**: #2908 failed its own control and has no
  survivor to overlap.

## Declared residuals and deviations
The declaration repeats R1–R9, D1–D8 and the PIT-registry statement of the construction spec (lines 205-218)
verbatim. This spec adds:
- **R10**: gaps at month-end marks are held at the last close. Stale marks relocate losses in time and can bias
  volatility, autocorrelation and covariance, not necessarily equally across portfolios; the stale-mark census reports
  their exposure and durations per portfolio.
- **R11**: recoveries sit in cash until the next X(D), identically for every portfolio.
- **R12**: `SPY` is charged the programme h.
- **R13**: ρ = 0, T_eff for the Sharpe, and the Sudakov–Fernique V bound in the DSR are constructions, not
  measurements.
- **R14**: minimum notional and broker reachability are unobservable historically, and are enforced only live.
- **R15**: C′'s screens bind only where resolved (the missingness confound above).
- **R16**: gap/terminated status is ex-post (stored series bounds).
- **R17**: recoveries are credited at recognition, not at a dated payment.

## Delivery
- **This PR**: this spec.
- **PR B**: `app/services/r6_monthly_trial.py` (the simulator, gate, statistics and verdict, all pure), its fixtures
  (reconciliation tests 1–7, the gate's alignment and refusal cases, the HAC/cohort-t reference values including
  q, the DSR `None` refusal and the V bound, the verdict table with one fixture per line), and
  `scripts/run_2901_quality_trial.py`. The runner refuses to open any arm figure before the gate passes, and records a
  holdout-access row before loading prices, as #2908's final governance correction did. Part 1 (#3380) is amended to
  this spec's statistics and verdict before it merges.
- **Then**: the frozen declaration (hashes; #2829 register entry), then **PR C**: one run, the result document, and
  #2901 closed.

Rung: judgement artefact (spec), so Codex ckpt-1 before merge.
