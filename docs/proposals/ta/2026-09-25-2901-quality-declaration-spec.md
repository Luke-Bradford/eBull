# #2901 — quality arm: declaration spec (simulator, identity gate, statistics, verdict, capital)

PR B of `docs/proposals/ta/2026-09-24-2901-quality-arm.md` (the construction spec; "Carried to the declaration
spec", lines 177-214). Refs #2899, #2908, #3362, #2829, #2364. Security: none (research harness; no broker, auth or
order path).

**Status: ckpt-1 rounds 1–4 (60, 57, 51, 55 findings) folded in, 2026-09-25.** No return or factor spread has been
read. What has been read: the pre-look census of the published artefact (`census-2026-09-25-2fb142d4.json`, no
returns; posted on #2901) and a date-only audit of the mirror (closure-dated bars, below). The construction froze
when PR A2 (#3378) merged (`2fb142d4`). This spec changes no construction rule; its three amendments to the
construction spec's carried list are declaration-scope.

## What this spec fixes, and what it produces
It fixes every rule the run applies to the construction artefact: the simulator, the identity gate, the statistics,
the verdict and the capital boundary. Its code (PR B) is a new module `app/services/r6_monthly_trial.py` plus a sealed
runner `scripts/run_2901_quality_trial.py`. The **frozen declaration** is a separate, hashed document written after
PR B merges and before the run. As #2908's did, it pins the artefact manifest, the implementation sha256s and every
input digest (`2026-08-24-r6-exclusion-preregistration.md`, "Frozen evidence identity"). PR C is the one run.

`r6_exclusion_trial.py` is **not edited**. Its sha256 is in #2908's result identity and in #3362's
`termination_identity`. The new module imports from it rather than re-implementing:
- public names: `TerminationPolicy`, `PROGRAMME_POLICIES`, `ZERO_RECOVERY`, `SeriesEvidence`, `load_series_evidence`,
  `read_price_series`, `binding_policy`, `evidence_sha256`, `termination_identity`, `month_pairs`, `HALF_SPREAD`,
  `WINDOW_END`, `haircut_net_return` and `_pearson`;
- the private solvers `_target_value` and `_holding_value`, whose bytes the module's pinned sha256 already freezes.

What is new is only the month-end marks, realisation into cash between rebalances, and the monthly series.

**Policies.** Only `PROGRAMME_POLICIES` are used. The runner asserts two things about them:
- each has `terminal_fractions`, so `_holding_value`'s legacy `unsplit` branch never runs;
- `termination_identity` equals #3362's recorded value, which pins every recovery matrix by hash.

"Worst" and "robust" below mean across **these three scenarios**, not across every possible issuer-specific recovery
(R19).

**Parity with the frozen simulator (terminal wealth only).** At a rebalance session, the monthly simulator runs the
annual simulator's own code path. `_holding_value` prices every holding at the open, a terminated or gap holding is
sold as a non-target through `_target_value`, and its half-spread is charged once, inside `traded`.
- Realisation into cash happens **only at marks**. Write v = last close × shares and R = f·v (the recovered notional).
  Realising at a mark instead of at the next rebalance moves the same R through one half-spread: at that rebalance
  `pre_cost` is lower by h·R and `traded` by R, so `_target_value` returns the same target.
- Holding a gap at a mark trades nothing.
- Hence **terminal wealth equals `simulate_portfolio`'s** on the same schedule and policy.

The runner checks this for A, C, C′, D₀ and the literal buy-and-hold (a single-formation schedule), under every
programme policy, **at h = HALF_SPREAD and at h = 0**, before any statistic. It asserts:
1. `simulate_portfolio`'s rebalance events (all but its last) are dated exactly on the artefact's `x_date`s, its final
   event on `WINDOW_END`, and the monthly simulator's rebalance, mark and final events on the same sessions
   (marks on each month's last NYSE session);
2. at every rebalance, both simulators' per-name target values agree (the cash/recognition representation differs,
   the target does not);
3. the terminal wealths W_m and W_a agree: |W_m − W_a| ≤ max(1e-9 · max(|W_m|, |W_a|), 1e-12), which covers W = 0.

SPY is checked against its analytic identity: net multiple = G(1 − h)/(1 + h).

The annual side's terminal wealth is taken as its final event's `pre_cost − cost`, never as `1 + total_return`, which
cancels digits. Parity cannot see *when* a return was booked, so the monthly timing is pinned by fixtures (test 8). A
mismatch is `REFUSED_PRE_GATE`.

## Portfolios (all read from the one artefact; identical process, costs and termination)
At each formation D (the 12 Junes 2013–2024), with X(D) the artefact's `x_date`:
- **A(D) arm**: rows with `in_arm` (top GP/A decile of E(D) plus boundary ties).
- **C(D) control, the headline comparator**: all of E(D) (`rung = eligible`). This is the #2908 lesson: the identical
  construction without the signal.
- **C′(D) complete-case diagnostic control**: rows that meet all of the following:
  - `executable` is true;
  - the SIC is recorded, valid and outside 6000–6999;
  - `fields.fpi` is not `true`;
  - there is no resolved `Assets` ≤ 0 and no resolved `StockholdersEquity` < 0.

  The screens bind only where the artefact resolved them. An issuer missing a period or a component tag is
  admitted, and so is `fpi = not_evaluated`. `no_companyfacts_entry` and integrity-excluded rows carry no recorded
  SIC and are out.
  - **What C′ is:** the executable, non-financial, **not-known-FPI** issuers that have a companyfacts entry and a
    recorded SIC, without the component-tag condition. It is not "the whole population a live sleeve could hold".
  - **Declared confound (R15):** an unresolved distressed issuer can enter C′ while an otherwise identical resolved
    one cannot. The census tabulates, per formation, every C′ combination of period found, assets resolved/screened,
    equity resolved/screened, and FPI true/false/not_evaluated. It counts the rows excluded by each reason — no
    companyfacts entry, integrity-excluded, SIC missing, SIC invalid, SIC financial, FPI, assets ≤ 0, equity < 0, not
    executable — both exclusively (first reason in that order) and overlapping. Each count ÷ |C′(D)| is the
    formation-time target weight; marked weights drift after X(D).
  - **Schema.** Every field C′ reads must be a valid artefact value (`fpi` ∈ {true, false, not_evaluated}; a resolved
    component is a finite canonical decimal). A corrupt value refuses the run (`REFUSED_PRE_GATE`); only the
    enumerated unresolved states are admitted.
- **D₀(D)**: E(D) rows with `decile = 0` under rule 7's frozen formula ⌊10·r/n⌋ on the (GP/A, CIK) order, i.e. ranks
  r < n/10 (⌈n/10⌉ names; the runner asserts the artefact's `decile` field equals the formula). Ties at the decile-0
  boundary are split by CIK; rule 7 keeps ties whole only at the decile-9 boundary. The number tied at
  that boundary is reported. D₀ is used only by the identity gate.
- **Secondary comparators, reported and never gating:**
  - a literal buy-and-hold of C(2013) from X(2013), never rebalanced;
  - `SPY` bought at the X(2013) open and sold at the window-end close.

**Schedule assertions (census-time, from the artefact, before any price is read):**
- exactly the 12 formations, strictly increasing and unique;
- each X(D) is an NYSE session, the first session of July of year D, strictly after D, and before the next formation
  and `WINDOW_END`;
- each target set is non-empty, with unique symbols;
- A(D) ⊆ E(D), D₀(D) ⊆ E(D), E(D) ⊆ C′(D), and A(D) ∩ D₀(D) = ∅.

The construction refuses n < 100, so D₀ has at least 10 names, and an overlap with A would need ties spanning 80% of
E(D). A failed assertion is recorded in the frozen declaration, and the run does not start. It is a fact about the
construction, not an outcome.

The run-time checks are #3362's `simulate_under_policies` validation: unique policy labels, `ZERO_RECOVERY` present,
evidence for every priced symbol, and loaded bars equal to the stored bounds.

Weighting is equal (D1). Every portfolio buys equal-dollar targets at the X(D) **adjusted open**. The first formation
X(2013) starts from cash 1.0, and every later formation rebalances to its new targets.

## Simulator (`r6_monthly_trial.simulate_monthly`)
This is an **ex-post scenario simulator**, not a point-in-time executable process (R16, R18).
- A missing bar is classified as a gap or a termination from the *stored* series bounds (#3362's evidence-bound
  rule). A trader on that session could not know which it is.
- Under the governing `zero_recovery` policy, the label on a missing bar cannot change terminal wealth. A series
  labelled terminated has no later bar by definition, and a series whose bar resumes is a gap under both readings.
  Its `gap_fraction` and every terminal fraction are 0, so a missing bar at any trade is worth 0 either way. The label
  changes only **which month** records the loss (a gap is held at marks), and so whether a ruin falls inside the 134
  months (inference eligibility), plus #3362's alive-at-capture rule at the final session.
- Under the classified policies, the classification also changes recovery values. That channel is not bounded; its
  exposure is reported in the realisation census.
- Executability (construction rule 2) reads the X(D) row itself, including its close, so admission uses information
  from after the purchase open. No inference about a strategy executable in real time is drawn (R18). A forward
  procedure belongs to the paper ticket (below).

**Valuation sessions** are taken from the NYSE calendar (`market_calendar`):
- the X(D) **rebalance**, at the open;
- the **month-end mark**, at the close of each month's last session;
- the **final**, at the `WINDOW_END` 2024-09-27 close.

X(D) is the first session of July (asserted), so a rebalance session is never also a month-end session. The final
session is not processed as a mark.

| session | a held symbol with a bar | missing bar, **gap** | missing bar, **terminated** | missing bar, **alive at capture** |
| --- | --- | --- | --- | --- |
| **rebalance** | adjusted open | `policy.gap_fraction` × last close, sold (annual code path) | policy terminal fraction × last close, sold (annual code path) | treated as **gap** |
| **month-end mark** | adjusted close | **last close, held (no trade, fraction 1 under every policy)** | policy terminal fraction × last close, **realised to cash**, half-spread charged | treated as **gap** |
| **final** | adjusted close, sold | `policy.gap_fraction` × last close, sold | terminal fraction × last close, sold | last close, sold (#3362 rule) |

- **Status** follows `_holding_value`'s rule, unchanged and imported:
  - **alive at capture** means stored `last_bar` > `INTRADER_CAPTURE_DATE − ALIVE_CUT_DAYS`, and such a series
    missing a bar before the final session is a gap;
  - **terminated** means not alive at capture and stored `last_bar` < session;
  - anything else is a gap.

  A bar on the session is always an ordinary price, and termination never overrides it.
- **"Last close"** is the adjusted close of the last valid bar before the session, taken from `read_price_series`
  output exactly as the frozen simulator reads it. That includes a bar dated on an NYSE full closure. Measured on the
  full mirror (2026-09-25, a prototype of `scripts/measure_2901_offcalendar.py`, which PR B commits and the
  declaration re-runs and records with its sha256 and the mirror commit): no weekend bars, 8 closure-dated bars inside
  2013-06 … 2024-09 across 22,879 files, and no file ends on one. The runner counts every valuation that takes its
  last close from such a bar and reports the count. They are kept because dropping them would change
  `read_price_series`' output away from what the frozen simulator and #3362's `evidence_sha256` consumed, and so
  break parity; stored first/last bounds are not the reason (no file ends on one).
- **Why a gap is held at a mark but takes the bound at a rebalance.** A mark trades nothing, so pricing a halted
  holding at zero there would book a −100% month followed by a recovery — a timing artefact of the mark. A rebalance
  and the final sale must trade the holding, so #2908's non-executability bound applies there, identically for every
  portfolio.
- **Recognition.** A terminated holding is recognised at the **first valuation session strictly after its
  `last_bar`**: a rebalance, a mark or the final. If `last_bar` is itself a valuation session, the holding is priced
  normally there and recognised at the next one. The recognition is booked in the month containing that session.
  - **At a mark**, the holding's shares are **removed**, and cash is credited R·(1 − h), with v = last close × shares
    and R = f·v. Against its previous mark value u, the month books a revaluation R − u and a spread h·R.
  - **At a rebalance or the final**, it follows the annual code path, as a sale of a non-target.
  - Either way the spread is charged exactly once, and a terminated symbol is never held again.
  - A gap **sale** at a rebalance or the final also emits a `Realisation` (status `gap`), and that symbol may be
    bought again at a later formation.
  - `realised_value` is logged before the spread, as in #3362.
- **Timing (R17).** The recovery is credited at recognition, not at a dated payment. Within the simulator this cannot
  change terminal wealth relative to the annual simulator, which is the parity claim. A real payment that arrived
  after the next rebalance or the final date would change it, and that is not modelled. Recognition timing moves
  *when* the loss lands in the monthly series, which feeds the statistics. That effect is declared, not bounded.
- **Cash** earns 0% and is **not reinvested mid-year**. It rejoins at the next X(D) rebalance, identically for every
  portfolio. For the buy-and-hold comparator it stays in cash to the end.
- **Final wealth** = cash + Σ holdings sold × (1 − h). The final spread is charged on holdings sold, never on cash.
- **Costs.** h = `HALF_SPREAD` (0.725%) on every traded dollar: the initial purchase, rebalance trades
  (`_target_value`), recognitions, and the final sale. `SPY` is charged the same h: one cost basis, with no per-name
  exception. With gross wealth multiple G its net multiple is G(1 − h)/(1 + h), and the report states the exact drag
  2hG/(1 + h).
- **Finiteness.**
  - Every price used must be finite and > 0, including the derived adjusted open `raw_open × adjusted_close /
    raw_close`.
  - Every wealth, target, cost, share count and return must be finite.
  - **Underflow:** any operation whose inputs are all positive (fraction × price, target ÷ price, shares × price,
    recovered cash, a target value) and whose result is 0 raises. It is never read as ruin.
  - Returns are derived from the wealth path and never the reverse: the simulator stores W at every mark, and total
    return, telescoping and ruin are read from W, so r = W_new/W_old − 1 rounding to −1 cannot invent a ruin.
  - Any violation raises.
- **Ruin, from total wealth only.** W < 0 is impossible (fractions lie in [0, 1] and h < 1) and raises. W = 0 exactly,
  meaning cash plus holdings, is reachable only when every holding takes an exact-zero fraction and no cash remains.
  - The month in which W reaches 0 has r = −1, and every later month has r_m = 0. `partial_return` is 0 when
    W(mark 2024-08) = 0.
  - A ruined book is absorbing: later rebalances record an event with pre-cost 0 and create **no** target entries,
    and the censuses record no held cells after ruin. (The annual code creates zero-share entries; both terminal
    wealths are 0, so parity holds.)
  - Descriptive ratios over zero wealth (turnover, stale exposure) are `unavailable`.
  - **Scope.** A ruin flag is keyed by (portfolio, policy, h, interval: the 134 months, or September).
  - **Inference refuses on ruin inside the 134 months** for every monthly statistic that uses that series (HAC t,
    cohort t, the gate's correlations for a gross gate leg A or D₀, the 135-observation descriptive HAC). Post-ruin
    zeros are bookkeeping, not observed returns on capital. A September-only ruin leaves the 134-month statistics
    evaluable and makes the 135-observation descriptive HAC `unavailable`.
  - **The haircut margins stay evaluable after ruin:** a total return of −1 is an observed economic outcome.
- **Monthly return.** r_m = W(mark_m) / W(mark_{m−1}) − 1, where W is cash plus holdings at the mark, after that
  month's costs.
  - W(mark_{2013-06}) is the starting cash, 1.0, so July 2013 includes the initial purchase cost.
  - For every later July, the return covers the outgoing book's overnight interval from the June close to the X(D)
    open, and then the new book. July 2013 has no outgoing book.
  - The statistics use the **134 full months**, `month_pairs((2013, 7), (2024, 8))`.
  - The partial month 2024-09 (mark 2024-08 → final) is `partial_return`.
  - Total return = ∏(1 + r_m) × (1 + partial_return) − 1.
  - Every portfolio × policy × cost series is asserted to carry exactly the 134 month keys before any arithmetic.
  - **Estimand limit (declared):** the monthly statistics exclude September 2024, including its liquidation costs. The
    haircut bars use total returns, which include them. The report also shows the headline HAC t with September
    appended as a 135th, partial observation. That figure is descriptive only.
- **Turnover.** Reported for A, C and C′, and never gating.
  - **Monthly one-way turnover, our conventions:** for each of the 134 months, (sales + purchases executed in the
    month) / 2 / W(mark_{m−1}); the initial purchase is excluded; recognitions at marks count as sales; a recognition
    at a rebalance is part of that rebalance's trades and counted once. The mean over the 134 months is shown beside
    Novy-Marx/Velikov's 50%/month bar (`strategy-evidence.md`) as a comparison of form only; these conventions are
    ours, not a reproduction of their formula.
  - September 2024 and the whole window (including the final liquidation) are reported as separate traded-notional
    figures, outside the 134-month mean.
  - **Forced exits:** in each month, the pre-loss last-close value removed by recognitions, ÷ W(mark_{m−1}). A
    zero-recovery termination trades nothing but still ends a position, so this is shown separately from trading
    notional.
- **Result per (portfolio, policy, h):** the 134-month series, `partial_return`, total return, events and
  realisations (#3362's `Realisation`), `realisation_census`, turnover and forced exits, and the stale-mark census.

**Stale-mark census (full population; R10).** For every portfolio × policy it reports:
- the count of held-symbol × valuation-session cells, cross-tabulated as **evidence status** (bar, gap, terminated,
  alive at capture) × **action** (priced, held stale, bounded and sold, recognised), so no cell is counted twice;
- the share of W in held-stale cells at each mark;
- gap durations in sessions, from the first missing session while held to the resumption, the sale or the window end,
  with a censoring flag for the latter two (a gap already running at purchase cannot occur: targets have a bar on
  `x_date`);
- per held series, `read_price_series`'s `invalid_rows` (an invalid interior row becomes a missing bar, and so a gap)
  and its closure-dated bars.

The census is descriptive and never gates.

### Reconciliation tests (fixtures)
1. ∏(1 + r_m) × (1 + partial_return) = 1 + total return (telescoping).
2. At every rebalance, pre-cost W = n·target + cost. At every mark, W = cash + Σ holdings.
3. Σ `realised_value × (1 − h)` over recognitions at marks equals the cash they credit. No terminated symbol is held
   after recognition. A gap-sold symbol can be re-bought.
4. With h = 0, no missing bars and one formation, the result equals a hand-computed equal-weight fixture.
5. The gap/terminated/alive-at-capture table: one fixture per cell, under all three programme policies, at h > 0 and
   at h = 0 (the gate consumes the gross path). Required cases:
   - a termination whose `last_bar` is a month-end;
   - a gap across a mark that recovers;
   - a gap across a rebalance;
   - an alive-at-capture series with an early gap;
   - a termination in the partial final month;
   - a termination recognised exactly at a rebalance;
   - a last close taken from a closure-dated bar.
6. **Parity:** on every fixture, terminal wealth equals `simulate_portfolio`'s under every programme policy, at h = 0
   and at h = HALF_SPREAD, with event dates asserted as above.
7. **Multi-formation carry:** a termination recognised at a mark, its cash carried across further marks, then
   reinvested at the next X. Checked under every policy and both h values, with parity.
8. **Timing:** the individual monthly wealths are pinned by hand in fixtures 5 and 7, at h > 0 and at h = 0. Parity
   cannot catch a mis-dated return; this can.
9. **Ruin:** every holding terminates at zero with no cash. Checked with ruin inside the 134 months (r = −1 then 0;
   the statistics refuse; the haircut margins evaluate), ruin before a further rebalance (no target entries created),
   and ruin in September (`partial_return` = −1; the 134-month statistics are unaffected). Telescoping and parity
   hold in each. Plus a D₀ ruin (the gate policy refuses).
10. **Underflow:** each positive-input operation listed under "Finiteness" that underflows to 0 raises, and a W ratio
    within one ulp of 0 does not produce a ruin.
11. Every A(D), C′(D) and D₀(D) target has a valid bar on `x_date`, with a finite derived adjusted open. This is
    asserted, not inferred; a violation means artefact or mirror drift (`REFUSED_PRE_GATE`).

## Identity gate (runs first; arm outcomes stay sealed until it passes)
Source: global-q `prof_monthly_2025/portf_gpa_monthly_2025.csv` in the pinned zip (sha256 `18dfced9…f963c`). The
header must be exactly `year,month,rank_GPA,nstocks,ret_vw`, and returns are in percent (÷ 100).
- **Ours**: the gross (h = 0) monthly spread r(A) − r(D₀) from `simulate_monthly`, under each programme policy.
- **Reference**: `ret_vw` of rank 10 minus rank 1 (VW vs our EW, D1; HXZ NYSE breakpoints vs ours over E(D), D2).
- **Reference parsing.** Raw rows are read before any dictionary is built.
  - A duplicate (year, month, rank) raw row refuses.
  - After filtering to ranks 1 and 10 and the 134 months, each used row must have an integer `nstocks` ≥ 1 and a
    finite `ret_vw` ≥ −100 (exactly −100 is a legitimate total loss). A value outside those domains refuses. No
    missing-value sentinel is documented for this file; a portfolio with `nstocks` ≥ 1 has a return, so a sentinel
    inside the domain is not detectable and is not claimed to be.
  - Rank 1 and rank 10 must each cover the 134 months exactly.
- **Calendar alignment is exact.** Our key set and the reference key set must both equal the 134 months; a missing,
  extra or duplicate key refuses. `validate_factor`'s key intersection is **not** used. The lead/lag correlations
  (positional shifts) run only after the equality assertion, on the 133 overlapping months, which is #2908's frozen
  treatment.
- **Units.** Correlation, the sign of the OLS beta and the lead/lag correlations are invariant to a positive
  rescaling of either **spread**, and the file is sha-pinned. Both reference legs pass through the same ÷ 100 before
  subtraction, and both of ours are fractions from one simulator, so a uniform unit error cannot move the gate, and
  no magnitude bound is imposed. A fixture pins the leg-level conversion.
- **Refusals inside the gate.** A non-finite value, a ruin of A or D₀ (gross), or a raise from `_pearson` (zero
  variance in the full series or in a shifted array) makes that policy's gate value **refused**, with its reason.
  Composition (below) keeps it distinct; at the verdict boundary a refused or failed gate is `GATE_FAIL`.
- **Pass**: #2908's frozen rule (correlation ≥ +0.20, OLS beta > 0, |corr| ≥ max(|lag|, |lead|)) under **every**
  programme policy. This is an inherited heuristic, not a calibrated identity test: #2908's declared rule, and its
  constants are not re-derived here.
- **What the gate publishes**: months, correlation, beta sign, lag and lead correlations, and pass/fail per policy.
  It does **not** publish either leg's mean, the beta's magnitude, the alpha or any cumulative figure.
- **A failed gate** means the identity is **unvalidated**. That can be a defect, or it can be the deliberate
  differences (EW vs VW, universe, breakpoints, termination). It is not a market finding. Arm outcomes stay sealed.

## Corrections and trial accounting — one rule for every stage
- **Configuration.** A configuration is identified by the frozen documents and code that change a computed cell. A
  change that alters no computed cell (encoding, audit logging) is the same configuration.
- **Exposure.** Exposure is any outcome-dependent information that becomes visible. That covers a gate statistic, a
  portfolio or comparison figure, a parity or finiteness failure on real data, or any flag derived from those. The
  only exemption is an attempt that ran on synthetic fixtures only; any later real-data run of that configuration is
  exposure as usual. For #2908, exposure is reconstructed from every channel — emitted cells, exit status, failure
  category, traceback — using its documents and ledger, not from emitted return cells alone.
- **Trial rows.** A trial row is a distinct (configuration, comparator) with exposure. The comparators are A−C
  (headline), A−C′ (diagnostic), and for #2908 each arm it actually emitted. A is shared by the headline and the
  diagnostic, and the gate exposes A (through A−D₀), so any exposure of a #2901 configuration creates **both** its
  rows. Rows are counted once, whatever number of correction events touched them.
- **Grounds.** A correction needs an **independently evidenced implementation or data defect**: a failing fixture, a
  mismatched input, a mis-keyed month. It is never a change of construction, threshold or policy made to improve a
  result.
- **Declaration.** Each correction is a written, hashed declaration frozen before the re-run. It carries the revised
  trial-row table, M and the #2829 register identity. Every run attempt, refused or not, is recorded in the
  holdout-access ledger together with what it exposed.
- **Frozen family, before exposure.** M is frozen in the declaration **before** the run and covers every permitted
  attempt: #2908's rows + the #2901 headline and diagnostic rows of the first run (2) + 2 rows for each of the 3
  permitted corrections (6). Corrections never raise M after the fact, so the Bonferroni bar does not move with
  outcomes. The family is every #2829 register row of the r6/selection programme with exposure before the
  declaration freezes (#2908's, and any other the register lists), plus #2901's permitted rows; the declaration
  enumerates it. Trials registered later (#2902–#2904) are not in this family; each of their declarations counts
  #2901's rows.
- **Descriptive readouts** used later to select anything charge every inspected candidate as a row.
- **Limit.** At most **one** correction per stage (pre-gate, gate, post-gate), counted persistently across re-runs. A
  correction at a later stage re-runs every earlier stage, and a stage that fails again after its own correction is
  used up is final.
- **Terminal states.** Each verdict applies when a correction is unavailable (no independently evidenced defect),
  declined, or used up:
  - `REFUSED_PRE_GATE` before the gate;
  - `GATE_FAIL` at the gate;
  - `FAIL(simulator_invariant)` after the gate.
- **#2908's rows under the same rule.** Its correction 1 froze after a run that emitted zero bytes and no return
  cell, so there was no exposure. Corrections 2 (JSON encoding) and 4 (audit logging) alter no computed cell. Its
  correction 3 changed the resolver after outcome `95ff5c23…` had been emitted, so both the pre-correction-3 and
  post-correction-3 configurations have rows, one per arm each exposed. Correction 1's zero-byte run is checked
  against its recorded exit status and failure category before it is exempted. The frozen declaration builds the
  table from those documents and the ledger, and states the command.

## Statistics
All statistics are computed per policy on the 134 monthly **net active** returns a_m = r_m(A) − r_m(C), and on
a′_m = r_m(A) − r_m(C′) for the diagnostic. T = 134.

**Refusals.** Each statistic returns a value or a refusal with a named reason, and every intermediate is checked
before any division, square root or cap. A predicate over a refused value is **refused** (not false); refusal is
carried through composition and becomes "does not hold" only at the verdict boundary. A refused policy never enters a
numeric extremum: `binding_policy` receives only numeric values, the refusing policies are listed separately, and if
every policy refused the binding value and policy are `unavailable`.

| statistic | refuses when |
| --- | --- |
| mean(a) | fewer than 2 observations; any non-finite a_m; a 134-month ruin of A or of the comparator |
| HAC t | the mean's refusals; γ̂₀ = 0; the HAC variance is ≤ 0 or non-finite |
| cohort t | fewer than 2 complete cohorts; any non-finite cohort return; cohort sd = 0; ruin |
| Bonferroni bar | M is not a positive integer |
| haircut margins | any non-finite total return (ruin does not refuse them) |

1. **HAC t (headline).** mean(a) divided by its Newey–West standard error.
   - The estimator uses the Bartlett kernel, 1/T autocovariances centred on the sample mean, no prewhitening and no
     finite-sample adjustment.
   - The lag is L = ⌊4(T/100)^{2/9}⌋, the rule-of-thumb truncation from Newey & West 1994 (*Review of Economic
     Studies* 61(4):631-653). It is the initial lag of their Bartlett plug-in procedure, **not** the full automatic
     bandwidth, so it is not R `sandwich::NeweyWest(lag = NULL)`. At T = 134, L = 4, and the runner computes it.
   - The reference distribution is N(0, 1), an asymptotic approximation assuming stationarity and weak dependence.
     Stale marks, annual reconstitution and recognition timing may violate that. The headline claims no robustness to
     dependence beyond lag L; test 2 is an additional refusal at the annual horizon, not a repair of this estimator.
2. **Holding-year cohort test (a separate conjunct).** Cohorts are **calendar holding years**, July(D) … June(D + 1).
   The 2013–2023 formations give 11 complete cohorts.
   - The cohort active return is ∏(1 + r(A)) − ∏(1 + r(C)) over the cohort's months. This is a compounded-wealth
     estimand, different from mean monthly a.
   - The cohort t is the one-sample t-test of the 11 cohort active returns: mean / (sd / √11), ddof = 1, referred to
     **Student t with 10 df**. Its assumption is that the 11 values are independent draws from a normal distribution
     — assumed here, not established. (The few-clusters literature — Bester, Conley & Hansen 2011, *J. Econometrics*
     165(2); Cameron & Miller 2015, *J. Human Resources* 50(2), §VI — motivates the t(G − 1) reference for G fixed; it
     does not validate independence here.) It is used only as an additional refusal.
   - The bar is q = F⁻¹_t10(Φ(3)), upper tail. It is computed from the closed-form t CDF for integer ν (Abramowitz &
     Stegun 26.7.3/26.7.4), by bisection on [0, 100] to 1e-12. Tests pin 2.228 (0.975), 3.169 (0.995) and the
     operating q ≈ 3.957, cross-checked in the test by independent numerical integration of the t₁₀ density.
   - The 2024 cohort (July–August plus the partial September) is reported separately and never tested.
3. **Deflation over the trial family: Bonferroni on the HAC t** (Harvey, Liu & Zhu 2016, *Review of Financial Studies*
   29(1):5-68, §4, "Bonferroni's adjustment"; α = 0.05, their level).
   - With M the frozen family size above, the condition is **t > 3 and p ≤ α/M**, where p = 2(1 − Φ(t)) is the
     headline's two-sided HAC p-value. Written t_M = max(3, Φ⁻¹(1 − α/(2M))), that is t > t_M when 3 binds and
     t ≥ t_M when the Bonferroni term binds; the runner evaluates the two inequalities literally. The t > 3 is this
     programme's inherited bar (#2908; motivated by HLZ's recommended hurdle), separate from the Bonferroni
     adjustment itself.
   - Bonferroni controls the family-wise error rate under arbitrary dependence between trials **given valid
     per-trial p-values**. Ours are asymptotic HAC p-values, so the control is asymptotic and assumption-dependent.
     No ρ, trial Sharpe variance or effective sample size is needed. It is conservative when trials are correlated.
   - The FWER claim covers the **headline hypothesis** (A beats C). The diagnostic rows are counted in M so the family
     is complete, but the diagnostic's own acceptance rule (mean(a′) > 0 and the haircut) is a sign condition, not a
     multiplicity-adjusted test.
   - This replaces the Deflated Sharpe Ratio. #2364's `c6-dsr-threshold-0.95-v1` is the trade-axis promotion bar and
     is not claimed here. If a later promotion path requires a DSR, the paper ticket's spec must supply it (below).
4. **Haircut bars.** These are #2908's rule and constants, inherited as **policy scenarios** and restated against the
   control.
   - d ∈ {0.15, 0.58}: 0.58 is McLean & Pontiff's (2016) post-publication return decay (`strategy-evidence.md` §2.6),
     and 0.15 is #2908's lighter scenario. Applying a period-level decay to a cumulative window edge is **not** a
     published formulation, so the result holds only under these scenarios.
   - For each policy and d, on total returns over the whole window, edge = A_gross − C_gross.
   - adjusted = C_gross + (edge > 0 ? edge·(1 − d) : edge) − (A_gross − A_net), which is
     `haircut_net_return(strategy_gross=A_gross, strategy_net=A_net, buy_hold_gross=C_gross, haircut=d)`.
   - pass(d) holds iff margin₁ = adjusted > 0 and margin₂ = adjusted − C_net > 0. Each margin is reported with its own
     least-favourable policy.
   - Two implications are tested. With edge ≤ 0, the rule reduces to A_net > 0 and A_net > C_net: no discount applies,
     and an arm can pass only through lower costs than the control. Because the discount acts on the compounded
     window edge, its size depends on window length.

### Composition with termination policies
Every statistic is computed under each of `PROGRAMME_POLICIES`: zero_recovery, which governs, classified_worst and
classified_best.
- Each condition is **three-valued per policy**: true (T), false (F) or refused (R).
- One operator composes everything — across policies, across a haircut's two margins, across C and C′, and across
  `base`'s conjuncts. It is an intentional **refusal-dominant** conjunction (not strong-Kleene): R if any input is R;
  otherwise T if every input is T; otherwise F. Truth table for two inputs: (T,T)→T, (T,F)→F, (F,F)→F, (T,R)→R,
  (F,R)→R, (R,R)→R. A refusal is never silently read as a failure or a pass.
- The least-favourable value is taken in the direction of the condition: the minimum for "> bar" conditions, the
  maximum for "< bar" and "≤ 0" conditions. The report records it and its policy (via `binding_policy`, per
  condition, with the direction applied), plus any refusing policies. There is no single binding policy for the
  verdict.
- Adding a policy can only remove **pass eligibility**; it can turn a determinate label into a refusal or a different
  not-pass label. The gate composes the same way.

## Verdict (evaluated in this order; the first matching line is the verdict)
Notation:
- `base` = the composition of: the Bonferroni condition (t > 3 and p ≤ α/M), cohort t > q, and mean(a′) > 0.
- `P(d)` holds when pass(d) holds for C, and also with C′ in place of C.

1. `REFUSED_PRE_GATE`: a run-time refusal before the gate (schedule, parity, data, finiteness), with correction
   unavailable, declined or used up. Only the stage and the exception type are published, and the exception text
   must not carry a figure.
2. `GATE_FAIL`: the identity gate fails, with correction unavailable, declined or used up. No arm figure is
   published.
3. `FAIL(simulator_invariant)`: a raise after the gate passed, with correction unavailable, declined or used up. The
   partial output is **sealed** in the holdout area; only the stage and the exception type are published.
4. `PASS_ROBUST`: `base` and `P(0.58)` hold.
5. `PASS_CONTINGENT`: `base` and `P(0.15)` hold, and `P(0.58)` **fails** (evaluated, not refused). **No capital.**
6. Otherwise **not a pass**. Exactly one label applies, taken in this order:
   - `NOT_PASS_REFUSED`: any gating condition is refused (e.g. ruin), with the reasons listed.
   - `UNDERPERFORMS_CONTROL_MONTHLY_HAC`: t < −3 and p ≤ α/M under every policy (the binding value is the maximum t).
     A HAC-only finding on the monthly mean; no cohort test is applied to it.
   - `FAIL_CONTROL_MONTHLY`: mean(a) ≤ 0 under every policy.
   - `FAIL_ECONOMIC`: `base` holds and `P(0.15)` fails: significant, but it does not survive the haircuts.
   - `FAIL_DIAGNOSTIC`: the headline conditions hold (Bonferroni and cohort) but the composed condition mean(a′) > 0
     does not (false under at least one policy).
   - `UNDETERMINED_AT_THIS_POWER`: the only failed conditions are the Bonferroni or cohort significance conditions.
     Never labelled "no edge".
   - `NOT_PASS_MIXED`: anything else (e.g. significance and the diagnostic both fail), with every failed condition
     listed.

   The report lists every failed or refused condition, with its policies, and shows the compounded comparison beside
   the monthly labels.

**Complete-case diagnostic, its purpose and its limit.** E(D) admits only issuers that tagged all four components. If
that selection handicaps the control (for example by keeping distressed late filers), the arm can beat C(D) without
beating the broader executable set C′. The diagnostic cannot remove a collider inside E(D), and beating C′ does not
show that the complete-case selection is harmless. It bounds one consequence for capital: **an arm that does not beat
C′ gets no capital**. It is a conjunct and cannot rescue a failed headline. The `GrossProfit` sensitivity is
**withdrawn** by the amendment to the construction spec's carried list: `gp_tag` is a status, never an operand, and
the sensitivity could only downgrade the result while adding trial rows.

**Descriptive readouts.** Their definitions are outcome-independent and frozen here. A failure to compute one (e.g.
missing SPY data) makes that readout `unavailable`; it never refuses the run unless it invalidates shared evidence. They are published only after
the gate passes, never gate anything, and are never used to choose a sub-period, a policy or a rescue; any later
selection made with them would be a new trial row. The readouts are:
- each of the 11 complete cohorts, with the 2024 partial cohort shown separately;
- the mean cohort active return over the 2013–2017 formations against the 2018–2023 formations. Both halves contain
  complete cohorts only, the split is on formation year, and the partial 2024 cohort is excluded from both;
- the SPY regime at D: the sign of SPY's adjusted-close return from the last session on or before D − 365 days to
  the last session on or before D, or `unavailable` if either bar is missing;
- the realisation and stale-mark censuses;
- the literal buy-and-hold;
- SPY.

## Power, stated before the look (no verdict predicted)
`uv run python -m scripts.measure_2901_power --zip <prof_monthly_2025.zip> --sha256 18dfced977d9412e2e6ec0582646f0c61b596db3a4ac88dbcdd9dae53fef963c`
(re-run 2026-09-25; it reads pre-window months only) prints the proxy tracking error and the annual excess needed for
50% / 80% power at t > 3 over 134 months. The declaration copies that output verbatim. The run re-executes the
script and refuses on a mismatch.

The proxy has these limits:
- it is IID;
- it compares value-weight decile 10 against the decile average;
- it covers 1967-07 … 2013-06, from the 2025 release;
- it covers the HAC t at 3.0 only, not t_M if that is higher, nor the cohort, haircut or diagnostic conjuncts.

This book is equal-weight against an E(D) control, with unknown TE and dependence. No verdict is predicted from the
proxy (construction spec, "Power").

## Minimum notional and broker reachability, "at formation"
Neither is observable point-in-time. The broker's `minPositionAmount` (`etoro_broker.py`, `min_position_amount`) and
its instrument list are current values, and the historical run cannot apply them. The run **reports** £50,000 /
|A(D)| per formation as a nominal illustration, and gates nothing on it. Both checks bind only live (below).

## Capital boundary (restating #2908's, against this arm's control)
- ISA allocation is **£0** (#2915). The sleeve maximum is **£50,000**, in an ordinary account, long/x1, USD.
  - FX sensitivity: 0.70% of the converted amount on each of the two conversions, applied multiplicatively, so the
    GBP net multiple = the USD net multiple × (1 − 0.007)². It covers the fee only; the GBP/USD rate move is not
    modelled, and the verdict figures are USD strategy returns.
- `PASS_ROBUST` opens **only a paper-deployment ticket**, not capital. `PASS_CONTINGENT` and every non-pass get £0.
  That ticket needs its **own spec, reviewed at ckpt-1** and frozen before its first paper order. The spec must fix:
  - a **forward-available** selection and execution procedure (the historical executability predicate reads the
    execution day's close), and its reconciliation with the tested one;
  - a fresh-universe rebuild at the live June formation;
  - an **injective** mapping of targets to currently validated, BUY-enabled eToro instruments, covering ≥ 95% of
    target weight, checked over the full target list;
    - unmapped weight is **held as cash**;
    - mapped orders whose actual post-cost value falls below `min_position_amount` are also **held as cash**;
    - after both exclusions, **≥ 95% of target weight must actually be executed**, or the formation deploys nothing;
    - the mapped-plus-cash portfolio is reconciled against its comparator;
  - global admissibility: `R6RankingIdentity.QUALITY` and the shared datasets admissible in the PIT registry, and no
    target with a failing cell (construction spec, PIT-registry statement);
  - whatever deflation evidence the promotion path (`check_promotable`) requires;
  - paper reconciliation through the #2844 sandbox, the execution guard and the kill switch;
  - the turn-off rules, each with its action. Every trigger blocks new risk. Risk-reducing exits stay allowed, and
    whether to liquidate is set per trigger. The triggers are:
    - a PIT or hash failure;
    - mapping drift below 95%;
    - a kill-switch or execution-guard refusal;
    - a missed annual rebuild;
    - realised execution cost above h, measured as side-aware signed implementation shortfall including fees,
      value-weighted, with a stated treatment of partial and no fills;
    - the live readout failing at an **annual rebalance**. Its statistic, threshold, window, comparator (its own live
      C(D)), cost basis and missing-data rule are frozen in that spec.

    Underperformance between annual decisions does not authorise abandonment.

  Any operational failure means **no new allocation** (an evidence-backed £0 increment); whether existing holdings
  are liquidated is the per-trigger action above.
- #2908's "quality-overlap gate" (pointing at this ticket) is **moot**: #2908 failed its own control and has no
  survivor to overlap.

## Declared residuals and deviations
The declaration repeats R1–R9, D1–D8 and the PIT-registry statement of the construction spec (lines 215-229)
verbatim. This spec adds:
- **R10**: gaps at month-end marks are held at the last close. Stale marks relocate losses in time and can bias
  volatility, autocorrelation and covariance, and not necessarily equally across portfolios. The stale-mark census
  reports their exposure and durations.
- **R11**: recoveries sit in cash until the next X(D), identically for every portfolio.
- **R12**: `SPY` is charged the programme h.
- **R13**: the Bonferroni deflation is conservative under correlated trials, and the family is only as complete as
  the trial-row table.
- **R14**: minimum notional and broker reachability are unobservable historically, and are enforced only live.
- **R15**: C′'s screens bind only where they resolved (the missingness confound above).
- **R16**: the gap/terminated status is ex-post, taken from the stored series bounds. Under `zero_recovery` it moves
  only the month in which a loss is recorded; under the classified policies it also moves recovery values.
- **R17**: recoveries are credited at recognition, not at a dated payment.
- **R18**: executability reads the X(D) row itself, including its close, so no real-time-executable inference is
  drawn.
- **R19**: "worst" and "robust" are across the three programme policies only.

## Delivery
- **This PR**: this spec, plus three declaration-scope amendments to the construction spec's carried list: the
  `GrossProfit` sensitivity is withdrawn, the DSR is replaced by Bonferroni, and the minimum-notional and broker
  checks move from "at formation" (unobservable historically) to the live formation.
- **PR B**: `app/services/r6_monthly_trial.py` (the simulator, gate, statistics and verdict, all pure), with its
  fixtures, and `scripts/run_2901_quality_trial.py`.
  - The fixtures cover reconciliation tests 1–11; the gate's alignment, parsing and refusal cases; the HAC/cohort-t
    reference values, including q; t_M; the refusal table; the three-valued composition; and the verdict table,
    with one fixture per line plus the boundary cases: equality at each bar, M where the Bonferroni term starts to
    bind, all policies refused, false-plus-refused, mixed policy signs, and simultaneous statistical and economic
    failure.
  - The runner refuses to open any arm figure before the gate passes. It records a holdout-access row before loading
    prices, as #2908's final governance correction did.
  - Part 1 (#3380) is amended to this spec before it merges: its DSR is removed, Bonferroni and the refusal table
    are added, and the verdict labels change.
  - `scripts/measure_2901_offcalendar.py`: the mirror date audit, committed and hashed.
- **Then**: the frozen declaration (hashes, the trial-row table and M, the #2829 register entry), followed by **PR
  C**: the one run, the result document, and closing #2901.

Rung: judgement artefact (spec), so Codex ckpt-1 before merge.
