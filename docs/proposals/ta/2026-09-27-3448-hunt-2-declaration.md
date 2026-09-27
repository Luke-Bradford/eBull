# #3448 hunt 2: illiquid extreme-loser reversal, next-open entry, under the corrected bar

Refs #3448, #2437 (supervisor correction 2026-09-27 21:50Z), #3387 (hunt 1, closed), #3385 (harness), #3386 (store).
Security: none (a research document; no code, broker, auth or order path).

**Status:** v3, after two Codex ckpt-1 rounds. v1 drew 53 findings and v2 drew 29. Applied:
- a door blocker: a small hunt cannot freeze validation (see "Harness change required");
- power moved onto the excess series;
- the tracker is always total return, with gap and validity refusals and a pinned identity;
- the gate is frozen into the validation declaration;
- the flag-drift disposition;
- the recorded-spread interpretation;
- arithmetic corrections.

## Mandate (#2437, 2026-09-27 21:50Z; "SC n" = its bullet n)
- SC1: every hunt construction declares its holding period, turnover and capital utilisation. Its bar is **≥ 8%/yr
  annualised net excess over the cap-weighted tracker, net of the per-side tariff + RECORDED spreads + a declared
  cost-stress arm**. Power-first is computed at that bar. The rule replaces route A's per-trade OR1 for new
  declarations only.
- SC2: hunt 1 is not reopened or rescored. Its signal may be declared as a new registered trial (hunt 2). The trial
  is charged to the register, frozen before any validation look and validated on 2009-01 → 2021-06-28. The
  tick-bounce explanation is separated by next-open entry, and the cost-stress arm binds.

## Why hunt 2 needs its own discovery row
The harness door pins only a candidate with a discovery outcome **in the same hunt** that BY flagged
(`hunt_door.declaration_numbers`: `pin_*_no_discovery_outcome`, `pin_*_not_by_flagged`). Candidates are owned per
split by one hunt (`evaluate`: `candidate_owned_by_other_hunt`), and `entry_point` is a `CANDIDATE_FIELDS` member.
Hunt 2 is therefore:
1. one discovery row, 1990-01-02 → 2008-12-31;
2. the flag below;
3. on a flag, the validation declaration, frozen before its one look. The split runs 2009-01-02 → 2021-06-28. The
   63-session embargo and the lag put its first formation in April 2009, and the readout states the first scored
   date.

**Prior exposure, declared.**
- Hunt 1's close-entry readout on the same discovery data chose this construction. Hunt 2's discovery p-value is
  therefore not a fresh test: selection used the same data. BY counts the row through the door's global M rule
  (M_inh + every `hunt_trials` row + reserved pins), but counting does not undo the selection.
- Validation is the test. It is not untouched:
  - #2481's 2020+ variants overlap it in time (hunt 1 "Residuals"; counted in the inherited floor).
  - This spec computed and publishes SPY's validation-window return, 0.1587 (see "Design-time arithmetic"). That
    is the tracker, not a trial outcome, and no gate reads it. It is disclosed all the same.

## The trial: `extreme_loser_reversal_illiquid_next_open_v1`
Every field and construction note equals hunt 1's trial table (`2026-09-26-3387-hunt-1-route-a-spec.md`, "The
trial" and the paragraph under it), except these:

| field | hunt 2 |
| --- | --- |
| `hunt_id`, `family` | `hunt-2`, `extreme_move_illiquid` (same signal module and `signal_code_sha256`) |
| `entry_point` | **`open`**: with `lag` 1 and `h` 5 (e = t + lag, x = e + h − 1; `hunt_evaluator`), entry is at the **open of t+1** and exit at the close of t+5 |
| mechanism prediction | the arm beats the control, and `excess_ann` ≥ 0.08 in every base and stress cell (replaces hunt 1's OR1 prediction) |
| competing explanations | (1) print noise at the t+1 open: an illiquid name's first print can be stale or bid-side, so apparent reversal can survive the excluded window; (2) informed selling drifts on (#2481); (3) real post-loss spreads exceed the charge |

**What next-open separates, and what it does not.**
- It excludes close t → open t+1 from every position. That window is where a formation close printed at the bid
  first re-prices.
- Hunt 1 excluded more: it skipped the whole t+1 session, including its intraday.
- It does not prove the bounce resolves at the first print. Explanation (1) stays open.
- Validation is post-decimalisation (tick $0.01, down from 1/16). The bounce's size in the selected population is
  not measured.
- The fill is assumed at the printed open. Opening-auction participation, depth and partial fills are unmodelled.

## Holding period, turnover, capital utilisation (SC1)
- **Holding:** h = 5 slots of weight 1/h (Jegadeesh–Titman's slot framework; h = 5 itself is hunt 1's construction,
  not a literature value). Each position runs from the t+1 open to the t+5 close.
- **Turnover (scheduled):** one slot of 1/h is replaced every session, so the scheduled one-way turnover is
  252/5 = 50.4× the book a year, ≈ 420%/month. That is ~8× the Novy-Marx–Velikov ~50%/month level above which
  anomalies rarely survive costs (`strategy-evidence.md` §4, item 1). The prior is a miss, and costs are the kill
  criterion SC2 names.
  - Realised traded notional differs: missing entries, early terminations and the split's edges.
  - Slot rebalancing is uncosted (hunt 1 residual). Here that residual favours the arm in the gated figure.
- **Utilisation:** inside a slot, the cohort's positions are equal-weighted. A position that never enters stays
  cash; a terminated position's proceeds stay cash to the slot's exit; a fully idle slot is cash at 0%
  (`hunt_evaluator`). Uninvested cash is charged inside `excess_ann`, not excused from it.
  - The readout reports two figures, both descriptive and neither gating: the idle-slot share of slot-sessions, and
    the entered share of cohort positions in non-idle slots.
  - Funding follows the evaluator's slot convention: the book's return on each session is the mean of its slots'
    gross factors − 1 (`hunt_evaluator`), so slots are held at 1/h every session. That implicit daily
    rebalancing is uncosted (hunt 1 residual).

## The bar (SC1), exactly
Per cell, over the grid sessions d on which the tracker has a return:
- `arm_ann` = 252 × mean of the cell's arm book return on d;
- `tracker_ann` = 252 × mean of the tracker return on d;
- **`excess_ann` = `arm_ann` − `tracker_ann`**.

The bar is **`excess_ann` ≥ 0.08**.
- **Arithmetic annualisation.** 252 × the per-session mean is expectancy, the harness's unit
  (`hunt_door.SESSIONS_PER_YEAR`). A compounded rate is not a decision metric here (`cost-aware-viability.md` bans
  CAGR, because it is dominated by the largest winners).
- **Tracker = SPY,** the cap-weighted S&P 500 fund. It is read from `spy_chain_v1`'s Intrader segment: series 7694,
  `unadjusted`, first bar 1993-01-29. Discovery and validation both end before the chain's 2022-05-10 seam.
  - **Always total return:** (close_d + dividend_d) / close_{d−1} − 1, where d−1 is the previous NYSE session.
    Every cell uses it, because the investable tracker pays its dividends. A without-dividend arm cell is
    therefore stricter, never easier.
  - Dividends come from the same series' `dividend` column, on the ex-date row, in its unadjusted price scale
    (SPY has no split history; `market_regime_provider`).
  - The tracker is charged nothing. Its price is already net of the fund's expense ratio, and one buy-and-hold
    entry spread amortises to ≈ 0. That is the pre-check precedent `k_tracker ≈ 0`
    (`2026-09-26-3387-hunt-1-precheck.md`, Bar A).
- **Coverage, measured on the full calendar.** Series 7694 has a bar on every NYSE session
  (`hunt_panel.nyse_sessions`) in 1993-01-29 → 2008-12-31 (4,012 sessions) and in 2009-01-02 → 2021-06-28 (3,143).
  None is missing and none is extra.
  - Through 2021-06-28 there are 7,155 rows, with 7,155 distinct dates (`(series_id, bar_date)` is the primary key).
    Dividends: 115 non-zero, 7,040 zero, 0 NULL. There are 0 non-positive or NULL closes.
  - The first tracker return is 1993-02-01 (d−1 = 1993-01-29). Grid sessions before it have no tracker return. They
    are excluded from all three means, and the readout states the count used.
  - **Refusals, each counting as not met:** any other missing bar on d or d−1, a NULL dividend, or a non-finite or
    non-positive close.
  - An omitted dividend stored as zero is undetectable row by row. The audit is the count: 115 over 28.4 years is
    ≈ 4 a year, SPY's quarterly schedule.
  - Discovery's economic readout therefore covers 1993-02 → 2008. Its significance tests cover the full grid, so
    the two gates do not describe the same sessions. Declared.
- **Tracker identity, per split:** sha256 of the canonical form of the ordered (ISO date, close, dividend) rows
  that fed the split's returns: the session before the first return, through the split's last session. It is
  computed from those same rows, in the same snapshot. Discovery's identity is stored with its readout. The
  validation freeze computes validation's identity and pins it. Reading the benchmark rows is permitted before the
  freeze, because they are not a trial outcome (disclosed above). Validation's readout recomputes the identity, and
  a mismatch refuses.
- **Net of tariff and spreads:** each position pays the per-side tariff and a half-spread band on entry and on
  exit. Both are priced once, at the as-traded entry price (`hunt_compute._half_spreads`); an exit is not re-banded.
  Cost regime: `tariff-2026-counterfactual`.
  - **Base cells** use the frozen band. They are **not** claimed to be net of recorded spreads. The route-A census
    shows the band under the recorded illiquid mean in two of three price bands, by up to 0.217 points round trip
    (≥ $100, n = 48).
  - **Stress cells are the declared cost-stress arm.** Arm positions are charged max(base band, the unknown-price
    band). That band is 1.450% round trip, a 0.725% half-spread per side, and it exceeds every illiquid price
    band's recorded mean and p90 in the census.
  - **This spec's reading of SC1's "RECORDED spreads":** no recorded spread exists at a historical entry, so the
    binding stress charge stands in for it, as a proxy set above every recorded figure. Forward paper is the only
    literal application.
  - That census is one calm 2026 midday capture of 1,187 names. It is unconditional, and it covers neither the
    historical opens nor the post-loss state. Forward paper measures those (below). The stress level is inherited
    and frozen, not fitted to this construction, and it adds nothing where a base band already reaches it.
- **The stress arm binds:** the bar must hold in every base cell **and** every stress cell (three termination
  policies × with/without dividends × {base, stress} = 12 cells).

## Discovery flag → validation
Validation is declared only if **all** of the following hold at the discovery readout. A refused cell, an
unavailable readout, a non-finite input or zero entered arm positions counts as not met.
1. BY flag (programme-wide, `hunt_door.discovery_by_readout`).
2. In every base cell, the active mean (arm − control) > 0.
3. In every base **and** stress cell, `excess_ann` ≥ 0.08 (point estimate).
4. **Power-first at the bar.** Take the canonical cell's discovery **excess series**: arm return − tracker return,
   on the sessions the bar uses. Feed it to the door's power formula (`hunt_door.power_statement`: Newey–West
   long-run variance at the validation split's lag, t > 3, normal approximation) with the validation grid's
   length. The 80% minimum detectable annualised excess must be **≤ 0.08**. The comparison uses the returned
   six-significant-figure value (`hunt_door.sig`), which is the door's own comparison form.
   - Only discovery data and the validation calendar are read.
   - This is additional to the door's active-series power statement, which stays in the declaration unchanged.
   - Like the door's own statement, this is canonical-cell power. It is not power for the stress cells, the
     other policies, the joint rule or DSR.
   - It assumes discovery's variance carries over to a post-decimal market. It measures whether validation could
     tell an 8% excess from zero at the harness's t > 3. It is not the probability that a true 8% excess clears
     the point-estimate gate: at exactly 8%, that is about one half.

Otherwise hunt 2 closes. Each failing condition gets its own label:
- (1) "undetermined";
- (2) "arm does not beat control";
- (3) "below the corrected bar (point estimate)", naming the worst cell;
- (4) "underpowered at the bar".

Nothing is re-screened.

**The gate is frozen into the declaration.** The validation declaration document gains a `hunt_gate` block that
the freeze recomputes like every other number (obligation 122: any stale key refuses). It holds:
- the bar 0.08;
- the 12 cell keys;
- conditions 3 and 4 as evaluated;
- the excess-series power statement with its inputs: canonical cell, sessions used, target count and lag;
- discovery's and validation's tracker identities;
- the sha256 of the runner module that evaluates condition 3 at validation.

The validation readout re-reads the gate from the frozen document, never from code.

**Flag drift.** The flag is read at the discovery readout. The freeze recomputes BY, conditions 3–4 and every number
in ONE snapshot, under the programme lock.
- **Substantive refusals close hunt 2**, with that condition's label, or "undetermined" for the harness codes. They
  are:
  - `pin_*_not_by_flagged`;
  - `pin_*_stale_harness_model`;
  - a recomputed condition 3 or 4 no longer holding;
  - any `declaration_*_stale` key.
- **Procedural refusals are fixed and retried:** the register tripwire (`register_disagrees_with_log:*`) and a
  non-canonical document. A retry recomputes from the snapshot current at that moment. There is no choice of
  snapshot.
- The harness code is frozen from the discovery run until the validation look.

**At validation:** promotion to forward paper needs harness `PASS` **and** condition 3 again on the validation
readout, in every base and stress cell. `PASS` supplies the active-series significance. The 8% excess stays a
point-estimate gate; no confidence that excess exceeds 8% is claimed.

**Before any capital:** forward demo paper under its own declaration, frozen before the first paper trade:
- window, minimum completed trades, fixed endpoint, fill and quote protocol, forward control, the dependence
  treatment and the annualisation denominator;
- power at the 8% bar.

Promotion needs all of these at actual quotes:
- the arm beats its forward control;
- realised `excess_ann` ≥ 0.08;
- the same, recomputed with the stress charge in place of the realised spread wherever that is lower.

## Harness change required: V[SR] for a small hunt
The door's V[SR] is the variance of trial Sharpes over **this hunt's** discovery trials. It refuses below 10
eligible distinct Sharpes (`hunt_inference.MIN_V_POPULATION`; harness spec "DSR"), and `declaration_numbers` turns
that into the freeze refusal `v_population_trial_population_too_small`. **A hunt with a budget under 10 can
therefore never freeze a validation declaration.** Hunt 1's route-A spec carried the same latent blocker; it
closed before reaching the door.

Rule, for every hunt, by construction:
- **Eligibility and dedup are unchanged:** finite, non-ruined series with non-zero variance, identical series
  counted once, exclusions counted.
- **≥ 10 eligible:** unchanged, V = max(measured, floor).
- **2–9 eligible:** V = max(measured, floor), labelled `small_population`.
- **Exactly 1:** V = floor, labelled `floor_only`, with `measured_variance` null.
- **0 eligible:** refuses, as now.

The floor is the harness's existing construction, the mean over the population of 1/T_i: "the IID sampling
variance of a per-observation Sharpe under zero edge" (harness spec "DSR").
- Basis: in Bailey & López de Prado (2014), E[max SR] is formed from the cross-trial variance of Sharpe
  estimates. Under an IID zero-edge null, that variance is the sampling variance.
- **This is a construction, not a demonstrated calibration.** Under serial dependence the true null variance
  exceeds the floor, so the deflation is understated (harness-spec residual). Programme-wide M does not
  compensate for that.
- N̂ = M (≥ 379) is unchanged, so the deflation still charges every search.

The rule changes `hunt_inference`, so `HUNT_HARNESS_MODEL_ID` moves. The implementation PR:
- updates the harness spec's minimum-ten line;
- adds the label and nullable field to the stored form. Old declarations still round-trip, because their
  populations are ≥ 10 or they never froze;
- tests zero, one and nine eligible trials, exclusions and duplicates, the stored-form round trip, and an
  end-to-end one-trial validation freeze.

## Design-time arithmetic (context, not a gate)
From hunt 1's stored close-entry discovery readout (`scripts.run_hunt_1_discovery --readout`), converted per trade
× 252/h. This is an approximation: it ignores idle slots, compounding, varying cohort sizes, trade- versus
capital-weighting and the daily slot rebalancing. Its tracker window (1993-01-29 →) also differs from the trades' (1990 →).

| cell (with dividends) | hunt 1 per trade | ≈ annualised | discovery bar (T_disc 0.0818 + 0.08) | shortfall |
| --- | ---: | ---: | ---: | ---: |
| base | +0.325% | +16.4% | 16.2% | ≈ 0 |
| stress | −0.550% | −27.7% | 16.2% | 43.9 points ≈ 0.871%/trade net |

- T_disc = 0.0818: SPY's arithmetic annualised total return, 1993-01-29 → 2008-12-31. To reproduce, take the mean
  of (close_d + dividend_d)/close_{d−1} − 1 on series 7694 over the window, × 252.
- The stress shortfall is ≈ 0.88 points of gross per trade at the stress charge, on top of hunt 1's ≈ 0.90% gross.
  Next-open entry would have to roughly double hunt 1's gross to clear discovery's stress cells.
- On validation, SPY's bare-window figure is 0.1587, so the stress requirement is ≈ 0.474% net per trade, a
  shortfall of ≈ 1.02 points against hunt 1's discovery stress figure. (This figure is context only. It gates
  nothing, and condition 4 does not read it.)
- Power: hunt 1's arm − control series gives an 80% minimum detectable **active** mean of 0.134 on the validation
  grid (3,079 sessions). The excess series is unhedged against the market, so its variance is unlikely to be
  smaller. Condition 4 may bind on its own.

A miss is the likelier outcome. The run is cheap, and SC2 orders it.

## Residuals
- Everything in hunt 1's "Residuals" and construction notes (relative selection need not select actual losses;
  size, dispersion and turnover are not formable; dividend drops can rank; ties can enlarge the arm), except the
  pre-decimal tick bounce, which is now partly separated (above).
- Survivorship: validation 2009 → 2013-06-21 is conditioned on survival (`SURVIVOR_CONDITIONING_DATE`). This favours
  the arm. It is declared, not corrected, and termination-policy cells cannot recover missing failed firms.
- Stale-mark exits, terminal haircuts, no market impact and uncosted slot rebalancing can each lift the gated
  figure.
- The tracker is SPY's printed close, not an index; tracking error to the S&P 500 is not modelled.
- Candidate identity includes `harness_model_id`. After this PR's model bump, hunt 1's close-entry construction
  would hash as a new candidate. Candidate ownership therefore does not stop a later hunt re-running it. What
  stops it is `HUNT_BUDGETS`, because a hunt opens only through a reviewed PR.

## Budget and delivery
- `HUNT_BUDGETS["hunt-2"] = 1`: one discovery `evaluate` row, no variants. A refusal before registration registers
  nothing. Any registered row counts.
- **Next PR** (behavioural, Codex ckpt-2):
  - the tracker readout: a per-cell block in `hunt_compute`'s statistics with `arm_ann`, `control_ann`,
    `tracker_ann`, `excess_ann`, sessions used, invested share and tracker identity, plus the canonical excess
    series for condition 4. `hunt_panel` loads series 7694's closes and dividends;
  - the small-hunt V[SR] rule;
  - the `hunt_gate` block in the validation declaration (build, freeze-recompute, readout);
  - the budget entry;
  - `scripts/run_hunt_2_discovery.py`: frozen `TrialSpec`, refusal dry-run, `--run`, and the four-condition flag.

  `HUNT_HARNESS_MODEL_ID` moves. Hunt 1's stored outcome keeps its own model id and hash. The BY and V readers read
  only `cells` and the active series, so the added key leaves them unchanged; a test pins that hunt 1's outcome
  still reads and counts.
- **Sequencing (implementation, #3448 slice 2a):**
  - The tracker readout, budget and runner land first.
  - The `hunt_gate` block is built only if discovery flags.
  - Until it is built, `hunt_door.HUNTS_AWAITING_GATE` refuses hunt 2's validation freeze
    (`hunt_gate_not_implemented`), so the generic PASS cannot promote without the gate.
  - The tracker's first bar is pinned (`hunt_panel.TRACKER_FIRST_BAR` = 1993-01-29). A lost leading row raises
    rather than silently shortening the window.
- **Then** the one discovery look. It closes hunt 2 (register entry `hunt-2-discovery`, `HUNT_CLOSED`), or it writes
  the validation declaration.
