# Trial-register reconstruction (Gate D-0.1)

Issue: #2600. Refs #2437, #2599. Implements the viability plan's Gate D-0.1:
*"Reconstruct an experiment ledger from scripts, issues, commits and result
identities. Replace the 12-trial promotion denominator with the conservative full
count."*
(`docs/proposals/ta/2026-08-11-portfolio-alpha-viability-plan.md` §6, Gate D-0.)

**Result: `M` moves 122 → 259**, and every family now carries an exactness flag.

## Source rule

Criterion 6 (`strategy-catalogue-and-backtest-validity.md`): the trial count
*"must include every variant evaluated — abandoned branches, manual eyeballing,
and parameter values tried and discarded."*

`app/services/trial_register.py`'s own header fixes the admission test, and this
reconstruction does not change it: **a trial is a search of the data, not a
design.** A rule specified and never run against price data is not a trial; a run
that was discarded, withdrawn, or later called an artefact is.

### ⚠ This register's unit is broader than the textbook DSR unit, and that is settled

Bailey/López de Prado's `M` is nominally the number of candidate strategy Sharpes
over which a maximum could have been taken, which would exclude a diagnostic
hypothesis test. **This repo's register already rejected that narrower reading**:
the merged `short-horizon-search-session-2026-08-09` entry charges *"25 breadth
cells, 12 confluence buckets, 13 individual conditions"* — conditioning
diagnostics, not candidate strategies. Criterion 6's own wording ("manual
eyeballing") is what drives that.

This reconstruction stays consistent with the settled unit rather than
re-litigating it, for two reasons: a mixed population is safe in the direction
that matters (a larger `M` raises `SR_0` and LOWERS the DSR), and switching units
mid-register would make the 101 floor incommensurable with everything added here.
⚠ The cost is that `M` is a count of *searches*, not of candidate Sharpes, and the
DSR computed against it is correspondingly conservative. Anyone reading a **pass**
off this register must know that; a **fail** is unaffected.

Two further consequences that decide several rows below:

1. **Manual eyeballing counts.** A `read` access on the hold-out ledger is an
   eyeball of the hold-out and is counted once.
2. **A recompute counts when it changes the estimand or when the pre-correction
   number was already quoted.** The register already counts #2260's non-causal
   original *and* its causal recomputes separately — *"an artefact is still a
   search."* ⚠ It does NOT count a re-run that reproduces an identical estimand to
   fix an implementation error and whose first number never left the run: nothing
   was selectable. The stored S-1..S-4 re-runs qualify under the first clause —
   they changed the cost model (`static-p75-insession-v1` →
   `...-v2+split-adjusted-max`) and the return basis, both of which move the number.

### What is NOT a separate trial: the robustness fan

`ambiguity_arm` (`best_case` / `worst_case`) and `quarantine_arm` (`admitted` /
`masked`) fan every stored evaluation into four rows. They are **not** four
trials, because the promotion gate requires them to pass jointly —
`check_promotable` refuses on `ambiguity_material` and on
`quarantine_arms_not_compared` (`app/services/strategy_result.py`), so the
flattering arm cannot be selected. A trial is something a maximum could have been
taken over; these are not.

⚠ **The same rule is applied to every family below, including where it costs
count.** The autocorrelation grid's pooled and year-clustered tables are two
inference treatments of the same 28 effects, so they are 28 searches and not 56.

Verified on the full population — every stored evaluation is exactly a 4-fan:

```sql
select fan_size, count(*) as evaluation_groups from (
  select strategy_id, strategy_version, namespace, window_start, window_end, corpus_version,
         cost_model_id, sizing_rule, position_rule_set_version, outcome_rule_set_version,
         input_rule_set_version, metric_set_id, count(*) as fan_size
  from strategy_results_store group by 1,2,3,4,5,6,7,8,9,10,11,12
) g group by 1 order by 1;
--  fan_size | evaluation_groups
--         4 |                55
--         8 |                 3      -- 3 configurations measured TWICE (2 evaluations each)
-- 244 rows = 55*4 + 3*8; 58 distinct configurations; 61 evaluations.
```

## Reconstruction cutoff

**`2026-08-12T07:00:00+00:00`** — `TRIAL_REGISTER_CUTOFF` in
`app/services/trial_register.py`.

It sits immediately after the last search this page counts. Both durable clocks
agree on that instant:

```sql
select max(created_at) from strategy_results_store;      -- 2026-08-12 06:39:47.602459+00
select max(accessed_at) from strategy_holdout_accesses;  -- 2026-08-12 06:39:47.602459+00
```

Any outcome-touching search opened after the cutoff charges itself under the
#2599 declaration contract.

⚠ **The cutoff is a declaration, not an enforcement.** Nothing today intercepts a
read-only script or an ad-hoc SQL session, so a post-cutoff search that never
declares itself is invisible to this register exactly as pre-ledger ones were.
Closing that is #2599's scope (the research-side gate), not this ticket's; the
constant exists so #2599 has a boundary to enforce from.

## Exactness flag

`DeclaredTrial.exactness` is `exact` or `floor`.

- **`exact`** — every arm is individually enumerated by a durable artifact: a
  database census, a code-level grid at the commit that ran it, or a result
  page's own table. The count is the number of searches, not an estimate of it.
- **`floor`** — **the true number of searches is at least this, and the excess is
  not recoverable.** A floored count admits only arms the evidence shows actually
  ran. Where the evidence bounds a range, the floor takes the *smallest* count
  consistent with it; where a family is known to have run more searches than any
  artifact records, that is what the flag says and nothing is invented to cover
  the gap.

⚠ This is narrower than Gate D-0.1's *"prefers a visible overcount to false
precision"*, and deliberately. An unevidenced padding number is not conservative,
it is fictional — it would be indistinguishable from an entry invented to make a
DSR look harder-won, which is the failure the register's `evidence` field exists
to prevent. The honest conservative move is a defensible floor plus a flag saying
it is one.

⚠ **The flag does not change `sharpe_variance`, and that is deliberate.**
`V[{SR_n}]` is keyed by `trial_id`, so a floored family of 101 arms supplies at
most **one** measured Sharpe. That understates the spread of trial Sharpes and so
understates `SR_0`, which RAISES the Deflated Sharpe. It compounds with the
under-count bias the module header already states: **both point the flattering
way, so a DSR computed on this register remains an UPPER BOUND on the honest
one.** The converse still does not hold.

## Correlation between near-duplicate arms

Scope item 3 of #2600. **The register does not adjust for family correlation, and
must not.** Equation (9)'s `implied_independent_trials(rho, M)` already shrinks
`M` to an effective `N`, and `rho` is **measured** off the trials' realised
per-entry-date return series — not declared here (`scripts/verify_2240_statistics.py`
P11 asserts exactly this). Discounting correlated arms inside `M` as well would
apply the same correction twice, in the anti-conservative direction.

So the register's job is to declare **how many searches happened**. Whether eight
per-name-cap arms of one event stream carry eight arms' worth of independent
evidence is equation (9)'s question, and it answers it from data.

## Per-family reconciliation

| family | `trial_id` | arms | was | flag | source of the count |
|---|---|---:|---:|---|---|
| S-1 | `s1-time-series-momentum` | 19 | 1 | floor | hold-out access ledger |
| S-2 | `s2-cross-sectional-momentum` | 19 | 1 | floor | hold-out access ledger |
| S-3 | `s3-mean-reversion-in-trend` | 19 | 1 | floor | hold-out access ledger |
| S-4 | `s4-volatility-compression-breakout` | 8 | 1 | floor | hold-out access ledger |
| #2260 RSI | `rsi30-*` (7 entries) | 7 | 7 | exact | issue #2260; unchanged |
| PEAD | `pead-historical-sue-net-income-v1` | 8 | 1 | exact | result page tables |
| 2026-08-09 session | `short-horizon-search-session-2026-08-09` | 101 | 101 | floor | plan-of-attack §2b; unchanged |
| C-2 sizing stress | `extreme-shock-portfolio-sizing-stress-v1` | 15 | 8 | exact | result page tables |
| Form-4 code-P | `form4-code-p-opportunistic-purchase-v1` | 7 | 1 | exact | result page tables |
| autocorrelation term structure | `autocorrelation-term-structure-2026-08-09` | 28 | — | floor | **new** — code grid at `61fb17da` |
| Roll bid-ask bounce | `roll-bounce-spread-recovery-2026-08-09` | 4 | — | exact | **new** — code grid at `61fb17da` |
| insider first look | `insider-purchase-forward-returns-first-look-2026-08-09` | 4 | — | exact | **new** — code grid at `61fb17da` |
| residual confluence | `residual-confluence-v1-development-arms` | 7 | — | floor | **new** — result page |
| ETF intraday momentum | `etf-intraday-momentum-v1-retained-census` | 4 | — | floor | **new** — retained census |
| sizing-rule attribution | `sizing-rule-attribution-2026-08-12` | 9 | — | floor | **new** — result page |
| | **total `M`** | **259** | 122 | | |

`19 + 19 + 19 + 8 + 7 + 8 + 101 + 15 + 7 + 28 + 4 + 4 + 7 + 4 + 9 = 259`.

### S-1 to S-4 — the hold-out access ledger, not the result store

`strategy_holdout_accesses` is the better source: it records every look at the
hold-out, including ones that wrote no result row. `check_promotable` already
treats it as complete (`holdout_accesses_unrecorded` fires when accesses fall
short of evaluations).

```sql
select access_kind, strategy_id, count(*) as accesses
from strategy_holdout_accesses group by 1,2 order by 1,2;
--  evaluate | s1-time-series-momentum            | 64
--  evaluate | s2-cross-sectional-momentum        | 64
--  evaluate | s3-mean-reversion-in-trend         | 64
--  evaluate | s4-volatility-compression-breakout | 28
--  read     | (each of the four)                 |  1
-- 224 accesses total; the 220 'evaluate' rows are exactly the 220 hold-out result rows.

select strategy_id, count(*) filter (where namespace='in_sample') as in_sample_rows
from strategy_results_store group by 1 order by 1;
--  s1 8 | s2 8 | s3 8 | s4 0
```

Per strategy: `evaluate accesses / 4` hold-out evaluations, `+ in_sample rows / 4`
in-sample evaluations, `+ 1` `read` eyeball (the #2469 audit).

- S-1, S-2, S-3: `64/4 + 8/4 + 1` = **19** each
- S-4: `28/4 + 0 + 1` = **8**

**Flagged `floor`.** Two known populations are not recoverable:

1. Read-only harness runs — `scripts/verify_2240_statistics.py`,
   `scripts/probe_2240_*.py` — measure the same rules over the same corpus and
   create neither a result row nor an access row.
2. **Pre-ledger parameter development.** The four rules' windows, thresholds and
   exit constructions were chosen before `strategy_holdout_accesses` and
   `strategy_results_store` existed. The counts above are evaluations of
   *already-selected* rules; the searches that selected them are unreconstructed.
   This is the single largest known gap in `M`.

The four entries keep their existing `trial_id`s because
`scripts/verify_2240_statistics.py::_SLEEVE_TRIAL_IDS` maps sleeves onto them and
`sharpe_variance` raises on any key the register does not declare.

### PEAD — 1 → 8, exact

`docs/proposals/ta/2026-08-10-pead-result.md` (#2476) enumerates its own arms.
The existing entry charged the family as one; the page shows eight
candidate-bearing results:

1. the preregistered equal-gross long/short 62-session primary;
2. the Long leg; 3. the Short leg (both separately tabulated and quotable);
4. trailing-24-month pooled; 5. trailing-36-month pooled;
6.–8. the declared 5-, 20- and 40-session horizon diagnostics.

The matched middle-SUE control is a control and is not counted.

### C-2 extreme-shock sizing stress — 8 → 15, exact

`docs/proposals/ta/2026-08-11-extreme-shock-portfolio-result.md` (#2481) states
*"Those eight evaluations are now charged to the trial [register]"* — 4 per-name
caps × sector cap on/off. It then reports **seven calendar-year returns for the
1%/25% diagnostic arm** (2020 through 2026-to-frontier) which were never charged.
Era cuts are exactly what the page warns against selecting on (*"do not rescue it
by selecting a cap, threshold, hold, stop, era, sector"*), so they are searches.
8 + 7 = 15.

### Form-4 code-P — 1 → 7, exact

`docs/proposals/ta/2026-08-10-insider-purchase-result.md` (#2480):

1. the purchase-value-weighted long-opportunistic / short-routine primary;
2.–6. the five reported windows (trailing 36, trailing 24, 2024, 2025, 2026 YTD);
7. the equal-weight spread.

The timing-matched placebo is a control and is not counted.

### Autocorrelation term structure — new, 28, floor

`scripts/verify_2437_autocorrelation_term_structure.py`, added 2026-08-09
02:28:07 +0100 (`61fb17da`) — **before** the plan-of-attack committed at 03:13:41
(`dbe5107b`), whose §2b floor enumerates seven families summing to exactly 101
and does not include this grid.

Grid read at the commit that ran it, not at HEAD:

```bash
git show 61fb17da:scripts/verify_2437_autocorrelation_term_structure.py | grep -nE "^HORIZONS|return \"[a-d] "
# 57:HORIZONS = (1, 5, 21, 63, 126, 252, 756)
# 78:        return "a <$5"
# 80:        return "b $5-20"
# 82:        return "c $20-100"
# 83:    return "d >=$100"
```

7 horizons × 4 price bands = **28 cells**. The script prints each cell twice — a
pooled table and a year-clustered table — but those are two inference treatments
of one effect, which the fan rule above excludes from the count.

⚠ **A double-count against the 101 floor is possible and is declared rather than
resolved.** §2b's floor is itemised only to seven family names and cannot be
reconciled arm-by-arm against this grid; both were produced in the same session.
Declaring this family separately may count some arms twice. The alternative —
folding it into an unitemised floor — would count them zero times if §2b never
covered them. Under-counting `M` raises the DSR, so the overcount is the safe
error, and this is the one place the page knowingly takes it. Flagged `floor`
because the number of times the script was run is not recorded.

### Roll bid-ask bounce — new, 4, exact

`scripts/verify_2437_roll_bounce.py` (same commit, `61fb17da`). One
implied-spread test per `cost_model.BANDS` entry; the script's own summary line
is `f"\n{failures} of {len([b for b in BANDS])} bands look like pure bounce."`

```bash
PYTHONPATH=. uv run python -c "from app.services.cost_model import BANDS; print(len(BANDS))"   # 4
```

### Insider purchase forward returns, first look — new, 4, exact

`scripts/verify_2437_insider_forward_returns.py` (same commit). `HORIZONS =
(21, 63, 126, 252)`; one year-clustered excess-return test per horizon against a
matched random-date control. The control is a control, not a candidate.

Distinct from `form4-code-p-opportunistic-purchase-v1`, which is the later sealed
monthly portfolio run (#2480, 2026-08-10) on a different construction.

### Residual confluence — new, 7, floor

`docs/proposals/ta/2026-08-10-residual-confluence-development-result.md`
(candidate `residual-confluence-v1+946d549861cc`, #2499). Six evidenced arms plus
one evidenced-but-uncounted discard:

- 2 development-year primary arms (calendar 2024, calendar 2025). The
  best/worst and quarantine-admitted tables are the robustness fan.
- 2 broad top predicted-EV **decile** cuts, one per year — a decile boundary is a
  threshold searched over the outcome.
- 2 predicted-EV-crosses-zero action-boundary cuts, one per year; the page's
  decisive finding is that these two disagree with the decile cuts.
- **+1** for the discarded diagnostic runs: the page records the intended 2026
  terminal hold-out as *"contaminated by discarded diagnostic runs and permanently
  ineligible as untouched evidence"*. That evidences **at least one** such run and
  bounds nothing above it, so the floor is 1 — a larger number would be invented.

Not counted: the page's own no-model comparator (*"applying the same
instrument-overlap rule without the model"*) is a control; and the raw-shock,
market-only residual and matched-random challenger arms were preregistered and
the page states they were **not executed**.

⚠ Known gap the floor does not cover: only the retained decile and zero-crossing
boundaries are reported, so any other thresholds tried are unreconstructed.

### ETF intraday momentum — new, 4, floor

`docs/proposals/ta/2026-08-10-etf-intraday-momentum-retained-census.md` (#2502)
reports four executed results: `long_only` for SPY, QQQ and IWM, plus a signed
SPY diagnostic.

The preregistration declares two arms (`signed`, `long_only`) × three
instruments = 6, but **an unexecuted design is not a trial** by the admission
test, and the census does not evidence `signed` running for QQQ or IWM. The floor
is therefore the four executed arms, not the six preregistered ones.

The always-long-last-half-hour comparators are controls. The first census attempt
*"selected no rows"* and loaded no outcome; correctly not a trial.

### Sizing-rule attribution — new, 9, floor

`docs/proposals/ta/2026-08-12-sizing-rule-attribution-result.md` (#2430).

- 8 completed arms: 2 non-production sizing rules (`entry_weight_drift_v1`,
  `calendar_month_end_equal_weight_v1`) × 4 strategies. S-4's best/worst rows are
  the ambiguity fan, not separate arms.
- **+1** for the abandoned pass: *"The first monthly run was deliberately stopped
  after review found that the truncated final evaluation date (2026-07-08) was
  being labelled a month-end."* An abandoned branch is a trial by criterion 6. One
  stopped run is evidenced; how many strategies it reached before being stopped is
  not, so the floor is 1.

The `Production return` column is not counted here — those are the
`equal_weight_concurrent_v1` evaluations already charged to S-1..S-4 above.
Counting them again would double-charge the same searches.

Reproduce with:

```bash
PYTHONPATH=. uv run python scripts/verify_2430_sizing_rule_ab.py --window primary-2022-plus
```

## Examined and NOT counted

Each of these touched research data without reading an outcome, so counting it
would inflate `M` and understate the DSR:

| artifact | why not a trial |
|---|---|
| `scripts/verify_2537_c3_feasibility.py` (#2537, C-3) | the result page states *"No return, rank, threshold, weight, factor sort, or portfolio outcome was read to reach it."* |
| `scripts/verify_2482_comparator_overlap.py` | price-source compatibility census; no outcome |
| `scripts/verify_2523_regime_context.py` | cohort/coverage census; no returns |
| `scripts/verify_2582_schedule13d_census.py`, `..._preregistration.py` | source census and a sealed preregistration; C-4 opens its outcome under #2599 |
| `scripts/verify_2426_benchmark.py`, `scripts/verify_2429_total_return.py` | accounting and benchmark-composition verification, not candidate searches |
| `scripts/verify_2437_trading_preflight.py`, `..._observation_storage.py` | broker/storage feasibility; no outcome |
| S-5 support/resistance retest, S-6 Fibonacci retracement | specified, blocked on #2279, never run — unchanged from the existing register |

## What this reconstruction still does not reach

Stated so the next session contests it rather than inherits it as complete:

1. **Pre-ledger S-1..S-4 parameter development** (above) — the largest gap.
2. **Ad-hoc SQL, console sessions and issue-comment tables.** Some outcome-bearing
   figures live only in issue comments; no systematic sweep of those was possible
   within this ticket, and no artifact bounds their count.
3. **Discarded thresholds inside counted families.** Result pages report retained
   boundaries; the boundaries tried and dropped are not recoverable.
4. **The 101/28 overlap** declared above.

All four bias `M` downward, which biases the DSR upward. That is the direction the
module header already warns about and this page does not repair it — it narrows it.

## Version bump

`TRIAL_REGISTER_VERSION`: `trial-register-2026-08-11-r3` → `trial-register-2026-08-12-r4`.

Every stored result carries the register version it was deflated against, and
both promotion gates already reject a superseded one:

- `app/services/strategy_result.py::check_promotable` → `trial_register_superseded`
- `app/api/strategies.py` (data-layer promotion gate) → same refusal

So the bump invalidates every stored promotion claim that names an older
register, as Gate D-0.1 requires. The rows themselves are immutable and are not
touched. Measured before the bump:

```sql
select trial_register_version, trial_count, count(*) from strategy_results_store
group by 1,2 order by 1;
--  trial-register-2026-08-07    |  11 | 108
--  trial-register-2026-08-10    |  12 |  48
--  trial-register-2026-08-11-r3 | 122 |  48
--  (null)                       |     |  40
```

After the bump all 204 register-stamped rows are superseded. That is the intended
effect, and it costs nothing real: **none of them was promotable anyway** — all
244 rows are `universe_basis = survivor_only` and `carry_unmodelled = true`.

## Acceptance

| clause | evidence |
|---|---|
| register version bumped | `TRIAL_REGISTER_VERSION = "trial-register-2026-08-12-r4"` |
| per-family counts with exactness flags | table above; `DeclaredTrial.exactness` |
| reconciliation page with reproducing commands | this page |
| `verify_2240_statistics.py` P8 passes | `PYTHONPATH=. uv run python scripts/verify_2240_statistics.py --curve`; output on #2600 |

⚠ **The acceptance run is itself a post-cutoff search, and is deliberately not
charged.** `--curve` measures S-1 and S-3 over the full corpus. It opens no
hold-out namespace, records no `strategy_holdout_accesses` row and writes no
result row — it is one of the read-only harness runs that the S-1/S-3 entries are
flagged `floor` for in the first place, so it sits inside a bound the register
already declares. Charging the acceptance run of a register reconstruction to
that reconstruction would also be circular. This is stated rather than assumed
because a post-cutoff search that goes unmentioned is precisely what the cutoff
exists to make impossible.
