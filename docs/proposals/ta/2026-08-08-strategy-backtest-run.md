# `strategy_backtest_run` — §3.2, settled against the corpus

Spec for #2394 §3.2, the second of the two jobs
`docs/proposals/ta/2026-08-08-strategy-runner-and-manifest.md` declares. §3.1
shipped in `cc89c63e`. Parent: `docs/superpowers/specs/2026-08-04-ta-strategy-platform-design.md`
§5; criteria from `docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`
§5; the frozen split, the arms and the gate from
`docs/proposals/ta/2026-08-07-bounded-backtester.md` §§3.4, 5.2, 5.4, 6, 9.
Refs #2240, #2288, #2284, #2277.

⚠ **Every figure below is printed by**

```bash
PYTHONPATH=. uv run python scripts/verify_2394_backtest_run.py --all
```

exit 0, on the dev corpus, 2026-08-08 — with **three labelled exceptions**, all
in §§8-9 and all estimates rather than measurements: the four-run cost
comparison (four invocations, not one), the six-pass extrapolation, and the
synthetic-control cost.
Acceptance criterion 10 names them so the distinction cannot erode. Every other
figure, including every share and percentage, is computed by the script; none is
hand-written arithmetic, per the standing rule that a derived statistic written
into prose goes stale silently in the place a reader trusts most.

⚠ **`--arm` measures ONE strategy per invocation, and every figure it produces is
a statement about that strategy.** The default is S-1. Where this document
generalises from it — the ambiguity-arm claim in §6 — the generalisation rests on
a structural check `--runnable` performs over the whole manifest, not on the
single arm.

**What §3.2 says today, in full:** *"Runs the harness for a strategy × namespace
× arm and persists via `result_ledger`. Manual-trigger only: it is expensive, and
its `StrategyIdentity.version` must be stable for a stored row to mean
anything."* Three sentences. The measurement below contradicts the first one at
the unit, and replaces "it is expensive" — which is a guess — with the reason
that actually forces a manual trigger.

## 0. The population, measured

`--population`, over the corpus ∩ §4.0 validated universe:

```
validated universe        6,735 instruments
of which the corpus holds 5,266
bars in window       23,339,583   1962-01-02 … 2026-07-08
  in-sample          17,501,058   74.98%
  hold-out            5,838,525   25.02%
series with in-sample bars only        0
series with hold-out bars only     1,244
series spanning the boundary       4,022
hold-out-only share                23.6%   in-sample population ceiling 4,022 of 5,266
```

The bar split reproduces §5.2's frozen literals to the bar (M14/M18), so the
boundary in `strategy_result.HOLDOUT_BOUNDARY` still describes this corpus. The
arm **compares and fails**; it does not re-fit. §5.2: *"a recomputed boundary
walks forward silently and re-admits hold-out data into training"*.

⚠ **The two namespaces are over different populations, and by 23.6%.** 1,244 of
the 5,266 series hold no bar before the boundary — they list after 2021-06-29 —
so an in-sample result is computed over **at most** 4,022 instruments and a
hold-out result over 5,266. `evaluated_instrument_count` and the gate's
`evaluated_instrument_ids` subset test therefore differ **per namespace**, and a
job that computes the count once and stamps it on both rows is wrong on one of
them. ⚠ 4,022 is a **ceiling, not the count**: warm-up, `not_evaluable` bars and
S-2's minimum cross-section all remove instruments that have in-sample bars but
produce no in-sample position. The count on the row is the measured set: on S-1
masked it is **3,541**, 12% below the 4,022 ceiling, so a job stamping the
ceiling would overstate its own population on every in-sample row. `--arm`
prints "instruments with >=1 in-sample position" for exactly this reason.

⚠ **Zero series end before the boundary**, which is the survivor-only label
(#2288 clause 1) showing up as a shape rather than as a caveat: a corpus that
contained delistings would have series that stop in 1998. It does not.

## 1. Source rules

| decision | rule, and where it comes from |
| --- | --- |
| Fill at the open of `t+1` | Parent §3.5 rule 1. Enforced by `signal_ledger.resolve_fills`; this job does not re-implement it. |
| The 75/25 boundary and its inclusivity | §5.2, bar-weighted, **frozen** in `strategy_result.HOLDOUT_BOUNDARY`. Not recomputed here. |
| A position spanning the boundary is hold-out | §5.2, verbatim. Implemented in `strategy_result.namespace_for_position`. |
| Result identity's members | `sql/262`'s grain comment + `ResultIdentity`. Fourteen fields; §7 below lists who supplies each. |
| Hold-out access is logged | Criterion 5, enforced by `sql/264`'s trigger — a hold-out row with no `evaluate` record is refused by the database. |
| `M`, the declared trial count | Criterion 6. This run originally used version `trial-register-2026-08-10`, **M = 12**. That historical denominator is superseded and its results are now refused: the current register includes a documented 101-arm prior search family and has **M = 113**. ⚠ This is still a conservative floor; it includes abandoned branches and is not derivable from the manifest. |
| The two ambiguity arms | §3.4. Both computed, both reported; a material gap blocks promotion. |
| The two quarantine arms | Criterion 9. Both stored, through `result_ledger.store_*_arm_pair`, which makes the lone-arm state unreachable. |
| A duplicate identity raises | §2 of the runner spec: *"raising, loudly. `DO NOTHING` would hide corpus drift"*. |

⚠ **No threshold, window or ratio is chosen in this document.** Every constant it
uses is imported from a module that already froze it, which is why the tables
below cite module attributes rather than numbers.

## 2. ⚠⚠ The invocation unit is the strategy SET, not one strategy

§3.2 says *"runs the harness for a strategy × namespace × arm"*. It cannot, if
the row is to be judgeable, and the reason is arithmetic rather than
architectural.

Criterion 6's Deflated Sharpe deflates the observed Sharpe by `V[SR_n]`, the
variance of the Sharpes **across the measured trials**. `deflated_sharpe.py`:

```
MIN_MEASURED_TRIALS: Final = 2
```

A single strategy is one measured trial, `V[SR_n]` does not exist, and
`deflate_sharpe` refuses. Equation (8)'s inter-trial correlation needs the same
thing from the other side: it is measured off the trials' realised return series
on the dates **both** traded, so one trial has nothing to correlate with.

So a per-strategy invocation writes rows that carry `deflated_sharpe = NULL`, and
the gate refuses every one of them with `deflated_sharpe_not_computed` — a
refusal the operator cannot act on, because no amount of re-running one strategy
will clear it.

**Rule.** One invocation evaluates **every runnable strategy** in
`STRATEGY_MANIFEST`, and the DSR is computed once across them at the end. A
`strategy_id` param may narrow the set for a debugging run; the job then declares
the DSR absent and says why, rather than writing a row that looks incomplete for
an unexplained reason.

⚠ **What the DSR consumes is per-trade moments and a per-entry-date return
series, per strategy** — the Sharpe variance across trials, and equation (8)'s
correlation off the dates two trials both traded. Both come off the trade list
the evaluation already produced, so the job holds each strategy's trade returns
keyed by entry fill date (criterion 3's cluster key, so the two correlation
constructions in this phase agree about "the same day") until step 2 of §10's
sequence. ⚠ **S-2 has no per-instrument holding period** — its closes are
rebalance drop-outs — but it still produces positions with entry fills and
returns, so its trade axis is the same shape as S-1's. The one thing the job must
not do is build the correlation on a date axis one strategy padded with zeros: a
trial that did not trade on a date carries no pairwise information and the dates
are intersected, not unioned.

⚠ **The register and the manifest agree, and that is checked rather than
assumed.** `verify_2240_statistics.py` carries a hand-written
`_SLEEVE_TRIAL_IDS` map from sleeve label to register trial id, guarded by three
P8 checks against staleness and collision. This job needs none of that: measured
by `--runnable`, **every one of the 4 manifest ids is a declared trial id**, so
`strategy_id` *is* the trial id. The equality is asserted by the arm, so a fifth
strategy landing without a register entry fails there rather than silently
under-counting `M` — which raises the DSR, in the favourable direction.

## 3. S-4's causal bracket makes it runnable; missing providers still refuse

`--runnable`, over `STRATEGY_MANIFEST` rather than a hand-written list:

```
s1-time-series-momentum              per_series       RUNNABLE
s2-cross-sectional-momentum          cross_sectional  RUNNABLE
s3-mean-reversion-in-trend           per_series       RUNNABLE
s4-volatility-compression-breakout   per_series       RUNNABLE
runnable 4 of 4
```

S-4 owns `s4_exit_bracket`: it recomputes ATR14 at signal bar `t` and fixes stop
and target around the next-open fill. The manifest adapter constructs the
version-pinned `ExitLevels` handed to `outcome_resolver.resolve_outcome`. Keeping
that adapter outside the strategy package avoids making the outcome rule-set
version part of every strategy's input hash; it is already a separate result
identity member. A daily bar spanning both levels is resolved twice, once at the
stop and once at the target. No future bar participates in level construction.

Unresolved `price_series_break` dates are loaded once for the corpus instruments.
For each fill, the next strictly later break bounds the old segment at its final
stored bar; a fill on the break date is already in the new segment. Measured
2026-08-10: **79 unresolved boundaries across 68 research-resolved instruments**
would otherwise be available to span. Crossing one produces
`unresolved(series_break)`, never a target or stop result on a different scale.
That position remains counted but deliberately has no mark: a close from the
new scale cannot be compared with its old-scale fill, so it is excluded from
return and equity evidence rather than silently manufacturing a gain or loss.
Its structural hold ends at the first stored bar on the new scale, allowing a
new-scale signal on that boundary or later to form a separate position; the
unpriceable old trade must not truncate the rest of the instrument's evidence.
S-4's signal input is split at the same boundaries: the old segment's final bar
cannot fill across the transition, and ATR/compression/breakout warm-up restarts
inside the new scale instead of carrying pre-break state forward.

The old refusal remains executable as a contract probe. A `level_based` manifest
entry without an `exit_levels` factory is a named exclusion, and
`build_positions` is called to demonstrate the precise reason. It is not skipped
or promoted by convention.

## 4. ⚠⚠ Both namespaces come out of ONE pass — so criterion 5 has to be enforced by the JOB

`namespace_for_position` is a **filter over positions the corpus sweep has
already produced** — a property of the function, not of any strategy. Measured by
`--arm` on S-1 masked, one pass (the *split* is S-1's; the *freeness* is
structural):

```
in_sample   2,456,097   78.34%
hold_out      679,258   21.66%
```

Both numbers came out of the same corpus sweep (§8's timings are for that one
pass). The hold-out arm is therefore **free**, and that is the problem.

Criterion 5's whole mechanism is that looking at the withheld side is rare,
deliberate and logged; `store_holdout_result` makes `accessed_by` and `purpose`
keyword-only with no defaults for exactly that reason, and `sql/264`'s trigger
refuses a hold-out row with no `evaluate` record. But a job that computes both
partitions anyway and writes both turns the access log into **a count of cron
fires**, and `check_promotable`'s criterion-5 clause — every evaluation must have
a recorded access — then passes on rows nobody deliberately looked at.

**Rule.** The job computes and writes the **in-sample** arm by default. The
hold-out arm is written only when the invocation carries both of:

- `holdout_purpose` — a free-text, non-empty, no-default param. It is stored as
  `strategy_holdout_accesses.purpose` and is the audit's only content.
- `holdout_accessed_by` — likewise required and non-defaulted.

⚠ **`accessed_by` cannot be derived from the request, and I checked.**
`pending_job_requests` has a `requested_by` column and `app/api/jobs.py` fills
it (`_identify_requestor`), but the listener dispatches
`invoker(params=validated_params)` and nothing else — the `JobInvoker` contract
is `Callable[[Mapping[str, Any]], None]`, so the requester never reaches the job
body. Plumbing it is a separate change to the job runtime; until then the param
is the mechanism, and the `requested_by` column on the request row is the
corroborating record beside it.

⚠ **The hold-out partition must not be COMPUTED on an in-sample invocation
either**, not merely left unwritten. It costs nothing to compute, so the only
thing separating "we have the number" from "we looked" is the job's own
restraint; a run that computes the hold-out metrics and logs them at INFO has
performed an unrecorded evaluation whatever the table says. The positions are
partitioned, the hold-out side is **counted and discarded**, and the count is the
only hold-out figure an in-sample run emits.

## 5. The per-namespace equity axis — a construction nothing has had to choose yet

`grep -rn namespace_for_position app scripts tests` returns
`verify_2240_holdout_namespace.py`, which **counts** the partition and builds no
curve from it, and `tests/test_strategy_result.py`. **No namespace-scoped equity
curve has ever been built in this repo.** `verify_2240_statistics.py --curve`
runs one curve over the whole 16,236-date axis, both namespaces mixed.

That is a real gap, because `compute_metrics` takes the axis and
`periods_per_year` is measured from it: run a hold-out arm on the full axis and
its CAGR is diluted by 59 years in which it holds nothing.

The obvious answer — each namespace's axis is its own date range — does not
survive §5.2. A position that spans the boundary **belongs to the hold-out and
keeps its true entry fill**, so its leg starts before 2021-06-29 and cannot be
placed on an axis beginning there. Measured on S-1 masked:

```
of the hold-out rows, 2,661 entered BEFORE the boundary (spanning) — 0.39%
earliest hold-out entry   2018-10-03
hold-out axis lead        1,000 calendar days before the boundary
latest in-sample close    2021-06-28   (boundary is 2021-06-29)
```

2,661 spanning positions out of 679,258 hold-out rows, and the earliest reaches
back **1,000 calendar days** — under three years, not decades. That number is
what makes the construction below usable rather than degenerate: had it come back
1974, a hold-out arm containing a 50-year leg would have been a different
problem. The in-sample side lands on 2021-06-28, the last trading day before the
boundary, which is `namespace_for_position` behaving.

**Rule, fixed by construction and stated because no published formulation covers
it.** A namespace's equity axis is the evaluation axis truncated to the closed
span of that namespace's own positions: from the earliest `entry_fill_bar_date`
among them to the latest close-or-mark. ⚠ **Both ends are measured, neither is
named** — the in-sample start is that namespace's earliest entry fill and NOT the
corpus start, because a strategy's warm-up means its first position lands well
after the first bar and an axis padded back to 1962 would dilute exactly the
CAGR this rule exists to protect. For hold-out the span reaches back to the
earliest spanning entry. Both bounds are **measured per run and reported**, never
declared, and the job asserts the in-sample bound is strictly before
`HOLDOUT_BOUNDARY` — a violation means `namespace_for_position` mis-classified,
not that the axis needs widening.

⚠ **`window_start` / `window_end` on the row stay the full evaluation window**,
and the two are not the same thing. `sql/262` is explicit that no CHECK ties the
namespace to the window because *"the window is the EVALUATION window and the
namespace selects within it"*. The axis is how the arm's metrics are computed;
the window is what the arm was cut from. Storing the truncated span in
`window_start` would make two rows over one corpus look like two corpora.

## 6. The arms, and the row count

`--arms`, every factor a `len()` of the thing that defines it:

```
runnable strategies      4   s1-…, s2-…, s3-…, s4-…
namespaces               2   hold_out, in_sample
ambiguity arms           2   best_case, worst_case
quarantine arms          2   admitted, masked
result scopes            2   portfolio, sleeve
product                 64   result rows per full invocation
sleeve scope only       32
```

**`portfolio` scope is out.** It is a statement about a cross-strategy allocator
and nothing in `app/` allocates across strategies. 32 sleeve rows.

⚠ **32 rows is NOT 32 corpus passes**, and the difference is the whole cost
argument:

- the **two namespaces** are one pass (§4);
- the **two ambiguity arms** share one pass for S-1, S-2 and S-3 because their
  non-level close regimes cannot produce an ambiguous touch. S-4 is genuinely
  evaluated twice: the same-bar class is booked at its target in `best_case`
  and its stop in `worst_case`;
- the **two quarantine arms** are genuinely two passes: `masked` and `admitted`
  are different bar sets.

So: **3 non-level strategies × 2 quarantine arms + S-4 × 2 ambiguity arms ×
2 quarantine arms = 10 corpus passes → 32 stored rows.**

For a non-level strategy the second ambiguity row is the same measured population
under the required identity. For S-4 both are real populations. Equal Sharpes
prove a zero gap. If they differ, this runner has no attached random-control
threshold for §3.4's materiality comparison, so the gate records
`ambiguity_arms_not_compared` and remains closed.

## 7. The identity — fourteen members, and who supplies each

`ResultIdentity` hashes fourteen fields into `result_version`, and `sql/262`
stores every one as its own column *"because a hash tells you two rows differ and
never which field moved"*. §4 question 6 of the runner spec asked which the job
must pin. All of them; here is where each comes from.

| field | source | job's choice? |
| --- | --- | --- |
| `strategy_id` | `STRATEGY_MANIFEST` key | no — iterated |
| `strategy_version` | `entry.identity(universe=…, cost_model_id=…).version` | no |
| `result_scope` | `'sleeve'` | fixed, §6 |
| `namespace` | the partition | **param-gated** for hold-out, §4 |
| `ambiguity_arm` | both, §6 | no |
| `quarantine_arm` | both, §6 | no |
| `sizing_rule` | `equity_curve.SIZING_RULE_ID` | no |
| `cost_model_id` | `cost_model.COST_MODEL_ID` | no |
| `corpus_version` | `strategy_result.CORPUS_VERSION` | no |
| `window_start` / `window_end` | frozen default, or `RECENT_EVIDENCE_WINDOWS` | pinned enum only |
| `position_rule_set_version` | `position_builder.RULE_SET_VERSION` | no |
| `outcome_rule_set_version` | `outcome_resolver`'s rule-set version | no |
| `input_rule_set_version` | `price_quarantine.RULE_SET_VERSION` — see below | no |

⚠ **Thirteen of the fourteen are read from a module that froze them, and that is
the design working.** #2447 adds only code-pinned recent windows: the operator
selects an id from `RECENT_EVIDENCE_WINDOWS`, never raw dates, and the exact
dates remain in the result hash. A job that accepted a sizing rule or arbitrary
window would still mint identities that no code path can reproduce.

⚠⚠ **`input_rule_set_version` is the QUARANTINE rule set, and it is NOT the same
thing as `StrategyIdentity.input_rule_set_versions`.** Two similarly-named
concepts that a spec saying "the indicator + quarantine rule sets" would have
merged. `strategy_registry.INPUT_RULE_SETS` is
`{"indicator_series": INDICATOR_SERIES_RULE_SET_VERSION}` — indicator only — and
it is already inside `strategy_version` (#2333), so folding it in here would
hash it twice. The result column is a single string and `sql/262` says what it
is for: *"`input_rule_set_version` is `sql/256`'s third key member and is here
for its reason: re-run the quarantine under a changed rule set and the same
signal resolves differently with the resolver byte-identical."* `sql/256`'s own
comment names `load_masked_series` and `price_quarantine`'s rule set explicitly.
So it is `price_quarantine.RULE_SET_VERSION`, and indicator drift reaches the row
through `strategy_version`.

⚠ **`outcome_rule_set_version` has to be stamped even though no runnable strategy
produces an outcome.** It is a member of the hash and `sql/262` declares it
`NOT NULL` with a non-empty CHECK, so the job reads the resolver's live version
rather than writing a placeholder. A blank would pass `NOT NULL` and silently
merge two results — the #2286 shape the constraint was written against.

## 8. Cost

`--arm`, one arm (S-1, masked, full corpus, whole window), full population, run
**four times** on the same box and the same code:

| phase | run 1 | run 2 | run 3 | run 4 |
| --- | ---: | ---: | ---: | ---: |
| corpus pass | 256.8s | 415.9s | 356.6s | 290.8s |
| — masked bar loading | 84.7s (33.0%) | 142.9s (34.4%) | 136.8s (38.4%) | 84.8s (29.2%) |
| — signals → positions | 148.1s (57.7%) | 229.9s (55.3%) | 184.2s (51.7%) | 178.1s (61.2%) |
| — curve accumulation | 3.8s (1.5%) | 4.5s (1.1%) | 4.1s (1.1%) | 4.7s (1.6%) |
| curve + metrics | 33.4s | 97.9s | 87.8s | 39.7s |
| **TOTAL** | **290.3s** | **513.7s** | **444.4s** | **330.5s** |

3,135,355 positions over 5,266 series, 0 empties — **identical in all four**.

⚠ **The wall clock is not reproducible to better than ~1.8× on this box, and the
spread is reported rather than averaged away.** All four runs did the same work
on the same data and produced the same positions; the box was carrying other
sessions' load. Plan on the slowest figure. What IS stable is the phase *shape* —
29-38% of the time is loading bars, 52-61% is signals→positions, and the curve
accumulation is ~1% in every run — which is the part an optimisation decision
would rest on, and it is the reason the spread does not undermine the section.

**Six passes is 29–51 minutes.** ⚠ That multiplication is an estimate and is
labelled one: only S-1 was measured. S-3 is per-series and comparable; S-2 is
`cross_sectional` and its cost is **not** S-1's, because `s2_select` needs the
panel's scored cross-section resident on each rebalance date rather than one
series at a time. The implementation measures S-2 separately and this section
gets its number.

⚠ **"It is expensive" was never the reason for a manual trigger, and pretending
it was would have got the design wrong.** Half an hour is a scheduled job's
workload, not a human's. The reasons the trigger is manual are §4 (criterion 5
requires a purpose a cron fire cannot supply) and §2 of the runner spec (a stored
row is meaningless if its identity moved between runs). Both are governance, and
neither is affected by making it faster.

⚠ **The block bootstrap is a third of the non-corpus time and scales with
trades**, not with series: 2,000 resamples over 15,731 date clusters. It is
inside `compute_metrics` and is not optional — `effective_sample_size` is
criterion 3's number and criterion 6 consumes it.

## 9. What the job can and cannot close on the gate

`--arm` runs `check_promotable` on a probe row. Measured, not predicted —
**8 refusals**:

| refusal | can this job close it? |
| --- | --- |
| `universe_basis_not_survivorship_free` | **no** — the corpus is survivor-only (#2284) |
| `carry_unmodelled` | **no** — `cost_model.CARRY_BPS` is `None` (#2277) |
| `holdout_never_evaluated` | yes, on a deliberate hold-out invocation (§4) |
| `deflated_sharpe_not_computed` | yes, once the run covers ≥ 2 strategies (§2) |
| `trial_count_undeclared` | yes — `TRIAL_REGISTER.declared_count`, M = 11 |
| `ambiguity_arms_not_compared` | yes — the stored pair supplies `ambiguity_material` |
| `quarantine_arms_not_compared` | yes — both arms stored via the pair writer |
| `synthetic_control_not_run` | **not in this cut** — see below |

⚠ **The row is a WHOLE-WINDOW probe, not a namespace arm, and the substitution is
demonstrated rather than waved through.** `--arm` deliberately builds no
namespace-scoped curve — §5 is the reason: choosing that axis is the
implementation's first decision and nothing in the tree has ever made it. So the
probe's metric set spans both namespaces while its identity has to name one, and
that inconsistency would matter if the gate read a metric *value*. It does not:
blanking criterion 3's block-bootstrap field group and re-gating adds
**exactly `effective_sample_size_not_computed` and removes nothing** — printed
by the arm, and a failure if it ever moves. The gate reads one metric, for
presence.

⚠ Even so, the list is measured on **one** row and is the *bare* worst case: no
DSR, no trial count, no arm comparison supplied. Every entry in the table's
right-hand column is a claim about what a fuller run closes, not something
`--arm` demonstrated. Acceptance criterion 8 requires the implementation to
re-measure the list on **every** written row.

⚠ A side result worth keeping: the same 8 refusals came back from a 300-series
`--limit` run and from the full 5,266-series one. The list is a function of what
the row *carries*, not of how much data went into it — which is why a probe can
answer this question at all.

⚠⚠ **`effective_sample_size` is not free, and it nearly read as if it were.**
`compute_metrics` computes criterion 3's block bootstrap **only when
`bootstrap_seed` is passed** — it defaults to `None`, and the module says so:
*"⚠⚠ `bootstrap_seed` IS REQUIRED FOR CRITERION 3 AND DEFAULTS TO OFF."* The
reason `effective_sample_size_not_computed` is absent from the measured list is
that `--arm` goes through `_Sleeve.report`, which passes
`verify_2240_statistics.BOOTSTRAP_SEED = 20260807` — **a verify-script literal.
Nothing in `app/` owns a block-bootstrap seed.** The job must declare and freeze
one of its own, in `app/`, beside the constant it is an input to; inheriting a
script's literal would make every stored effective sample size a function of a
file no production path imports.

⚠⚠ **The job cannot make anything promotable, and that is correct rather than a
shortfall.** **Three** refusals survive every run this cut can produce:
`universe_basis_not_survivorship_free` and `carry_unmodelled`, both blocked on
work outside this epic (#2284's corpus purchase, #2277's carry measurement), and
`synthetic_control_not_run`, blocked on the stage below. §6 of the
bounded-backtester spec states the intended initial state in those words — *"the
gate's initial state is 'nothing is promotable'. That is correct, not a bug to
work around."* What this job changes is that the refusals become **specific and
few** instead of eight-of-eight.

**`synthetic_control` is out of this cut**, with a reason rather than an
omission. §9's control is *"for ONE strategy"* — the cohort's entries are placed
against that strategy's own holding distribution — so it is 1,000 full-corpus
member evaluations **per strategy**, not a corpus-level constant that could be
computed once and reused. At §8's measured pass cost that is days, and the only
existing cohort lives in a developer cache (`~/.cache/ebull/2240_cohort`, S-1
only) that no job may depend on. It needs its own stage and a persisted cohort;
until then the column is NULL and the refusal is visible.

## 10. Trigger, params, idempotency, transaction boundary

**Trigger.** `POST /jobs/strategy_backtest_run/run`, the existing durable-queue
path. Not in `SCHEDULED_JOBS`. Params-aware — a native `JobInvoker`, not
`_adapt_zero_arg` — with `ParamMetadata` declared so
`validate_job_params(allow_internal_keys=False)` rejects an unknown or malformed
key before the request is queued. Precedent: `sec_13f_quarterly_sweep` and
`sec_n_port_ingest`.

Four registry edits, none of them optional and all of them easy to leave out:
the name constant and invoker in `app/jobs/runtime.py` (`_INVOKERS`, hence
`VALID_JOB_NAMES`, which is what the API's 404 check reads); a new member of
`app/jobs/sources.py::Lane`; an entry in `MANUAL_TRIGGER_JOB_SOURCES` mapping the
job to that lane; and the params in `MANUAL_TRIGGER_JOB_METADATA`.

Params: `strategy_id` (optional, narrows the set — see §2's DSR consequence),
`holdout_purpose` and `holdout_accessed_by`, `trial_register_version` (optional
assertion — refuse if it is not the live register's, so a run cannot silently
deflate against a register that has moved).

⚠ **"Required together" is not expressible in `ParamMetadata`** — it declares
per-key type and requiredness and has no conditional model. So the pairing is
checked in the **job body**, first thing, before any corpus work: exactly one of
"neither supplied" (in-sample run) and "both supplied, both non-empty" (hold-out
run) is legal, and one-of-two is a refusal. Leaving it to the metadata layer
would let a hold-out run start with a blank `purpose`, which is the #2286 shape —
a present-but-empty field passing a presence check.

**Idempotency, and it must be checked FIRST.** `strategy_results_unique
(strategy_id, strategy_version, result_version)` — a re-run of an unchanged
configuration **raises**, per §2 of the runner spec. No `ON CONFLICT`, and none
is to be added: `DO NOTHING` would hide corpus drift, and a compare-every-field
no-op is not worth building before a collision is observed.

⚠ **The identity is fully determined before the corpus is touched** (§7: thirteen
of fourteen fields are module constants, and the fourteenth is the namespace).
So the job computes every `result_version` it intends to write and **refuses up
front** if any already exists. Discovering the collision at INSERT time would
throw away the full half-hour pass, and the operator's remedy — delete the row
deliberately — is the same either way.

**Transaction boundary, and it is NOT per strategy.** §2 puts the DSR across the
whole strategy set, and `StrategyResult.__post_init__` binds `deflated` to
`trial_count` and `deflated_sharpe` while `sql/266` has an all-or-nothing CHECK —
so a row cannot be written first and deflated later. `result_ledger` has no
updater at all; every writer is an INSERT. Therefore:

1. evaluate every strategy × quarantine arm, holding metrics in memory;
2. compute the DSR once, across the measured trials;
3. write, one `conn.transaction()` per arm pair, via the pair writers.

⚠ **This inverts "one strategy's failure does not stop the others", which the
first draft copied from §3.1 without checking whether it still held.** It does
not: the DSR's `V[SR_n]` is a function of *which* trials were measured, so a run
that loses S-2 in step 1 and proceeds would deflate S-1 and S-3 against a
two-trial variance while the run reports itself complete — a **more confident**
number obtained by losing evidence. A strategy failing in step 1 aborts the
invocation. Nothing has been written at that point, which is the reason the
phases are ordered this way rather than the phases being ordered to suit the
failure rule.

**Concurrency.** Reuse `app/jobs/locks.py`'s `JobLock` with a backtest-specific
lane. ⚠ **It is session-scoped `pg_try_advisory_lock(hashtext('job_source:{lane}')::int)`
taken on the lock manager's own connection, and the point of naming that here is
that it must stay so.** The prevention-log entry warns about
`pg_advisory_xact_lock` acquired inside a savepoint being held until the
top-level transaction commits; with the multi-arm write phase above, substituting
one would hold the lane until the last arm pair committed. Do not invent a second
locking vocabulary and do not switch to an xact lock.

⚠ **It must not share `strategy_signal_scan`'s lane.** The two jobs write
different tables from different corpora — the scan reads live `price_daily`, this
reads the frozen research corpus — and blocking the daily scan behind a
half-hour backtest is the exact coupling §3 of the runner spec split them to
avoid.

## 11. Observability

Per run, per `(strategy_id, namespace, ambiguity_arm, quarantine_arm)`:

- the row count written and the `result_version` of each;
- `evaluated_instrument_count` **per namespace** (§0: they differ by 23.6%);
- the position partition and the close-source census (§4, §6) — the latter is
  what keeps the ambiguity claim honest run to run;
- the per-namespace axis bounds (§5), measured, so a widening is visible;
- the refusal list from `check_promotable` for each row, so the operator reads
  *why* nothing is promotable without querying;
- **every excluded strategy with its reason** — including any future level-based
  strategy that omits a causal level factory.

⚠⚠ **Computing the refusal list must not itself touch the hold-out access log.**
`PromotionCandidate.quarantine_arms_compared` has a database-reading helper —
`result_ledger.quarantine_arms_compared()` — and on a `hold_out` identity it
**records a `read` access**, deliberately, because *"looking is the event
criterion 5 governs"*. A job that gated every hold-out row it had just written
would add one read record per row and turn the audit trail into a count of its
own automation. The job supplies the boolean **directly**: it wrote both arms in
the same transaction, so it knows, and it must not ask the database a question it
is the answer to. Same for `ambiguity_material`, which it computes from the pair
it holds.

⚠ A strategy that produced **no row** must be visible, and no index can say so;
the count is emitted against the runnable set the job computed, and a shortfall
**fails the run**. ⚠ **"Shortfall" means against the RUNNABLE set, not against
the manifest** — an entry with no declared outcome producer is an exclusion,
not a failure. The two are distinguished by the runnable computation
itself: a strategy that was runnable at plan time and produced no row is the
failure; one that was never runnable is a line in the exclusion list. Same
construction §3.1 landed after the review bot found its population gate anchored
on the wrong side.

## 12. What this job does not do

- **It touches no broker path**, and no live-data path either. It reads the
  frozen research corpus at `CORPUS_VERSION` through
  `research_price_structure_store.load_masked_series`;
  `strategy_signal_scan` reads live `price_daily` through
  `price_masked_bars`. The two are deliberately different sources and their rows
  are not comparable. ⚠ **The research loader is the only bar source this job may
  use, and S-2 is where that could slip** — assembling a panel is the one place a
  `price_daily` read would look like a convenience rather than a corpus change.
- **It does not store forward outcomes.** It resolves S-4 outcomes in memory for
  the frozen corpus only; a separate forward job owns `strategy_outcomes` and
  must carry the §3.1 spec §7 immature-window rule.
- **It does not run the walk-forward split.** `store_walk_forward_folds` exists
  and `WalkForwardFolds` refuses a partial set; wiring it is a follow-on once a
  result row exists to attach folds to.
- **It does not compute the synthetic control** (§9).
- **It writes nothing to `strategy_signals`.** The result layer reads the corpus
  directly and does not go through the live ledger.

## 13. Residual risk

⚠ **A re-freeze invalidates every stored row and nothing enforces the reread.**
`CORPUS_VERSION` is a literal; bumping it is §5.2's deliberate re-freeze. Rows
written under the old version stay, correctly, but nothing refuses to *compare*
them with new ones — that is a phase-6 read-side concern (question 7 of the
runner spec, "version mixing on read") and this job cannot close it. It can only
make the version visible on every row, which `sql/262` already does.

⚠ **`trial_count` is M for the whole register, not for the strategies this run
measured**, and that is criterion 6's intent — *"including abandoned branches,
manual eyeballing and discarded parameter values"*. A reader seeing `M = 11` on a
row from a 3-strategy run may misread it as an error. It is not, and the run
report says so explicitly beside the number.

⚠ **`--runnable` checks manifest ⊆ register, not the converse, and the asymmetry
is deliberate.** The register declares 11 trials of which 4 are manifest
strategies; the other 7 are RSI variants and spike arms that were searched and
abandoned. Those are exactly what `M` is supposed to count and exactly what
cannot be measured — they have no shipped strategy to run. So a register entry
with no manifest entry is the normal case; a manifest entry with no register
entry is the defect, and that is the direction checked. What this does NOT catch
is a register that has gone stale in the other sense — an entry whose evidence
link is dead. Nothing in this job can check that, and it is the register's own
maintenance problem.

## 14. Acceptance

1. A full invocation writes 32 sleeve rows — 4 runnable strategies × 2
   namespaces × 2 ambiguity arms × 2 quarantine arms. A **runnable** strategy
   that produced no row fails the run; a never-runnable one is a named line in
   the exclusion list (§11).
2. S-4 levels use ATR at signal bar `t`, the next-open fill, fixed 2×/3× ATR
   levels and a 40-bar cap; a level-based entry with no factory is refused.
3. A hold-out row is written **only** when `holdout_purpose` and
   `holdout_accessed_by` are both supplied and non-empty, and
   `strategy_holdout_accesses` gains **exactly one** `evaluate` record per
   hold-out row written and **no `read` records at all** (§11).
4. An in-sample invocation emits the hold-out **count** and no hold-out metric.
5. A colliding `result_version` is refused **before** the corpus pass, not at
   INSERT; and an INSERT collision still raises on `strategy_results_unique`
   rather than being absorbed by an `ON CONFLICT` that does not exist.
6. A strategy failing during evaluation aborts the invocation with **zero rows
   written**, and the DSR is never computed over a set smaller than the one the
   run planned.
7. Every row carries a non-null `effective_sample_size`, from a
   block-bootstrap seed frozen **in `app/`** — not inherited from
   `verify_2240_statistics.BOOTSTRAP_SEED`.
8. `check_promotable` is re-measured on **every** written row and returns exactly
   the refusals §9's table says the job cannot close, and no others. §9's
   measured 8-refusal list is the bare-row worst case, not this criterion.
9. Non-level ambiguity arms are asserted shared. S-4 is measured under both
   daily-OHLC bounds; an unequal pair remains refused until the random-control
   materiality threshold is attached.
10. Every figure in §§0, 3, 4, 5, 6, 9 and the per-arm timings in §8 is printed
    by `scripts/verify_2394_backtest_run.py --all`, exit 0. ⚠ **Three things in
    this document are NOT script output and are labelled where they appear:**
    §8's four-run comparison (four invocations), §8's six-pass estimate
    (arithmetic over an S-1 measurement, with S-2 explicitly excluded from the
    extrapolation), and §9's synthetic-control cost. Each is an estimate and
    none is quoted as a measurement.
