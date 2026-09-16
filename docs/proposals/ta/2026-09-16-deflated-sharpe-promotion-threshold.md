# The promotion gate checks that criterion 6 was COMPUTED, never that it PASSED

Issue #2364. Refs #2437 (phase 3 — hands-off live), #2843 (autonomous promotion),
#2240 (the parent programme).

## The defect

`check_promotable` (`app/services/strategy_result.py:1362`) reaches criterion 6 through
`deflation_promotion_refusals` (`:1272`), which fires on exactly three things —
`deflated_sharpe_not_computed` (the probability is NULL), `trial_count_undeclared`,
`trial_register_superseded` — plus criterion 3's `effective_sample_size_not_computed`.
**None of them looks at the value.** A result carrying `deflated_sharpe = 0.006` — a 0.6%
probability that its Sharpe survives the selection-bias correction — is, as far as the gate
is concerned, indistinguishable from one carrying 0.99.

⚠ This is not a deviation from the spec. `docs/proposals/ta/2026-08-07-bounded-backtester.md:717`
writes the gate clause as *"DSR not computed, or computed on an undeclared trial count
(criterion 6)"*, and the code implements that sentence exactly. The gap is in the clause,
which is the same shape #2364 already reports for two other §7 questions. Closing it
therefore amends the canonical clause too, not only the code.

## Full-population verification

⚠ **`strategy_results` is a VIEW restricted to `namespace = 'in_sample'` (40 rows). The
population is `strategy_results_store` (580).** A first draft of this spec measured the
view and reported 24 affected rows; the real figure is 488, and the hold-out namespace —
the one promotion actually reads — was the part excluded. Caught at Codex checkpoint 1.

```sql
select count(*)                                                   -- 580
     , count(*) filter (where deflated_sharpe is not null)        -- 488
     , count(*) filter (where deflated_sharpe > 0.95)             --   0
     , count(*) filter (where deflated_sharpe = 0)                --  13
     , min(deflated_sharpe), max(deflated_sharpe)                 -- 0.0 … 0.5413
     , count(*) filter (where effective_sample_size is null)      --   0
  from strategy_results_store;
-- namespace split: hold_out 540 (464 with a DSR, max 0.5413) | in_sample 40 (24, max 0.4863)
-- purpose:         harness_validation on all 580
-- dsr_model_id:    488 non-null, exactly 1 distinct value
```

Refusal frequency over the in-sample 40, via `result_ledger.stored_result_promotion_refusals`
(the gate's own stored-row replay, not a re-implementation): 40 `harness_validation_only`,
40 `synthetic_control_not_run`, 24 `metric_axis_unproven`, 24 `trial_register_superseded`,
16 `deflated_sharpe_not_computed`, 16 `trial_count_undeclared` — and **zero** about any
DSR's value.

Two limits on what that measures, stated rather than glossed:

- `effective_sample_size` is non-null on all 580 rows, so criterion 3's
  `effective_sample_size_not_computed` fires on nothing **in the stored population**. That
  is the state of these rows, not an invariant: a future degenerate evaluation can still
  produce a NULL, and the refusal stays necessary.
- Every stored row is `harness_validation`, so all 580 also refuse on
  `harness_validation_only`. Those rows are not rehabilitated by future work — they will
  never be promotion candidates. **The exposure is the NEXT population**, the
  `capital_candidate` rows #2833/#2840/#2842 will write, and #2843 puts that decision on
  evidence alone with no person in the loop. The masking today is real but incidental, and
  incidental masking is the weakest form of protection there is.

## Source rule — the threshold and its strictness are both published

Bailey, D. H. and López de Prado, M. (2014), *The Deflated Sharpe Ratio*, JPM 40(5):94-107,
SSRN 2460551 — the paper every equation in `app/services/deflated_sharpe.py` comes from.
Its worked example states the decision rule, and both halves are already transcribed in
`tests/test_deflated_sharpe.py:47-56`:

- *"DSR ≈ … = 0.9004 **< 0.95**"* — the discovery is rejected;
- *"Should the strategist have made his discovery after running only N=46 independent
  trials, **the investor may have allocated some funds**, as DSR would have been 0.9505"*.

**Strictness comes from the companion paper, not from our reading of silence.** Bailey &
López de Prado (2012), *The Sharpe Ratio Efficient Frontier*, §6 "Skillful Hedge Fund
Styles", read at spec time rather than recalled: *"A PSR(0) > 0.95 indicates that a SR is
greater than 0 with a confidence level of 0.95."* Strictly greater, on the same
probability-of-significance statistic the DSR deflates. The gate adopts that form:
promotion requires `deflated_sharpe > 0.95`.

⚠ **What is OURS is the risk tolerance, and it is named as such.** The papers demonstrate a
95% confidence level; they do not mandate that every allocator use it. Adopting the
conventional 5% significance level as *this engine's* promotion policy is a policy choice
on top of a published statistic, frozen as `DSR_PROMOTION_POLICY_VERSION =
"c6-dsr-threshold-0.95-v1"` so a later change of tolerance is a visible, versioned act
rather than a constant edit.

## #2364 item 1 — the minimum sample size, and why the threshold alone is NOT it

A draft of this spec argued that the DSR's `T` is criterion 3's effective sample size, so
the 0.95 threshold subsumes a minimum `n`. **Falsified at checkpoint 1**: the current code
returns `DSR = 0.998` for four trades (returns `[1,1,3,3]`, ESS 4, trial variance 0.01,
M 276, correlation 0). A high DSR at `n = 4` is reachable, so the threshold is not a
sample-size floor and the two checks are independent.

The floor is published, in the same 2012 paper, §4 "Track Record Length", verbatim:

> *"A note of caution is appropriate at this point: Eqs. (11) and (13) are built upon
> Eq. (8), which applies to an asymptotic distribution. CLT is typically assumed to hold
> for samples in excess of 30 observations (Hogg and Tanis (1996)). So even though a
> MinTRL may demand less than 2.5 years of monthly data … the moments inputted in Eq. (13)
> must be computed on longer series for CLT to hold."*

So the gate gains `effective_sample_size_below_minimum`, firing when the effective sample
size is not **in excess of 30** — strictly greater, the paper's own phrasing, and the same
`MIN_EFFECTIVE_SAMPLE_SIZE` constant carries the quote.

⚠ **Applied to the ESS, not the nominal trade count, and that is the conservative
direction.** The paper's caution is about the series the moments are computed on, which
here is the nominal trade series; the ESS is that series after the block bootstrap's
overlap correction, so `ESS ≤ n` always and a floor on the ESS refuses a superset of what
a floor on `n` would. Criterion 3's own rule — *"a nominal n is the number criterion 3
forbids"* — points the same way.

Blast radius, full population: **4 of 580 rows have `ESS ≤ 30`** (2 of them carry a DSR);
min stored ESS is 4.08, and **0 rows have `trade_count ≤ 30`** — so the two floors are not
the same check, which is the point. Rows passing both new checks: **0**.

⚠ Item 1's second half — *"are confidence intervals shown"*, i.e. suppressing a displayed
win rate — is still not answered. A promotion refusal gates promotion, not rendering.

## What this does NOT close
- **#2364 item 2 (baseline comparator) is substantially stale and is not in scope.**
  `evidence_refusals` already carries `candidate_does_not_beat_challengers` over five
  required challenger roles (`strategy_promotion_evidence.py:27`) and
  `synthetic_control_sharpe_below_cohort` over the 1,000-strategy random-entry cohort. The
  residue — `return_vs_buy_and_hold_pct` computed and gated nowhere — is recorded on #2834.
- **The DSR's own calibration is unchanged and still limited**, in three ways the sources
  themselves name: the trial register is a declared historical floor with known missing
  trials (`trial_register.py`), its correlation and variance inputs are estimated from
  measured usable strategies rather than all 276 declared trials, and MinTRL/PSR results
  are asymptotic. A threshold makes the statistic binding; it does not make it better.
- **No model-compatibility check.** Nothing compares a row's `dsr_model_id` against
  `DSR_MODEL_ID`, so a threshold could in principle accept a differently-constructed
  statistic. All 488 stored DSRs share one model id, so nothing is wrong today. Recorded as
  a named residual rather than built here — it is a fourth refusal on a different axis
  (construction provenance, the shape `trial_register_superseded` already has).

## Design

Three refusal codes, because the three states need different operator actions:

- `deflated_sharpe_below_threshold` — measured, and it lost.
- `effective_sample_size_below_minimum` — the DSR's asymptotic basis does not hold, so the
  number it reports is not interpretable at any value. Separate from
  `effective_sample_size_not_computed` for the same reason `deflated_sharpe_not_computed`
  and `below_threshold` are separate: absent and too-small are different broken things.
- `deflated_sharpe_invalid` — present but not a probability in `[0, 1]`, non-finite, or
  unconvertible. `sql/266:110` already calls an out-of-range value *"not a strong or weak
  result, it is an arithmetic failure"*, and the existing `profit_factor_invalid` /
  `profit_factor_not_above_one` pair is the precedent. Reusing one code for both would make
  a broken statistic report as a measured failure — the distinction the vocabulary's own
  comments defend everywhere else.

`deflation_promotion_refusals` gains these as further **independent** `if`s (never `elif`,
never an early return — the function's existing contract is that every reason is returned),
guarded on the value being present: a NULL already draws `deflated_sharpe_not_computed`.

The parameter is declared `object` deliberately — float in memory, `Decimal` off a stored
row — so the comparison converts through `Decimal(str(value))` inside a `try`, matching
`evidence_refusals`' `profit_factor` handling, and compares in `Decimal` so a stored value
is never round-tripped through binary float. Conversion failure, non-finite, and outside
`[0, 1]` all take the `invalid` branch; `0 <= v <= 1` and `v <= 0.95` takes the threshold
branch.

### Every site that must move

1. `app/services/strategy_result.py` — the vocabulary, the constant, the shared function.
2. `app/services/backtest_run.py::_expected_refusals` (`:3081`) — criterion 8 asserts the
   stored refusal set **equals** its independently predicted set, so an unpredicted code
   **aborts the run**. It currently takes `deflated: bool` and must take the value. ⚠ The
   duplication is deliberate (its own docstring: *"a helper shared with the gate would make
   the comparison vacuous"*), so this is a second hand-written comparison on purpose.
3. `app/api/strategies.py::_promotion_refusals` (`:1699`) — hand-reimplements the deflation
   block, so the operator surface would otherwise disagree with the gate.
4. `app/services/strategy_paper_executor.py:340` — pinned-evidence qualification is SQL
   (`AND r.deflated_sharpe IS NOT NULL`) and carries the same computed-not-passed defect.
5. `scripts/audit_2745_in_sample_run.py::_row_refusals` — the same shape again, in an audit.
6. `app/services/strategy_control_plane.py:54` — `GOVERNANCE_GATE_VERSION` is stamped on
   every promotion row, so a gate that refuses more must not label its decisions with the
   version that refused less. Bumped; **0 stored promotions and 0 deployments**, so the
   bump costs nothing today.
7. `docs/proposals/ta/2026-08-07-bounded-backtester.md` — the gate clause at `:717` and the
   C6 acceptance paragraph at `:1254`, both of which still say "computed".

⚠ Recorded, not fixed: sites 3 and 5 are the drift `deflation_promotion_refusals`' own
docstring warns about (*"THE ONE COPY … a second hand-written copy would drift the first
time the deflation rule changed"*). Collapsing them is a refactor, and mixing a refactor
into a gate change makes both harder to review.

## Scope the change actually has

⚠ Readiness in `app/api/strategies.py` refuses when **any required arm in any required
window** refuses, so this bar applies per arm and per window, not to a single headline
number. That is the intended reading — a strategy whose deflated significance holds in one
window and collapses in another has not cleared multiple testing — and it is stated here
because it is a stronger claim than "the candidate must clear 0.95".

## Blast radius

- **No stored row changes.** Refusals are computed, never persisted on `strategy_results`.
- **No preregistration moves.** #2599 freezes `structural_promotion_refusals` only (four
  codes, behind `STRUCTURAL_REFUSAL_POLICY_VERSION`); these codes are outside that set.
- **488 rows gain `deflated_sharpe_below_threshold`** and **4 gain
  `effective_sample_size_below_minimum`** in any surface rendering the live set; `invalid`
  fires on none, because both producers already bound the value. All 580 already refuse on
  `harness_validation_only`; **zero rows are promotable before and zero after.**
- **0 promotions, 0 deployments stored**, so there is no approved-under-the-old-rule
  population to revalidate.
- The change can only ever add refusals. No result becomes promotable that was not.

## Tests (pure logic, no DB)

1. `0.9505` (the paper's own allocated value) → neither new code.
2. `0.9004` (the paper's own rejected value) → `deflated_sharpe_below_threshold`.
3. Exactly `0.95` → refused: the source's acceptance is `> 0.95`.
4. Boundary neighbours: `Decimal("0.950000000000000001")` passes, `Decimal("0.949999999")`
   refuses — and the same values as `float` do not disagree with the `Decimal` verdict,
   which is what the `Decimal`-side comparison exists to guarantee.
5. `0.0`, `1.0` endpoints → refused / accepted respectively.
6. `1.01`, `-0.01`, `float("nan")`, `float("inf")`, `Decimal("NaN")`, `"0.99"`-as-object,
   an arbitrary object → `deflated_sharpe_invalid`, and NOT `below_threshold`.
7. NULL → `deflated_sharpe_not_computed` only; neither new DSR code double-fires.
7a. `effective_sample_size` of `30` refuses, `30.0000001` passes, `None` draws
    `effective_sample_size_not_computed` **only** (the floor does not double-fire), and a
    tiny ESS beside a `0.998` DSR refuses on the floor — the checkpoint-1 counter-example,
    pinned as a test so the argument cannot quietly regress.
8. Every other refusal is still returned alongside (the independent-`if` contract).
9. `_expected_refusals` predicts the same code for the same value — including the
   run-aborting case, so criterion 8's assertion is exercised rather than assumed.
10. The API copy and the shared function agree on one stored row shape.
11. Existing fixtures that use `deflated_sharpe = 0.72` and expect a clean set are updated
    deliberately, not by loosening an assertion.

## Security

No security surface: a pure decision function over already-stored numbers, adding refusals.
It cannot admit anything; the only direction it can move any result is toward refusal.
