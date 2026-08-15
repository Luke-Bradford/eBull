# Holdout synthetic-control support (#2737)

## Decision

A holdout result does not run or store the 1,000-member random-entry synthetic
control. Promotion, monitoring, and paper allocation instead replay the control
from exactly one identity-compatible in-sample result. Missing or ambiguous
support fails closed as `synthetic_control_not_run`.

## Why

The synthetic-control runner intentionally executes only in sample. Repeating
the strategy over 1,000 random-entry cohorts on withheld outcomes would spend
1,000 looks at those outcomes and undermine the single preregistered holdout
evaluation. Before this change, every downstream allocation query nevertheless
required `synthetic_control_passed = true` on the holdout row. The runner could
therefore never produce evidence capable of reaching paper allocation.

Copying an in-sample verdict onto a holdout row would obscure provenance, and a
caller-provided support id would permit favourable-result selection. The
database view `strategy_result_control_support` derives candidates from frozen
identity and measurement stamps and returns an id only when the candidate count
is exactly one.

## Compatibility identity

The support row must match the holdout row on strategy id and version, result
scope, ambiguity and quarantine arms, universe and corpus, cost/carry/FX stamps,
sizing, benchmark and return rules, position/outcome/input rule versions, and
metric-set version. It must carry the same immutable universe rule and validated
universe, cover the public runner's full evaluation start, and reach the frozen
holdout boundary.

The evaluated subset is deliberately not required to be identical. The public
runner produces one full in-sample falsification, while every declared recent
window is holdout-only and may evaluate fewer names. The control supports the
fixed strategy and selection process; it is not represented as a control
computed over each withheld window.

Purpose is intentionally not an identity member. Synthetic control is a
falsification performed by the in-sample harness; the withheld result may later
be a `capital_candidate` only if all of its independent promotion evidence and
governance gates pass.

## Enforcement

- Promotion reconstructs the derived support row and applies the existing pure
  synthetic-control refusal function to it.
- The operator API reports that same composed verdict while leaving the
  holdout row's own control fields empty.
- Monitoring and the paper executor require one derived candidate and a passing
  control on that candidate.
- No live-trading path is enabled by this change; live remains fail-closed.

Integration tests cover passing support without copying, failing support,
duplicate candidates, the real historical-promotion transition, and the paper
executor's qualified-evidence path.
