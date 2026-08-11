# Prospective forecast calibration and drift authority (#2555)

Status: implemented behind an unseeded immutable policy. No policy means no
capital authority.

## Question this answers

For forecasts issued recently under one exact model/setup/exit-policy scope,
did the stated target/stop/timeout probabilities agree with what subsequently
happened, and are they still better than a no-feature recent base-rate
forecast?

This is not a profitability score. Path returns remain gross, and broker costs
remain a separate execution-time contract.

## Frozen mathematics

For resolved observation `i`, probability vector `p_i`, and one-hot outcome
`y_i`, the normalized three-class Brier score is:

`B = sum_i sum_k (p_ik - y_ik)^2 / (2N)`

The factor of two fixes the three-class score to `[0, 1]`. Brier is a strictly
proper scoring rule: truthful probabilities minimise expected loss. See
[Gneiting and Raftery (2007)](https://doi.org/10.1198/016214506000001437).

Absolute error is not sufficient. The same cohort is scored using its
empirical target/stop/timeout frequencies as a constant no-feature forecast.
Forecast skill is:

`skill = 1 - model_Brier / base_rate_Brier`

Capital policy must set a strictly positive minimum skill. Equality with a
no-feature forecast is not useful predictive evidence. A single-class recent cohort
has a perfect base-rate score of zero; skill is undefined and authority fails
closed rather than dividing by zero or claiming an easy win.

Calibration error is the maximum classwise expected calibration error across
target, stop and timeout. Each class uses deterministic equal-frequency
(adaptive) bins; bin count is part of immutable policy. This choice is explicit
because ECE conclusions are sensitive to class conditioning and binning, while
adaptive bins are more stable than fixed-width bins in the empirical study
[Measuring Calibration in Deep Learning](https://arxiv.org/abs/1904.01685).
Histogram calibration is sample-inefficient, so minimum sample and bin count
cannot be treated as decoration; see
[Verified Uncertainty Calibration](https://arxiv.org/abs/1909.10155).

## Refusals and denominators

Only `target_first`, `stop_first`, and `timeout` enter Brier/calibration scores.
`ambiguous` and `unresolved` are explicit terminal refusal rates. Forecasts
without a terminal row are the pending rate. All three use total forecasts in
the recent decision-time cohort as denominator and each has a preregistered
maximum.

An assessment passes only when all of these pass:

- minimum resolved sample;
- maximum normalized Brier score;
- minimum non-negative Brier skill versus recent class frequencies;
- maximum classwise adaptive-bin calibration error;
- maximum ambiguous, unresolved, and pending rates.

There is deliberately no default policy row. Thresholds must be registered
with an evidence reference and effective time; changing a threshold requires a
new policy identity.

## Recency, identity, and storage

The cohort is selected by forecast decision date inside the policy's recent
window and grouped by forecast-policy, calibration model, exact immutable
calibration record, setup, and exit policy. The calibration ID is part of the
scope so one recalibration can never authorise another that happens to reuse a
model label. Outcomes must match the current resolver and quarantine-input
versions.

Immutable assessment rows are keyed by the exact policy/scope/version tuple and
a hash of forecast IDs, probabilities, terminal outcome IDs, and states. An
unchanged daily run reuses that evidence row and updates one bounded current
pointer. A forecast resolving or entering/leaving the recent cohort changes the
hash and creates one new evidence row. No bars, feature vectors, polling rows,
or per-instrument activity feed are copied.

Ranking and execution both require the latest effective policy's matching
assessment to pass and its current pointer to remain within the policy's age
limit. Missing policy, missing assessment, failed assessment, and stale
assessment are separate fail-closed reasons before broker access.
