# Residual-confluence development result

Status: **rejected in development**. Candidate #2499 must not enter the
strategy manifest, capital picker, forward scanner or order path.

## Frozen identity and run scope

- candidate: `residual-confluence-v1+946d549861cc`;
- research corpus: `paperswithbacktest/Stocks-Daily-Price@2026-07-08`;
- comparator snapshot: `etoro-comparators-2026-07-08-v1`;
- quarantine rules: `price-quarantine-v1+d0423dbd9cb5`;
- static cost model `static-p75-insession-v1` is applied to entry and every
  conditional exit;
- anchored prior-only training; calendar 2024 and 2025 development tests;
- 2,000 circular block-bootstrap resamples, clustered by entry date, seed
  `20260810`;
- development frontier: **2025-12-31**;
- intended 2026 terminal holdout: **contaminated by discarded diagnostic runs
  and permanently ineligible as untouched evidence**.

The verifier is read-only. It created no feature, outcome, strategy, capital or
order rows. The corrected run streamed 4,229,543 development bars and retained only in-memory
candidate observations and aggregate output.

⚠ #2400 later invalidated v1's use of split-adjusted prices as nominal cost-band
keys. The cost-policy change intentionally moved the frozen candidate to
`residual-confluence-v1+32dae77ea948`; that version has not been evaluated.
This page remains the historical rejection record for `+946d549861cc`, not
evidence for the new identity.

## Primary masked-data result

| Test | Ambiguity | Trades | Win rate | Expectancy | 95% block CI | Profit factor | Effective n |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2024 | best | 11,789 | 48.38% | +0.036% | −0.678% to +0.686% | 1.02 | 290.9 |
| 2024 | worst | 9,793 | 49.06% | +0.073% | −0.716% to +0.715% | 1.03 | 245.5 |
| 2025 | best | 787 | 49.17% | +0.223% | −2.642% to +2.815% | 1.08 | 31.7 |
| 2025 | worst | 421 | 43.23% | **−1.332%** | −4.288% to +2.202% | **0.66** | 18.9 |

All four intervals cross zero. The conservative 2025 arm is negative before
carry and FX, which remain unmodelled promotion refusals. Brier scores are
0.647–0.653 and multiclass log loss 1.071–1.079; neither supports a claim of
strong probability discrimination.

Extraction census:

- 960,214 computable feature rows;
- 475,395 rejected because the residual shock was not negative;
- 4,675 refused because the five-session outcome was incomplete by the
  development frontier;
- 1 refused because the 1.5×ATR target could not clear the frozen spread;
- 480,143 resolved negative-shock observations;
- 178 series refused because SEC SIC could not resolve to a sector comparator.

The nominal trade counts materially overstate independent evidence. Same-day
and serial clustering reduces 11,789 accepted 2024 trades to effective n 291.
The worst-case 2025 arm accepts only 421 trades across 29 entry dates and has
effective n 19, making the apparent point estimate both negative and unstable.

## What separated the selected trades from the rest

The model did reject a broadly losing opportunity set. Applying the same
instrument-overlap rule without the model produced after-spread expectancy of
−0.322% in 2024 and −0.330% in 2025 (best ambiguity arm), versus +0.036% and
+0.223% for the model-selected subsets. This is selection activity, but it is
not sufficient evidence of a tradable edge.

The decisive detail is that ranking broke at the exact action boundary:

- the broad top predicted-EV decile realised +0.140% in 2024 and +0.312% in
  2025 under the favourable ambiguity arm;
- under the conservative arm the broad top decile realised +0.127% and
  +0.191%;
- yet the much narrower observations whose predicted EV actually crossed zero
  realised **−1.332%** in conservative 2025, with 36.1% of accepted trades
  entering on one date.

The model therefore showed coarse ranking information but failed precisely
where it claimed a trade should be placed. Predicted positive EV was +0.045%,
while realised expectancy was −1.332%. That is not calibration suitable for
capital.

Nor is there a stable winning signature to retain. Standardised winner/loser
feature differences were generally small in 2024 apart from market stress
(about +0.25 standard deviations). In 2025, the larger differences shifted to
abnormal volume (−0.36), liquidity (+0.25), market stress (+0.25), shock
(+0.20) and candle location (−0.13). Those are outcome-derived diagnostics,
not a coherent invariant rule, and cannot be turned into thresholds on this
data without creating a new overfit candidate.

## Quarantine sensitivity

Admitting stored quarantined values changes 336 observations out of roughly
480k and does not repair the result:

| Test | Ambiguity | Trades | Win rate | Expectancy | 95% block CI | Profit factor | Effective n |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2024 | best | 11,778 | 48.36% | +0.030% | −0.625% to +0.571% | 1.01 | 341.7 |
| 2024 | worst | 9,791 | 49.07% | +0.071% | −0.716% to +0.716% | 1.03 | 245.0 |
| 2025 | best | 779 | 49.17% | +0.223% | −2.716% to +2.798% | 1.08 | 32.3 |
| 2025 | worst | 412 | 42.48% | **−1.486%** | −4.374% to +2.091% | **0.62** | 19.3 |

The failure is therefore not caused by conservative quarantine masking.

## Evidence still missing

This rejection does not pretend to complete the original promotion contract.
The development verifier did not freeze a membership hash for the resolved
instrument universe, did not execute the raw-shock, market-only residual and
matched-random challenger arms, and did not model slippage, carry or FX. It
also does not yet produce portfolio drawdown, expected shortfall or capacity
evidence. Any one of those omissions would independently refuse promotion even
if the point estimate had looked attractive.

In particular, daily OHLC can identify a single intrabar stop touch but cannot
recover the eventual stop-market fill. The development result uses the frozen
stop level plus static spread and therefore does not claim stop-slippage
precision. A future executable study needs intraday quote/trade observations or
an adverse slippage sensitivity arm.

Issue #2505 makes those challenger, attribution and integrity outputs mandatory
for future candidates. The diagnostics above answer why this model failed at
its own action boundary; they do not establish causal importance for any one
feature. A winner/loser difference observed after outcomes is not a formula.

## Corrections made before accepting a result

Diagnostic execution exposed three integrity issues; affected results were
discarded:

1. “exact common sessions” was initially implemented as requiring the
   instrument and both comparators to have identical calendars. The corrected
   implementation forms their exact observed-session intersection and still
   lets a masked instrument close poison every return window that consumes it.
2. A low-volatility 1.5×ATR target can be smaller than the frozen spread. Such
   a bracket is now counted as `uneconomic_bracket` and refused before model
   scoring; a target touch is never mislabeled as a profitable trade.
3. The first corrected processes bounded reported folds to 2024/2025 but loaded
   bars through the 2026 corpus frontier to resolve late-2025 observations.
   That is still holdout access. The verifier now makes 2026 inaccessible and
   refuses outcomes incomplete at 2025-12-31. The prior access is disclosed;
   the intended 2026 holdout will never be described as untouched.

Unit coverage pins scalar/vector feature parity, calendar intersection,
quarantine/missing-data refusal, ambiguous arms, incomplete outcomes,
after-spread bracket viability, anchored training and overlap suppression.

## Decision

Reject v1. Do not compute or report a 2026 result: a development candidate that
already has no positive lower expectancy bound and fails its conservative 2025
arm cannot be rescued by contaminated evidence.

Do not post-hoc change the shock threshold, barriers, model, direction or
preferred ambiguity arm. Any narrower regime suggested by these results is a
new preregistered candidate and needs new future data. The next research work
should prefer a genuinely independent catalyst or microstructure hypothesis,
not another combination of transformations of the same daily OHLCV path.
