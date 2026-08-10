# Residual-shock confluence candidate preregistration

Status: feature and model definition frozen before outcome measurement for
#2499. Parent #2469.

## Hypothesis and scope

This is a **long short-horizon residual-reversal** test. It asks whether an
instrument-specific negative daily shock has target-before-stop information
when the completed candle location, abnormal volume, liquidity and market
volatility state are consumed together. It is not a vote across technical
indicators and does not inherit evidence from S-1 through S-4.

The economic hypothesis is liquidity provision after a stock-specific shock.
Nagel's short-term-reversal evidence makes market stress a declared conditioner;
Gu, Kelly and Xiu motivate a small interaction of price trend, liquidity and
volatility. Neither paper establishes that this exact retail-costed rule works.

## Frozen identities and universe

- equity corpus: `paperswithbacktest/Stocks-Daily-Price@2026-07-08`;
- comparator snapshot: `etoro-comparators-2026-07-08-v1`;
- US common equities with exact research `instrument_id`, SEC SIC to sector-SPDR
  mapping, market/sector comparator sessions and quarantine coverage;
- signal close at least USD 20;
- prior-20-session median dollar volume at least USD 10m;
- survivor-only and unresolved delisting limitations remain explicit promotion
  refusals; this trial cannot relabel the corpus survivorship-free.

Every constant and identity is emitted by
`app.services.residual_confluence_candidate.definition_json()` and SHA-256
fingerprinted. No measured coefficient or outcome enters that identity.

## Point-in-time features

All returns are log returns on exact common sessions. At completed session `t`,
fit this OLS on exactly `t-126..t-1`:

```text
r_i,d = alpha + beta_market*r_market,d + beta_sector*r_sector,d + epsilon_d
```

The current residual includes the fitted intercept (a correction recorded on
#2499 before outcome access):

```text
residual_t       = r_i,t - alpha - beta_market*r_market,t
                         - beta_sector*r_sector,t
sigma_resid_t    = sample_std(epsilon[t-20..t-1])
shock_z_t        = residual_t / sigma_resid_t
close_location_t = (2*close_t - high_t - low_t) / (high_t - low_t)
abnormal_volume  = ln(volume_t / median(volume[t-20..t-1]))
liquidity        = ln(median(close*volume[t-20..t-1]))
market_stress    = sample_std(r_market[t-20..t-1])
                   / sample_std(r_market[t-252..t-1])
```

The 252-session market history is mandatory. The earlier issue wording that
mentioned only 126 sessions was corrected before measurement. Missing exact
sessions, rank-deficient market/sector regressors, zero residual volatility,
zero-range candles, non-positive volume or non-finite inputs refuse the row.
The factor regression consumes the final 126 observations of that same
252-session market vector; callers cannot supply two inconsistent market
histories. Dollar volume is computed inside the feature engine from the same 20
prior closes and volumes rather than accepted as a second, potentially
inconsistent input.

## Fixed model

The first model is multinomial logistic regression for
`target_first | stop_first | timeout`, with L2 penalty exactly `1.0`. Its
objective is mean multiclass cross-entropy plus `0.5 * L2` times the sum of
squared non-intercept weights. It uses deterministic full-batch gradient descent
(learning rate 0.1, at most 10,000 iterations, maximum absolute gradient at most
1e-9). These optimiser values affect reproduction, not candidate selection.
Every
continuous column is standardised using training-fold mean and sample standard
deviation only. The columns are exactly:

```text
shock_z
close_location
abnormal_volume
log_dollar_liquidity
market_stress
shock_z * close_location * abnormal_volume
```

The interaction is formed from the raw three features and is then standardised
like the other columns. No subset, threshold, sign, transformation, penalty,
tree or barrier search is permitted. There is no post-hoc probability
calibrator in v1: raw multinomial probabilities must pass Brier, log-loss and
calibration checks or the candidate fails.

## Entry and outcome

- direction: long only;
- fill: next executable session open after signal `t`;
- target: entry plus 1.5 times Wilder ATR14 known at `t`;
- stop: entry minus 1.0 times the same ATR14;
- timeout: close of the fifth subsequent trading session, counting the entry
  session as session one;
- a gap through a barrier fills at the first executable open;
- a daily bar touching target and stop is retained in separate best/worst arms
  and never resolved optimistically.

Net expected value is:

```text
p_target*target_payoff - p_stop*stop_loss
+ p_timeout*expected_timeout_payoff
- spread - slippage - carry - FX
```

A feature match is not pending. After a completed bar it either produces a
positive predicted net EV or records a refusal. Before completion there is no
signal. The cohort-level after-cost expectancy confidence interval—not an
invented per-row confidence bound—is the promotion gate.

The executable decision contract, fixed before outcome access, is:

- only rows with `shock_z < 0` enter the candidate population;
- target and stop class payoffs are calculated after applying the frozen static
  half-spread to the entry and the corresponding gross exit;
- the timeout class payoff is the mean after-spread timeout return learned from
  the training fold only;
- act exactly when the three class probabilities times those three net payoffs
  sum to greater than zero; no EV threshold is searched;
- after one signal is accepted for an instrument, later signals in that
  instrument are suppressed through its exit session.

Because the frozen eToro comparator snapshot begins in 2022 and the feature
contract needs 252 prior sessions, the exact anchored evaluation is: train on
all eligible prior rows whose outcomes completed before 2024 and test calendar
2024; extend training through 2024 and test calendar 2025; then train through
2025 and read 2026-01-01 through the frozen 2026-07-08 frontier once as the
terminal holdout. The last five frontier sessions without a complete outcome
are refused. This is a recent-data constraint, not a claim that 2022 itself can
produce signals.

## Validation and one-read rule

- primary relevance begins 2022-01-01;
- use calendar-blocked walk-forward training with purging and embargo at least
  five sessions;
- keep one untouched terminal interval; once read, it is contaminated forever;
- report latest 24/36 months and each recent calendar year separately;
- older history is stress evidence only;
- compare raw-return shock, market-only residual, market+sector residual and
  matched random entries as declared arms with family-wise error treatment;
- bootstrap calendar blocks at least five sessions and cluster same-day events;
- report expectancy and confidence interval, target/stop/timeout calibration,
  profit factor, drawdown, expected shortfall, worst gap, turnover, concurrency,
  concentration and result excluding the best 1%;
- report monotonicity by predicted-EV decile, MAE/MFE, one-session entry delay,
  stressed cost and fixed-seed missed-trade diagnostics;
- every year/regime/liquidity/sector/strength slice is diagnostic. A successful
  post-hoc slice becomes a new preregistered future trial and cannot promote
  this one.

## Promotion and storage

Historical success promotes only to zero-capital forward observation. Capital
still requires a positive lower confidence bound on after-cost expectancy,
stable recent slices, acceptable calibration/tails/capacity, current eToro
eligibility and cost inputs, exact order/position reconciliation and every
standing live gate.

Persist only the immutable definition/evidence, one fired/refused feature
snapshot, one terminal outcome and material execution events. Do not persist
rolling features, every no-op evaluation, polling history or model snapshots.
