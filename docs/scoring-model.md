# Scoring Model

## Objective

Turn messy research into a ranked list without pretending the model is magic.

## Score families

### Quality score
Measures:
- profitability
- balance sheet strength
- capital discipline
- cash conversion

### Value score
Measures:
- cheapness vs earnings / FCF / assets
- discount to base case
- downside protection

### Turnaround score
Measures:
- evidence of stabilization
- margin recovery
- debt survivability
- management credibility
- dilution risk

### Momentum / regime score
Measures:
- price trend
- relative strength
- macro fit
- cyclicality risk

### Sentiment score
Measures:
- useful crowd / narrative tone
- retail hype vs concern
- only a minor modifier

### Thesis confidence score
Measures:
- data completeness
- freshness
- agreement between inputs
- number of unresolved red flags

## Suggested modes

### Conservative
- quality: 35
- value: 25
- confidence: 20
- momentum: 10
- sentiment: 5
- turnaround: 5

### Balanced
- quality: 25
- value: 25
- turnaround: 20
- confidence: 15
- momentum: 10
- sentiment: 5

### Speculative
- turnaround: 30
- value: 25
- momentum: 15
- confidence: 15
- sentiment: 10
- quality: 5

Start with **Balanced**.

## Penalties

Apply penalties for:
- stale thesis
- missing critical data
- unresolved legal/regulatory risk
- extreme dilution risk
- high crowd hype unsupported by fundamentals
- excessive debt stress
- poor liquidity or wide spread
- realized risk (v1.2+): high realized volatility / deep drawdown, from risk_v1 3y

## Rewards

Apply additive rewards for:
- strong risk-adjusted return (v1.3+): a Calmar return-ratio reward from the
  risk_v1 3y total-return Calmar (`tr_cagr / |max_drawdown|`). Total return is the
  SEC-derived price return + reinvested per-share dividends (#1635). The reward
  fires off the TOTAL-RETURN Calmar only when `tr_status ∈ {ok, no_dividends}`;
  for `tr_incomplete` it falls back to the price-return Calmar + a caveat. Tiered
  (high/extreme, calibrated to the universe tr_calmar p75/p90), mode-scaled
  (conservative full → speculative reduced). Additive, never multiplicative.

## Model versions

- v1   — return-only momentum
- v1.1 — TA-enhanced momentum (same family weights)
- v1.2 — v1.1 + additive realized-risk penalty (#1633)
- v1.3 — v1.2 + additive Calmar return-ratio reward (#1635 / #1633-vnext). DEFAULT.

Family weights are identical across v1.1/v1.2/v1.3 — each later version only adds
a penalty/reward block, so prior score history is preserved (rank_delta compares
within a model_version).

## Output fields

Each scored stock should emit:
- score timestamp
- score family values
- total score
- model version
- score delta from prior run
- explanation summary

## Rule

The ranking engine is allowed to be opinionated.
The execution engine is not allowed to be sloppy.
