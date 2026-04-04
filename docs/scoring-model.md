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
