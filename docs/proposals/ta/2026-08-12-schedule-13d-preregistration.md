# Schedule 13D public-catalyst preregistration

Date: 2026-08-12  
Issue: #2582  
Machine contract: `contracts/schedule13d-public-catalyst-v1.json`  
Status: frozen for implementation review; outcomes still unopened

## Verdict before measurement

This is a bounded attempt to falsify one mechanism, not a fifth visible
strategy. The current four strategy rows are harness controls and the completed
2022-plus holdout confirms that none should receive capital. This candidate
gets a separate identity and can be rejected without altering those controls.

The old activism literature is only a prior. Brav, Jiang, Partnoy and Thomas
report that roughly half of their 2001-2006 announcement-window abnormal return
occurred before filing, about two percentage points occurred on filing day and
the day after, and the cumulative abnormal return continued rising through day
20. The same paper reports a median activist holding period near a year; it is
not evidence for a scalp. The SEC subsequently shortened the initial 13D
deadline from ten calendar days to five business days, with the modern rule
effective before this structured corpus. The retained 2024-2026 population must
therefore stand on its own.

Sources:

- [SEC modernisation release](https://www.sec.gov/newsroom/press-releases/2023-219)
- [SEC adopting release and economic analysis](https://www.sec.gov/files/rules/final/2023/33-11253.pdf)
- [Brav, Jiang, Partnoy and Thomas](https://business.columbia.edu/sites/default/files-efs/pubfiles/4132/jiang_activism.pdf)

## Exact question

For a liquid US common stock whose clean first active Schedule 13D becomes
public, does buying at the first regular-session open strictly after the filing
date and selling at the tenth session close produce positive total return after
50 basis points round trip?

Ten sessions is the only historical holding horizon. There is no historical
stop or profit target. Searching brackets at the same time as the edge would
change the question and multiply trials. If the result passes, maximum adverse
and favourable excursion may inform a **newly frozen** bracket, which must then
survive untouched prospective shadow evidence. Until that later gate, there is
nothing to paper trade.

## Population and causality

The primary population is one accession per clean campaign:

- exact form `SCHEDULE 13D`;
- every reporting person contributes prior issuer/reporting-person history;
- only strictly earlier `filed_at` timestamps establish history;
- no earlier active filing, no earlier passive filing and no same-timestamp
  peer for any attached reporting person;
- one accession is counted once even when it has joint reporters;
- mapped liquid US common equity with 60 prior sessions and complete outcome
  coverage.

Conversions, repeats and same-timestamp ambiguity are labelled attribution
groups and never silently mixed into the primary estimate. Date-only rows and
the 123 rows with a precise time deliberately share the next-session-open rule:
the daily archive cannot simulate a causal intraday fill, and a more favourable
fill for one subset would make the arms incomparable.

Eligibility is known before entry: at least $5 at the proposed open and at
least $10 million trailing median daily dollar volume over 20 completed
sessions. Missing identity, security type, OHLCV or corporate-action treatment
is a refusal, not a dropped observation.

## Returns, controls and multiple testing

Execution uses raw open and close. Total return applies the change in
`adj_close / close` across those execution dates, so splits and distributions
are not silently ignored. The primary result charges 50 bps round trip and also
reports the cost at which expectancy reaches zero.

Three challengers use the same eligible observations and timing:

1. all otherwise eligible 13Ds, showing whether chain cleaning creates the
   claim;
2. one seeded non-event date on the same instrument and calendar month;
3. initial 13Gs, kept separate by Rule 13d-1(b), Rule 13d-1(c), or unknown and
   matched without replacement on pre-event properties.

Market, volatility, exchange, liquidity, price, purpose and prior-return fields
are attribution. They may explain a failure or define prospective risk, but
cannot rescue a failed primary result by selecting a winning slice. Signal
strength monotonicity is reported only for predeclared continuous source fields;
it does not authorise a threshold search.

The complete modern archive is one historical falsification interval. It does
not claim a pristine terminal holdout: the genuinely untouched interval begins
with filings arriving after this contract. That prospective interval is
required because the price archive is survivor-biased and lacks historical
broker eligibility and observed spreads.

## Power and admission

The planning target is a 1.0% net ten-session effect, 10% event-return standard
deviation, two-sided 5% alpha and 80% power. The normal approximation requires
785 independent observations. The source census has only 895 events with basic
60-prior/20-later coverage before chain, liquidity and identity refusals, so the
trial is likely underpowered for that target after clustering. This limitation
is declared before returns: a wide interval is `inconclusive`, not evidence of
no effect and not permission to relax filters.

Historical passage requires all of the following: adverse-cost clustered lower
bound above zero, profit factor above one, positive return after removing the
best 1%, no issuer/session concentration dependency and no material challenger
or regime contradiction. Even a pass cannot authorise capital. It only permits
a separate bracket/sizing contract and prospective shadow collection. A failure
is retained without a purpose, threshold, horizon or regime rescue.

## Storage and product boundary

The evaluator reads the existing canonical 13D/G document and daily price
archive. It creates no copied raw filing, indicator history, per-bar feature
table or non-firing poll log. Its retained research output is one bounded
aggregate artifact. The Strategies page should continue to show zero approved
strategies unless a later candidate clears every capital gate.
