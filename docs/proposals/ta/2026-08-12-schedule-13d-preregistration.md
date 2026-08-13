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

For a liquid current eToro stock on Nasdaq or NYSE whose clean first active Schedule 13D becomes
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
- public filing date comes only from `sec_filing_manifest.filed_at`; the typed
  blockholder `filed_at` is signature/fallback provenance and is forbidden as
  a public decision clock;
- every reporting person contributes prior issuer/reporting-person history;
- only strictly earlier SEC-manifest public filing dates establish history;
- no earlier active filing, no earlier passive filing and no same-timestamp
  peer for any attached reporting person;
- one accession is counted once even when it has joint reporters;
- mapped currently tradable eToro `Stocks` type on exchange id 4 (Nasdaq) or
  5 (NYSE), with 60 prior sessions and complete outcome coverage.

Conversions, repeats and same-timestamp ambiguity are labelled attribution
groups and never silently mixed into the primary estimate. Date-only rows and
rows carrying optional SEC acceptance timestamps deliberately share the
next-session-open rule:
the daily archive cannot simulate a causal intraday fill, and a more favourable
fill for one subset would make the arms incomparable.

Eligibility is known before entry: at least $5 at the proposed open and at
least $10 million trailing median daily dollar volume over 20 completed
sessions. Missing identity, security type, OHLCV or corporate-action treatment
is a refusal, not a dropped observation.

The security classification is a current eToro snapshot, not point-in-time
proof that every historical security was a common share. The historical result
therefore remains survivor-biased and cannot promote capital even if every
return test passes.

## Returns, controls and multiple testing

Execution uses raw open and close. Total return applies the change in
`adj_close / close` across those execution dates, so splits and distributions
are not silently ignored. The primary result charges 50 bps round trip and also
reports the cost at which expectancy reaches zero.

Three challengers use the same eligible observations and timing:

1. all otherwise eligible 13Ds, showing whether chain cleaning creates the
   claim;
2. one seeded non-event date on the same instrument and calendar month;
3. initial 13Gs, kept separate by Rule 13d-1(b), Rule 13d-1(c), both, or
   unknown and matched without replacement within filing month, fixed price,
   dollar-volume and prior-market-return buckets. A SHA-256 ordering over both
   accessions and seed 2582 resolves ties; unmatched treatments remain in the
   primary result but not that paired comparison.

Random-time candidates are price-covered entry sessions on the same instrument
and in the treatment entry year-month. Any session within ten NYSE sessions on
either side of any 13D entry for that instrument is excluded; an empty set is
reported as unmatched. Initial-13G matching processes treatments in accession
order within each separate rule population. Paired differences are treatment
return minus challenger return and retain the treatment issuer and entry-session
clusters.

For each paired comparison, the evaluator reports the two-sided 95% linear
percentile interval and a one-sided null-centred bootstrap p-value with the
finite-resample plus-one correction. Holm's step-down procedure adjusts the
three predeclared p-values (random time, Rule 13d-1(b), Rule 13d-1(c)); both and
unknown 13G rules remain separately reported attribution only. Passage requires
both a positive paired lower bound and adjusted p-value at most 0.05 for every
gating comparison. This wording replaces the underspecified phrase
"Holm-adjusted confidence bound": Holm defines a multiple-test procedure, not
a unique adjusted interval construction.

The initial-13G challenger is source-feasible: an outcome-free raw-document
census found 10,419 filings with the same basic coverage (7,429 Rule 13d-1(b),
2,263 Rule 13d-1(c), 32 carrying both and 695 unknown). Rule identity comes only
from the structured designation element; narrative mentions elsewhere in the
document cannot classify a filing. Its strong February and
quarterly filing waves are why filing month and rule are exact strata rather
than an undifferentiated placebo.

Market, volatility, exchange, liquidity, price, purpose and prior-return fields
are attribution. They may explain a failure or define prospective risk, but
cannot rescue a failed primary result by selecting a winning slice. Signal
strength monotonicity is reported only for the accession's maximum disclosed
percent of class; it does not authorise a threshold search. Item 4 text is not
classified in v1: its presence and document hash are audited, but a subjective
post-outcome purpose taxonomy cannot enter this trial.

Market matching and attribution use exactly research series 7713, the
`etoro-comparators-2026-07-08-v1` SPY snapshot, on price-only close-to-close
returns. Its `adj_close` is unavailable, so it is not called a total-return
benchmark and cannot support an excess-wealth claim.

The complete modern archive is one historical falsification interval. It does
not claim a pristine terminal holdout: the genuinely untouched interval begins
with filings arriving after this contract. That prospective interval is
required because the price archive is survivor-biased and lacks historical
broker eligibility and observed spreads.

The crossed-cluster bootstrap follows the product-weight construction studied
by Owen and Eckles; their result supports conservative variance estimation for
multi-factor crossed effects rather than treating filings as independent rows.
Holm's original sequentially rejective procedure supplies strong family-wise
error control without assuming independent p-values.

Method sources:

- [Owen and Eckles, *Bootstrapping Data Arrays of Arbitrary Order*](https://doi.org/10.1214/12-AOAS547)
- [Holm, *A Simple Sequentially Rejective Multiple Test Procedure*](https://doi.org/10.2307/4615733)

## Power and admission

The planning target is a 1.0% net ten-session effect, 10% event-return standard
deviation, two-sided 5% alpha and 80% power. The normal approximation requires
785 independent observations. The source census has only 895 events with basic
60-prior/20-later coverage before chain, liquidity and identity refusals, so the
trial is likely underpowered for that target after clustering. This limitation
is declared before returns: a wide interval is `inconclusive`, not evidence of
no effect and not permission to relax filters.

Historical passage requires effective sample size of at least 785, an
adverse-cost clustered lower bound above zero, Holm-adjusted positive
differences versus random time and the separately matched Rule 13d-1(b) and
13d-1(c) challengers, profit factor above one, positive return after removing
the best 1%, no issuer or entry session supplying 20% of gross positive return,
and positive recent stability (the latest and at least two of three
non-overlapping six-month means). Even a pass cannot authorise capital. This
event study reports concurrency but not portfolio drawdown or turnover: those
numbers require a capital-allocation rule that does not yet exist. A pass only
permits a separate bracket/sizing contract and prospective shadow collection.
A failure is retained without a purpose, threshold, horizon or regime rescue.

## Storage and product boundary

The evaluator reads the existing canonical 13D/G document and daily price
archive. It creates no copied raw filing, indicator history, per-bar feature
table or non-firing poll log. Its retained research output is one bounded
aggregate artifact. The Strategies page should continue to show zero approved
strategies unless a later candidate clears every capital gate.
