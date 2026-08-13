# Opportunistic insider-purchase preregistration

Status: frozen before outcome measurement for #2480. Parent #2469.

## Hypothesis and honest name

This is a **purchase-only adaptation** of Cohen, Malloy and Pomorski (CMP),
not a replication of their headline return. CMP classify insiders from their
prior trading calendar and report an 82 bp/month value-weight five-factor alpha
for a portfolio long opportunistic buys and short opportunistic sells. Their
buy-only regression difference was about 77 bp/month. eBull deliberately does
not infer a short signal from insider sales in this trial.

The SEC's transaction code `P` means an open-market **or private** purchase.
The free structured dataset does not separate those two cases, whereas CMP say
they excluded private transactions. The candidate is therefore named
"Form-4 code-P purchase" throughout and may not be represented as a pure
exchange-purchase replication.

Primary source: Lauren Cohen, Christopher Malloy and Lukasz Pomorski,
*Decoding Inside Information*, NBER Working Paper 16454 / Journal of Finance
67 (2012), pp. 1009-1043, especially pp. 11-12 and Table IV.

## Frozen source population

The source is the SEC Insider Transactions Data Sets from `2019q1` through the
latest **complete** archive available before the sealed run. Archive filenames
and a digest over their names and SHA-256 values identify the exact source.
They are streamed from a bounded transient cache; no transaction history or
derived indicator series is copied into the application database.

Only a row satisfying every condition below is a purchase observation:

- original Form 4 (`DOCUMENT_TYPE = '4'`), never Form 4/A or Form 5;
- one and only one reporting owner on the accession, with a nonblank reporting
  owner CIK; joint filings are attribution-ambiguous and refused;
- `NONDERIV_TRANS`, transaction code `P`, acquired/disposed code `A`;
- finite positive shares and price, no equity-swap flag and no deemed-execution
  date; and
- a transaction date no later than its filing date, a filing lag of at most
  five calendar days, and no SEC `L` timeliness flag.

CMP include officers, directors and 10% owners. This trial therefore imposes no
post-hoc role, direct/indirect ownership or purchase-size selection. Multiple
qualifying rows in one accession and transaction month are aggregated to one
insider purchase observation with summed disclosed value. Trade value never
determines inclusion or thresholds; it is the frozen split-invariant portfolio
weight described below.

An issuer is mapped to the frozen research corpus by SEC issuer CIK and exact
reported trading symbol against the corpus-native vendor symbol. A unique-CIK
research series is also acceptable. Classification is performed across the
complete SEC source before this mapping, so a missing current research series
cannot erase an insider's prior history and alter their class. An
ambiguous multi-class issuer, unresolved symbol or duplicate archive accession
is refused rather than assigned to the most favourable listed class.

The price corpus is survivor-only and cannot recover delisted outcomes. That
limitation is a permanent promotion refusal for this trial even if the effect
screen is favourable.

## Published classifier, made causal

Classification is fixed at the start of each calendar year `Y`, using only
eligible purchases in `Y-3`, `Y-2` and `Y-1`:

```text
observed(Y)      = at least one eligible purchase in each prior year
routine(Y)       = observed(Y) and intersection(months[Y-3], months[Y-2],
                   months[Y-1]) is non-empty
opportunistic(Y) = observed(Y) and not routine(Y)
unclassified(Y)  = not observed(Y)
```

Every eligible purchase by that owner during `Y` inherits the annual label.
This is CMP's trader-level baseline adapted to purchases only. It does not
label a new or sparsely observed insider "opportunistic" merely because no
routine was visible. The alternative trade-level classifier is another trial
and is not opened here.

## Knowledge time and monthly portfolio

Historical archives expose filing date but not exact acceptance time. Exact
`sec_filing_manifest.accepted_at`, where present, must not be later than the
portfolio formation time. Otherwise the filing date is treated as an
end-of-day knowledge boundary. Live firing requires exact acceptance and
refuses without it.

The portfolio signal month is the **filing** month, never the transaction
month. Firms whose qualifying classified filings were known by month end `t`
enter at the first usable regular-session open in month `t+1` and exit at the
last usable regular-session close in that same month. This is a conservative,
executable rendering of CMP Table IV: a firm with any qualifying purchase is
included once in that class's monthly portfolio. A firm may appear in both
portfolios when distinct insiders from both classes buy it; overlap therefore
cancels rather than being silently reassigned.

The primary monthly return is the executable long-opportunistic/short-routine
spread; both legs pay their own round-trip costs:

```text
opportunistic-minus-routine = purchase_value_weighted_long_net(opportunistic)
                              + purchase_value_weighted_short_net(routine)
```

The paper's market-cap weights cannot be reproduced honestly with this corpus:
historical OHLC is back-adjusted for future splits, SEC share counts are
as-reported, and `price_adjustments` contains no factors with which to put them
on one basis. Multiplying those units would inject future split information.
The frozen executable adaptation therefore weights each firm-month by the sum
of its qualifying, contemporaneously disclosed `shares × transaction price`.
These purchase-dollar weights are causal and split-invariant, but are **not**
CMP market-cap weights and cannot inherit their headline result. Weights are
recomputed independently within each portfolio and sum to one. Equal-weight
results are diagnostic and cannot replace the primary.

## Prices, liquidity, costs and controls

- Outcome evidence starts 2022-01-01; older source rows form the three-year
  classifier only. The frozen equity and comparator corpus ends 2026-07-08.
- Entry price must be at least USD 5. The 20 sessions before entry must be
  usable and their median close-times-volume at least USD 10m.
- Entry and exit use the standing spread/slippage model. Dividends are absent
  from the price corpus and no total-return claim is made.
- SPY and the exact SIC-resolved Select Sector SPDR use comparator snapshot
  `etoro-comparators-2026-07-08-v1`. Missing sessions or mappings are reported,
  not imputed.
- A deterministic matched control uses the same eligible firm in another
  available formation month, matched on calendar year and quarter where
  possible, with identical fill, holding, liquidity and cost treatment. The
  random choice is made before outcome prices are read with seed `2480001`.

## Sealed evidence and gate

The report includes full 2022+, trailing 24/36 months, each recent calendar
year, firm-month counts, active months, after-cost purchase-value-weighted expectancy,
month-clustered confidence interval, win rate, profit factor, worst month,
expected shortfall, drawdown, turnover, gross concurrency/capacity proxies,
market/sector-relative returns and every exclusion reason.

The primary gate is a positive lower 95% confidence bound on the monthly
opportunistic-minus-routine after-cost return. It must also beat the matched
control and pass the standing stability, tail, cost, universe and execution
gates. A 75% hit rate is neither required nor sufficient.

This effect trial defines no ATR target, stop, ratchet or capital sizing.
Passing it can only justify a separately preregistered portfolio/bracket and
forward-shadow trial. Until then it is not added to the strategy manifest and
cannot receive demo capital. Any negative or inconclusive result is retained in
the global trial register; no threshold, lag, lookback, role, size or horizon is
tuned against the same sealed interval.
