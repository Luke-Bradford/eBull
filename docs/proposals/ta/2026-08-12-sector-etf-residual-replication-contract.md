# Sector-ETF residual replication contract

Date: 2026-08-12
Issue: #2522
Status: primary rule transcribed; outcome access refused by point-in-time and
execution-source gaps

## Decision

Do not implement the proposed fixed 3-, 5- and 10-session rules as though they
were Avellaneda and Lee replications. The paper does not use those fixed exits.
It recomputes a stock/sector-ETF residual model daily, opens on a standardised
residual excursion and closes when the score returns toward equilibrium or the
estimated process stops meeting its speed test.

The exact published rule is worth preserving as a formula source. It is not
current evidence: its actual-ETF test covers 2002-2007, its thresholds were
selected on an earlier part of that sample, average reported Sharpe for
2003-2007 was 0.6, and the authors explicitly report degradation. No result is
opened until the source gaps below are closed or a separately named adaptation
is preregistered with its own prior and trial identity.

Primary source: Avellaneda and Lee, [Statistical Arbitrage in the U.S. Equities
Market](https://math.nyu.edu/~avellane/AvellanedaLeeStatArb20090616.pdf),
especially sections 3-5 and appendix 9.

## Published calculation

At each decision close `t`, for each stock `i` and its assigned sector ETF
`I(i)`, use the 60 completed daily simple returns ending at `t`:

```text
R_stock[n] = beta_0 + beta_i * R_etf[n] + epsilon[n]
X[k] = sum(epsilon[1:k])
X[n+1] = a + b * X[n] + zeta[n+1]

kappa = -log(b) * 252
m = a / (1 - b)
sigma_eq = sqrt(variance(zeta) / (1 - b^2))
s = (X[60] - centered_m) / sigma_eq
```

The OLS residuals make `X[60] = 0` in the paper's construction. Its final
implementation centres `m` by subtracting the cross-sectional mean of
`a / (1-b)` before calculating the score. The model is admissible only when
`0 < b < 0.9672`, equivalently `kappa > 252/30 = 8.4`; otherwise no new trade
opens and an existing trade closes. This is a maximum estimated characteristic
mean-reversion time of about 30 sessions, not a fixed 3/5/10-session holding
rule. The paper notes an average estimated reversion time around seven days,
but that descriptive result is not a parameter.

The pure mean-reversion actions are:

| state | rule | position |
|---|---|---|
| open long | `s < -1.25` | long USD 1 stock, short `beta_i` USD sector ETF |
| open short | `s > +1.25` | short USD 1 stock, long `beta_i` USD sector ETF |
| close long | `s > -0.50` | unwind both legs |
| close short | `s < +0.75` | unwind both legs |
| invalid model | speed test fails | refuse entry / close existing position |

The `+0.75` short-close asymmetry was chosen because it performed slightly
better in the paper's training period. It must therefore be charged as a
selected historical prior, not presented as a timeless mathematical constant.
The drift-modified score is not a second candidate: the paper reports that it
did not materially improve this horizon and does not present its backtest.

## Published universe, portfolio and fill assumptions

- Universe membership and the USD 1 billion minimum market capitalisation are
  evaluated at the historical trade date, not using today's survivors. This is
  the paper's explicit anti-survivorship rule (section 1, page 4: it contrasts
  capitalisation "at the trade date" with capitalisation when the paper was
  written); Table 3's January 2007 cross-section is an illustrative snapshot,
  not a fixed-universe definition.
- Each stock belongs to one of the paper's 15 sector buckets and is regressed
  against exactly one ETF: `HHH`, `IYR`, `IYT`, `OIH`, `RKH`, `RTH`, `SMH`,
  `UTH`, `XLE`, `XLF`, `XLI`, `XLK`, `XLP`, `XLV` or `XLY`.
- Signals and parameters are recalculated daily. The paper assumes the trade
  fills at the same closing price used to calculate that day's signal, includes
  dividends in P&L, beta-hedges each sector daily and charges 5 bp each time a
  position changes (10 bp round trip).
- Its portfolio is leveraged and sizes each active stock by a shared equity
  fraction. eBull's no-leverage rule and broker-specific stock/CFD economics
  are separate execution constraints; the published return cannot be copied
  into an unhedged single-stock position.

The same-close fill is not executable evidence for eBull. A signal requiring
the completed close cannot submit at that already-consumed close. The faithful
economic adaptation must use the next executable stock and ETF quotes and
charge both legs, or prove a causal pre-close signal/MOC contract as a separate
trial. Optimistic same-close results may be reported only as a paper-comparison
arm and can never authorise capital.

## Current eBull source gate

Already present:

- causal daily price paths and unit-regime quarantine;
- a bounded recent comparator snapshot containing SPY and all eleven current
  Select Sector SPDRs through 2026-07-08;
- pure completed-session market/industry context mathematics;
- prospective security-type, listing and provider-industry observations from
  2026-08-10 onward;
- current quotes and exact strategy/manual-position ownership separation.

Missing for either an exact replication or a defensible recent adaptation:

1. The paper's point-in-time 15-bucket stock-to-ETF mapping. eBull's nine
   provider industries and eleven current SPDRs are different taxonomies; no
   current label may be projected into historical decisions.
2. Point-in-time historical universe membership and a causal market-cap floor
   for the complete stock population, including later-dead names. The current
   research archive is not a survivorship-safe substitute.
3. Next-executable stock and hedge-leg bid/ask evidence at each historical
   decision. A daily candle close cannot prove a two-leg fill or the paper's
   10 bp round trip under eToro.
4. Historical shortability, product identity, CFD carry and side-specific
   broker costs for both the stock and hedge. Missing inputs cannot become zero
   cost or assumed availability.
5. A recent untouched interval after any adapted mapping and execution rule is
   frozen. Reusing the already-open residual/shock outcomes to select mapping,
   score or horizon would be another search.

### Free-source result

A targeted official-source check found no free source that closes the first two
gaps:

- the SEC's [ticker/exchange association
  files](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data)
  are periodically updated search aids for current associations, and the SEC
  explicitly does not guarantee their accuracy or scope;
- Nasdaq Trader's [symbol directory
  definition](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs) describes
  files updated throughout the current day, not a historical point-in-time
  membership archive;
- current Select Sector SPDR holdings are an S&P 500 subset under the modern
  eleven-sector taxonomy, not the paper's historical USD 1 billion stock
  universe or its 15 buckets;
- CRSP's official [research-product
  description](https://www.crsp.org/research/) documents the required permanent
  identity, inactive-security and corporate-event continuity, but it is a
  commercial research data product and is outside the no-subscription boundary.

SEC filing SIC values could support a separately defined causal industry
adaptation for identified filers. They cannot manufacture exchange membership,
dead-name price history or the paper's stock-to-ETF taxonomy, so that route is
not labelled an exact or complete replacement.

These are source failures before they are strategy failures. Consequently the
candidate remains outside `STRATEGY_MANIFEST`, the allocation engine and the
operator page. It creates no table, derived residual history or database row.

## Admissible next step

Run a read-only source census before any outcome calculation:

- prove whether a free, licensable point-in-time sector/universe source can map
  the recent complete population without survivor projection; the current
  official-source result is `not_found`, so a new source must first falsify that
  finding;
- report coverage by session, exchange, security type, price, market cap and
  mapping/refusal reason;
- prove both sides and product/cost contracts for a small current eToro panel;
- predeclare whether the trial is the obsolete 15-ETF replication or a current
  11-SPDR adaptation, plus the causal next-fill rule;
- make the published score-exit rule the primary arm. Fixed 3/5/10-session
  timeouts, unhedged longs or market-plus-sector regressions are separately
  named adaptations and separately charged trials.

Only after that census passes may code calculate outcomes. Promotion would
still require positive conservative after-cost expectancy on later data,
acceptable tails and drawdown, factor neutrality, capacity, and prospective
demo evidence. A high hit rate alone is not an admission rule.
