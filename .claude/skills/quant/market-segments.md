# quant/market-segments: segment before you test

## When to use

Use this when building a research panel, reporting any backtest, or choosing a universe. Stocks do not share one behaviour. An effect measured on "all names" is usually an average of a strong effect in one tranche and nothing (or the opposite) in another.

## The segmentation

Recompute at each formation date from point-in-time data:

1. **Size:** NYSE breakpoints, as in Fama & French.
   - **Micro:** below the 20th percentile of NYSE market cap. About 60% of names but about 3% of value.
   - **Small:** 20th–50th.
   - **Large:** 50th–80th.
   - **Mega:** above the 80th.
   - Use our NYSE set (`exchanges`) with the overlaid market cap.
2. **Cost bucket:** the eToro round-trip spread for the name's price band (`cost_model.py`) as a percent of a typical position. This is the axis that decides whether an effect is ours to take.
3. **Liquidity:** dollar ADV or Amihud ILLIQ terciles, ranked within size.
4. **Volatility:** quintiles of 126-session realised volatility.
5. **Flags:**
   - price under $5;
   - under 36 months since listing;
   - ADR / share class;
   - leveraged or inverse ETF;
   - institutional-ownership tercile (13F, forward-only from 2024);
   - short-interest decile (FINRA, 2021+).
6. **Industry:** FF-12 from SIC. Rank within industry when the signal is accounting-based.

Report every result per **size × cost** cell, with volatility as the conditioning axis, and state the minimum names per cell used.

## What behaves differently where

| tranche | known behaviour | consequence |
|---|---|---|
| Micro / sub-$5 | Most anomalies look strongest here and fail value-weighted (Hou, Xue & Zhang 2020); bid-ask bounce inflates reversal; widest spreads | Exclude by default, or test separately with honest costs |
| Small | Value and long-horizon reversal are concentrated here (Israel & Moskowitz 2013; our 3-year autocorrelation table in `strategy-evidence.md` §2.8a) | Viable only at multi-month holding periods |
| Large / mega | Most efficient; momentum and low-vol still present; cheapest to trade | The default universe for anything traded more than quarterly |
| Low analyst coverage | Slower information diffusion, so stronger momentum, especially for losers (Hong, Lim & Stein 2000) | Coverage proxy: 13F holder count or filings density |
| High MAX / IVOL, retail-heavy | Overpriced; the short leg is the payoff, and it is hard to borrow (Bali et al. 2011; Stambaugh, Yu & Yuan 2012) | Exclude from longs |
| Low institutional ownership | Anomalies stronger (Nagel 2005) | Possible conditioning variable once 13F history accrues |
| IPO < 3 years | Long-run underperformance in small non-VC issues (Loughran & Ritter 1995; Brav & Gompers 1997) | Exclude from longs |
| Leveraged / inverse ETFs | Daily-reset exposure makes multi-day returns path-dependent; they diverge from the multiple of the index, usually adversely in volatile markets (Cheng & Madhavan 2009) | Not held by strategies. Leveraged products also breach the no-leverage rule. The supervised #2838 SVXY/SVOL demo leg is a declared, time-boxed volatility-premium test, not a strategy holding |
| Cross-asset ETFs | Time-series trend works at asset-class level (Moskowitz, Ooi & Pedersen 2012); cross-sectional stock anomalies do not apply | A separate sleeve with its own backtest |

## Horizons

**Intraday and days:** at our costs, effectively untradable.

**Months:** momentum, trend, the earnings-announcement premium.

**Years:** value, profitability, low-vol, the equity premium. These are the horizons where our costs allow an edge to survive.
