# quant/strategy-menu: what is worth harvesting, and what is not

## When to use

Read this before proposing a strategy family. It is the current verdict per family for an unleveraged, long-real (short CFDs research-only) UK retail account trading at eToro costs. ⚠ Strategy capital is limited to US-listed eToro **Stocks** (`docs/settled-decisions.md`, universe contract). US ETFs may be offered to UK clients only as CFDs. Any ETF sleeve needs an explicit eligibility route (real-asset access per instrument, or a recorded decision) before it is deployable; backtesting it is not blocked. It supersedes the "four things worth building" list at the top of `strategy-evidence.md`; that file stays the deep evidence base. Sources and figures come from `docs/research/2026-10-04-strategy-research-sweep.md`.

## Worth researching: backtest these first

| family | why it can work for us | evidence | our data | expected size, honestly |
|---|---|---|---|---|
| **Cross-asset trend (TSMOM), long-or-cash, monthly** | Behavioural under-reaction and hedging demand; low turnover; diversifies equities | Moskowitz, Ooi & Pedersen 2012. Hurst, Ooi & Pedersen 1880–2013, positive every decade. Caveat: Huang et al. 2020 on per-asset significance | Cross-asset ETFs in the corpus (start dates vary) to 2024-09, plus eToro bars to now. `adj_close` carries dividends | Mostly drawdown reduction and diversification. ⚠ The century study simulated leveraged long/short futures, not a long-or-cash ETF book; its decade-by-decade record and equity correlation do not transfer, so our backtest is the evidence |
| **Profitability / quality (GP/A; QMJ is a quality long-short, not quality × value)** | Underpriced dull profitable firms; long-side premium | Novy-Marx 2013; Asness, Frazzini & Pedersen 2019; survives in Hou, Xue & Zhang | PIT XBRL from 2009 | A few % a year gross long-short, less long-only, after decay |
| **Value (B/M, E/P, CFO/P), within industry** | Overreaction and risk; highest capacity | Fama-French; Asness, Moskowitz & Pedersen 2013. 2007–2020 drawdown | PIT XBRL + prices | Small and regime-dependent; strongest in small caps |
| **Momentum 12-1 with crash scaling** | Slow information diffusion | Jegadeesh & Titman 1993; Daniel & Moskowitz 2016 (crashes forecastable after bear-market rebounds); Israel & Moskowitz 2013 (holds across size) | Prices from 1962 | Real, but turnover sits near the cost bar; needs a buy/hold band |
| **Low volatility / low beta (unlevered)** | Leverage-constrained investors overpay for high beta | Frazzini & Pedersen 2014; Blitz & van Vliet 2007. Construction caveats: Novy-Marx & Velikov 2022 | Prices | Better risk-adjusted return, not higher raw return |
| **Investment / asset growth** | Firms that expand fast underperform | Cooper, Gulen & Schill 2008; q-factor I/A | PIT XBRL | Modest; combine with the above |
| **Not yet assessed (rank before building):** accruals / earnings quality, net issuance / shareholder yield, industry and residual momentum, analyst revisions, ownership crowding | Each has published support; none has a disposition here yet | — | PIT XBRL / prices / 13F (forward) | Unknown until assessed |
| **Multi-factor combination of the above** | Diversification across weakly correlated premia | Fundamental law: IR ≈ IC·√breadth (Grinold & Kahn) | Panel | The realistic target: a steady, small active return on top of the market core |

## Worth doing as filters, not strategies

- **Avoid lottery, attention and meme names** (MAX: Bali, Cakici & Whitelaw 2011; Robinhood herding −4.7% over 20 days: Barber, Huang, Odean & Schwarz 2022). Exclusion is cheap and documented.
- **Avoid high short interest / expensive-to-borrow longs** (Drechsler & Drechsler 2014: anomaly short-leg returns concentrate in high-fee names).
- **Hold through scheduled earnings rather than selling before them** (earnings-announcement premium: Savor & Wilson 2016).
- **Entry timing nudge:** turn of month (McConnell & Xu 2008), only for purchases already planned.

## Not pursued without a new mechanism and new data

| family | reason |
|---|---|
| Day / intraday trading, ORB, gap fades | Edges of a few bps versus our 10–150 bp round trip. In Taiwan, fewer than 1% of day traders were *predictably* profitable net of fees (Barber, Lee, Liu & Odean 2014); that is evidence about human day traders, not a proof that every short-horizon rule fails |
| Short-term reversal (days to weeks) | The most cost-constrained family (Frazzini, Israel & Moskowitz); our S-3 ran at 6.7× the turnover bar |
| Daily TA setups | Our 15-row setup library is ≈ random entry minus cost (09-29); 10 TA strategies net-negative (#2827) |
| PEAD | Gone outside microcaps since ~2006 (Martineau 2022); our sealed test was inconclusive |
| Index inclusion / deletion, January effect | Decayed below our spread (Greenwood & Sammon 2025) |
| Dividend capture | Negative after 15% US withholding |
| Insider purchases as a stand-alone signal | Our sealed opportunistic-minus-routine test: placebo-adjusted −0.72%/month with an interval spanning roughly −5.8% to +3.9% — not demonstrated, not proven negative. Dense history only from 2023. May enter as one input to a multi-factor model |
| Shock / crash shorts | All 8 portfolio sizing arms lost money, drawdowns 41–69% (#2481); gap risk unbounded |
| CFD shorts of overpriced names generally | The shorting premium is concentrated in high-fee names (Drechsler & Drechsler 2014 find it survives lending fees). Our cost is eToro's hard-to-borrow CFD fee, tripled at weekends, plus unbounded tail risk; not tested at our costs |
| End-to-end LLM trading or LLM price targets | Rarely beats buy-and-hold (FINSABER, StockBench); target levels are biased (Bradshaw, Brown & Huang 2013). See `quant/llm-research.md` |
| Stop / target tuning | ATR gives scale, not expectancy (`trade-lifecycle.md` §1) |

## Reopening a closed family

A closed family reopens when at least one of these is new: the mechanism, the data, or a segment the original test did not cover. Record which, in the declaration. (The heading above is shorthand for this rule.)
