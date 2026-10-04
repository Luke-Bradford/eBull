# Strategy research sweep and programme (2026-10-04)

**Status:** current.

- **Inputs:** five research agents (participants, segments, external data, internal data inventory, LLMs in research), the six-lens committee review of the same day, and a first-hand verification of every load-bearing internal claim.
- **Reader:** the agents that run eBull's research and trading, and the operator.
- **Rule:** the operator's research-first rule (2026-10-04). No strategy trades, demo included, until it passes a historical backtest on point-in-time data net of costs.

## 1. What the evidence says, in five lines

1. **Most participants lose to the index, and the reasons are known.**
   - 89.5% of US large-cap funds trail the S&P 500 over 15 years (SPIVA US YE2024).
   - Active traders earn less than buy-and-hold (Barber & Odean 2000).
   - Fewer than 1% of Taiwanese day traders are *predictably* profitable net of fees (Barber, Lee, Liu & Odean 2014). This is about human day traders; it does not by itself rule out every short-horizon rule.
   - 97% of Brazilian day traders who persisted lost money (Chague, De-Losso & Giovannetti 2019).
   - Winners control costs, avoid capacity limits, survive drawdowns and stay disciplined.
2. **Return prediction is weak and decays.**
   - Anomalies lose ~58% of their return after publication (McLean & Pontiff 2016).
   - The average anomaly nets ~4 bps a month after realistic costs, the best ~10 bps and combinations ~20 bps (Chen & Velikov 2023).
   - Volatility is far more predictable than returns (ARCH/GARCH; Welch & Goyal 2008 on return predictors).
3. **"Regular quick gains" are not reliably available at retail cost.** Intraday, short-term reversal, ORB, gap, index-event, January and dividend-capture strategies all die at our spreads. PEAD is gone outside microcaps since ~2006 (Martineau 2022).
4. **What survives for an unleveraged small account is slow and diversified.**
   - **A cheap market core.**
   - **Low-turnover factor tilts:** profitability/quality, value, 12-1 momentum with crash control, low volatility.
   - **Cross-asset trend following:** Hurst, Ooi & Pedersen, 1880–2013, found it positive every decade with ~0 equity correlation, but for leveraged long/short futures. A long-or-cash ETF version is a different strategy and needs its own backtest.
   - **Behavioural avoidance:** do not buy lottery, attention or meme names (Bali, Cakici & Whitelaw 2011; Barber et al. 2022).
5. **Our structural advantages are real but modest:** no external benchmark-relative mandate (we still measure against SPY), no redemptions, no market impact, any holding period. They let us hold slow premia through drawdowns that institutions abandon (Shleifer & Vishny 1997). They do not create an edge by themselves.

## 2. Market tranches: segment before you test

A pattern that holds in one tranche routinely fails in another. Microcaps make anomalies "more apparent than real" (Hou, Xue & Zhang 2020); momentum is weak in the smallest names (Hong, Lim & Stein 2000); value lives in small caps (Israel & Moskowitz 2013). **Every backtest reports per cell of:**

- **Size:** NYSE breakpoints (micro below the 20th percentile, small 20–50, large 50–80, mega above 80).
- **Cost bucket:** the eToro round trip for the name's price band ÷ position size. This is the binding axis for us.
- **Liquidity:** dollar-ADV or Amihud terciles within size.
- **Volatility:** 126-day realised-vol quintiles.
- **Flags:** price under $5, under 36 months listed, ADR, leveraged/inverse ETF, institutional-ownership tercile (13F), short interest.
- **Industry:** FF-12 from SIC.

## 3. What we hold, and what is backtestable

Measured 2026-10-04:

| data | history | point-in-time | backtestable now? |
|---|---|---|---|
| US research price corpus (`research_price_daily`), survivorship-free, incl. cross-asset ETFs (list in `quant/data-map.md`) | 1962 → 2024-09 (Intrader), → 2026-07 (PWB); eToro `price_daily` 2019-12 → now | frozen archive | **yes**. Intrader OHLC are raw but `adj_close` carries splits and dividends to 2024-09-27; the gap is extending total return to the present (#3619). ETF selection must handle closed funds and inception dates |
| XBRL facts (`financial_facts_raw`, per accession) + the PIT bundle (`pit_fundamentals.py`) | filed 2009 → now, subject to the retention sweep (`financial_facts_retention.py`) | **yes** via the bundle (acceptance time; raw `filed_date` is not) | **yes**, 2009+; measure retained coverage first |
| Form 4 insiders | dense only from 2023 | yes | thin before 2023 (sealed test: placebo-adjusted −0.72%) |
| FINRA short interest | 2021-07 → now | **no as stored**: `filed_at` is settlement-date midnight, before publication; lag it | ~5 years: a screen, not an alpha test |
| 13F / N-PORT / 13D-G / DEF 14A | 2024+ | yes (bitemporal) | **no**: forward-accumulating only. Today it feeds only the UI |
| news, scores, theses, crowd, spreads, Reg SHO | 2026-05/06+ | yes | **no**: forward only |
| macro / factors (FRED, French, AQR) | decades | snapshot | yes, research only |
| intraday bars | 8 instruments, 2026-04+ | yes | no |

**Unused assets:**

- **13F / N-PORT / DEF 14A** (~20M rows): UI only. Worth becoming forward signals (institutional breadth, crowding) once 24+ months accrue.
- **eToro top-investor positions, what-if costs, Reg SHO:** recorded, read by nothing.

## 4. The programme, in order

Each item is a research ticket. A strategy reaches demo only after its backtest passes the bars in `.claude/skills/quant/research-process.md`. Every study is registered before its outcomes are read, and the programme keeps one global trial count.

0. **Cheap baselines first.** Before rebuilding factors stock by stock, measure what the simplest implementable alternatives would have earned net: the market core alone, a static diversified ETF mix, and a random-basket control. Every later result is reported against these.

1. **Research panel.**
   - Monthly, point-in-time: characteristics from the PIT XBRL bundle, extended with operating income, capex, debt and cash, × survivorship-free total returns (`adj_close`).
   - Segmented as in §2.
   - Factor constructions validated against published factor returns: Ken French, AQR, global-q, JKP, Open Source Asset Pricing. This is the #2901 failure mode, fixed by construction.
2. **Factor backtests on the panel.**
   - Families: GP/A profitability (Novy-Marx 2013), value (B/M, E/P, CFO/P), 12-1 momentum with crash scaling (Daniel & Moskowitz 2016), asset growth (Cooper, Gulen & Schill 2008), low volatility, quality (QMJ, a quality long-short). Also to assess: accruals, net issuance, industry/residual momentum.
   - Within-industry ranks.
   - Outputs: rank IC and IC-IR per family and segment; quintile spreads; top-N long-only books at our costs vs SPY total return; FF5+momentum regression; turnover.
   - **This replaces v1.5's untested weights.**
3. **Cross-asset trend backtest.**
   - Monthly 12-month TSMOM across the corpus ETF set (US/intl equity, sectors, Treasuries by duration, credit, TIPS, gold/silver, commodities, REITs).
   - Long-or-cash (no leverage). Volatility-scaled sizing within the pot.
   - Uses `adj_close` total return to 2024-09, extended to the present via #3619.
   - ⚠ Eligibility: strategy capital is limited to US-listed eToro Stocks (settled decisions, universe contract), and US ETFs may be CFD-only for UK clients. The backtest can run now; deployment needs an explicit eligibility route.
4. **Risk overlays as backtested rules.** Portfolio volatility targeting, the momentum-crash state, a drawdown de-gross ladder, credit/VIX stress, each measured as an overlay on (2) and (3).
5. **Avoidance filters.** MAX/lottery, extreme attention, very high short interest / borrow, as exclusions measured on (2).
6. **Data to start collecting now** (it cannot be backfilled):
   - borrow fees (IBKR `shortstock` file, daily; an indicative proxy, not eToro's CFD fee);
   - VIX term structure (VIX3M, VIX9D);
   - the Fed Excess Bond Premium release archive;
   - ALFRED vintages for macro.

   **Data to load:** factor libraries (JKP, global-q, AQR, OSAP portfolio returns) and a total-return source for after 2024-09.
7. **LLM research component, redesigned.**
   - The LLM extracts structured, evidence-quoted features: 10-K/10-Q changes (Cohen, Malloy & Nguyen 2020), guidance changes, red flags, earnings direction, and a sector-neutral outlook score (Lehner & Lopez-Lira 2026).
   - Each is evaluated like any factor, **forward-only after the model's training cutoff**. Earlier backtests are contaminated by memorisation (Lopez-Lira, Tang & Zhu 2025).
   - **Absolute LLM price targets leave the score:** sell-side targets show the level is biased; only within-industry ranks inform (Bradshaw, Brown & Huang 2013; Da & Schaumburg 2011).
8. **Portfolio assembly.** Combine the sleeves that pass (core + factor tilt + trend + overlays) under a risk budget. Demo forward test with pre-registered kill rules. Then a capped live canary (operator-funded).

## 5. Explicitly not pursued

Unless at least one of mechanism, data or segment is new:

- intraday/day trading;
- short-term reversal;
- ORB/gap strategies;
- index-event, January, PEAD and dividend-capture trades;
- CFD shorts of overpriced names (the premium concentrates in high-fee names, Drechsler & Drechsler 2014; untested at eToro's hard-to-borrow CFD cost and tail risk);
- end-to-end LLM trading (FINSABER, StockBench);
- daily-TA setups (our 09-29 setup library ≈ random entry);
- tuning stops or targets.

## Sources

The load-bearing citations are listed below; each skill under `.claude/skills/quant/` cites its sources inline.

- SPIVA US YE2024;
- Barber & Odean 2000 (JF);
- Barber, Lee, Liu & Odean 2014 (JFM);
- Chague et al. SSRN 3423101;
- McLean & Pontiff 2016 (JF);
- Chen & Velikov 2023 (JFQA);
- Hou, Xue & Zhang 2020 (RFS);
- Novy-Marx & Velikov 2016 (RFS);
- Hong, Lim & Stein 2000 (JF);
- Israel & Moskowitz 2013 (JFE);
- Daniel & Moskowitz 2016 (JFE);
- Moskowitz, Ooi & Pedersen 2012 (JFE);
- Hurst, Ooi & Pedersen (AQR, "A Century of Evidence on Trend-Following");
- Martineau 2022 (CFR);
- Greenwood & Sammon 2025 (JF);
- Drechsler & Drechsler NBER w20282;
- Cohen, Malloy & Nguyen 2020 (JF);
- Lopez-Lira, Tang & Zhu 2025 (arXiv 2504.14765);
- Bradshaw, Brown & Huang 2013 (RAST);
- Da & Schaumburg 2011 (JFM);
- Jensen, Kelly & Pedersen 2023 (JF);
- Novy-Marx 2013 (JFE);
- Asness, Frazzini & Pedersen 2019 (RAS).
