# #3612: a drawdown de-gross rule — measured, and not recommended

Source: #3612 risk lens, and `.claude/skills/quant/portfolio-construction-and-risk.md` "Risk limits that act": *"A drawdown de-gross ladder, e.g. cut gross exposure by a fixed fraction at −10%, −15% and −20% of the pot … This changes the 2026-08-22 settled decision, so it is the operator's call."* The supervisor queue (#2437, 2026-10-11) asked for it behind a config default of off, with the settled-decision question raised. This note measures it first.

## The question for the operator

Is the mandate's 25% maximum drawdown a **bound on losses**, which needs a rule that sells as the pot falls, or a **stop on new entries**, as built today? A bound would reverse *"The allocation boundary is the ONLY safety net"* (`docs/settled-decisions.md`, 2026-08-22).

**Recommendation: an entry stop. Keep the settled decision.** Safe default: no change. If the operator wants a bound instead, the measured candidate is Grossman–Zhou, below. Calibrated at 25% with same-close fills, it cost 2.7 to 3.2 points a year since 1993 on a fully deployed pot.

## What the limit does today

`max_portfolio_drawdown_pct` (25 on pool event 5) refuses new entries once the pot's drawdown reaches it (`portfolio_drawdown_limit`, `strategy_paper_executor.py:1323-1327`). The refusal does not itself force a sale. The core sleeve is held at its target (50%, mandate event 35) by `core_rebalance_execution`, which buys back into a fall until the pot's drawdown reaches the limit. From there its buys are refused too, and only a sale is allowed (`strategy_core_executor.py:1199-1207`).

## Source rule

- **Ladder.** The thresholds (−10%, −15%, −20%) are the skill's example. No source fixes the cut sizes, so they are fixed by construction at 1, 0.75, 0.5 and 0.25 of the target exposure. Exposure follows the current drawdown both ways, so it is restored as the NAV recovers.
- **Grossman & Zhou (1993),** "Optimal investment strategies for controlling drawdowns", *Mathematical Finance* 3(3). Under the constraint W ≥ (1 − D)·M, where M is the high-water mark, the optimal policy holds a risky amount proportional to the cushion W − (1 − D)·M. As a fraction of NAV that is m·(1 − (1 − D)·M/W).
  - D = 0.25, the mandate's limit.
  - Two choices here are calibration by construction, not from the paper: m = target / D, so the pot is exactly at its target at its high-water mark; and the cap at the target, since there is no leverage.
  - With continuous prices and trading, a non-negative cash return and no costs, the floor always holds. With daily rebalancing under the same cash and cost assumptions, it fails only when one session's market fall exceeds 1/m = D / target: 31.25% at a target of 0.8, 50% at 0.5. The worst session in the sample is −17.41% (19 October 1987), and RF is never negative in it (0 negative sessions).
  - Costs lower that threshold: a fee can breach the floor on its own, or shrink the cushion so that a smaller fall breaches it. The measurement checks NAV after each fee as well as at each close.
- **Data.** Kenneth French's daily three factors (`french_three_factor_daily`, snapshot 60, 0 missing values), parsed from percent to decimal by `reference_data._parse_french_zip` (`app/services/reference_data.py:347-412`). Market = Mkt-RF + RF, the Fama-French market of value-weighted CRSP firms with dividends; cash = RF, the one-month T-bill. 25,147 daily returns after the 1926-07-01 anchor, to 2021-12-31. The sample stops before the `primary-2022-plus` holdout.
  - Coverage: the snapshot's stored rows equal its `row_count`, and every date carries every series. No historical calendar is invented, so the witness is the longest gap between dates: 12 calendar days, 1933-03-03 to 1933-03-15, the 1933 bank holiday (7 days, 2001-09-10 to 09-17, since 1993).
- **Benchmark.** The objective is SPY. Over 1993-01-29 to 2021-12-31, SPY's dividend-adjusted close (research series 7694) returns 10.62%/yr and the French market 10.84%/yr.

## Method

`scripts/measure_3612_degross_ladder.py`. It is a one-factor proxy of the pot, not an engine replay. The pot holds a risky fraction of its NAV in the market and the rest in cash. Two targets:

- 0.5: today's core mandate, core sleeve only;
- 0.8: core plus the 30% active-risk budget at a beta of 1, the pot fully deployed. The active sleeves are modelled as market exposure; their stops and own crash betas are not.

Four rules:

- **today**: the baseline. Rebalance to the target while the pot's drawdown is under 25%; from the limit, never buy, so exposure is the lower of the drifted weight and the target;
- **static**: the target every day, rebalancing through any fall (reference);
- **ladder** and **GZ**, as above. Both keep today's refusal: from the limit they never buy.

Chronology: the static, ladder and GZ formulas were written into the script before the first run. The **today** baseline was added after it, once the core's refusal was read. Applying the refusal to the ladder and GZ was added after checkpoint 1.

Conventions:

- Each rule observes the close of day t − 1 and trades at that close, a same-close fill. A next-session fill is not measured.
- Rebalancing from the drifted weight costs c × |target − drifted| of NAV, with c ∈ {0, 5, 15} bp. These are assumed scenarios, not a calibrated eToro cost. The opening position is endowed at no cost.
- Drawdown is from the running high-water mark of the simulated NAV, with no capital flows. The engine measures its own pot NAV from its epoch (`observe_pot_nav`), not this series.
- Each sample starts at an anchor close and is annualised over calendar days from it, because the NYSE traded Saturdays until 1952.

## Results

At 5 bp. The excess column is the range over the three cost levels, against **today** at the same target. "Days beyond" counts closes at which NAV was below the floor, after the day's return or after a fee charged at that close. A crisis drawdown is the worst drawdown from the running high-water mark on any day inside the window; the peak may precede the window.

| sample | target | rule | return %/yr | excess bp/yr | max DD % | days beyond −25% | 2007–09 DD % | 1929–32 DD % | mean exposure | turnover /yr |
|---|---|---|---|---|---|---|---|---|---|---|
| 1926–2021 | 0.5 | today | 6.90 | | −50.5 | 1,600 | −30.1 | −50.5 | 0.49 | 0.39 |
| | | static | 7.00 | 9 to 10 | −57.2 | 1,370 | −30.0 | −57.2 | 0.50 | 0.45 |
| | | ladder | 6.31 | −60 to −51 | −29.2 | 436 | −22.7 | −29.2 | 0.43 | 0.84 |
| | | GZ | 6.22 | −73 to −66 | −24.3 | 0 | −20.5 | −24.3 | 0.39 | 0.59 |
| | 0.8 | today | 8.89 | | −70.7 | 4,286 | −45.4 | −70.7 | 0.79 | 0.21 |
| | | static | 9.00 | 11 to 12 | −76.1 | 3,916 | −45.7 | −76.1 | 0.80 | 0.29 |
| | | ladder | 8.24 | −122 to −64 | −34.7 | 1,533 | −26.6 | −34.7 | 0.63 | 1.17 |
| | | GZ | 7.06 | −209 to −170 | −24.9 | 0 | −24.0 | −24.9 | 0.52 | 1.59 |
| 1993–2021 | 0.5 | today | 6.87 | | −30.1 | 23 | −30.1 | | 0.50 | 0.48 |
| | | static | 6.88 | 2 | −30.0 | 22 | −30.0 | | 0.50 | 0.49 |
| | | ladder | 5.94 | −96 to −92 | −22.7 | 0 | −22.7 | | 0.46 | 1.06 |
| | | GZ | 5.77 | −114 to −108 | −20.5 | 0 | −20.5 | | 0.43 | 0.73 |
| | 0.8 | today | 9.27 | | −45.4 | 545 | −45.4 | | 0.80 | 0.27 |
| | | static | 9.35 | 7 to 8 | −45.7 | 523 | −45.7 | | 0.80 | 0.31 |
| | | ladder | 7.76 | −268 to −150 | −26.6 | 15 | −26.6 | | 0.67 | 1.52 |
| | | GZ | 6.39 | −316 to −273 | −24.0 | 0 | −24.0 | | 0.57 | 2.02 |

**A lower target instead.** The script also searches today's rule over targets 0.01 to 0.80 in steps of 0.01, at 5 bp, for the highest return among targets with no close beyond the floor (compared unrounded):

| sample | best target | return %/yr | max DD % | GZ at 0.8 |
|---|---|---|---|---|
| 1926–2021 | 0.19 | 4.68 | −24.5 | 7.06%/yr, −24.9% |
| 1993–2021 | 0.41 | 6.09 | −24.8 | 6.39%/yr, −24.0% |

Per-crisis drawdowns for 1937–38, 1973–74, 1987, 2000–02 and 2020 are in the script's JSON output.

**Cost sensitivity of the ladder.** Across the three cost levels the ladder's maximum drawdown moves by under 0.4 points, so "it does not hold the limit" does not depend on cost. Its return does: at 0.8 since 1993 the excess ranges over 118 bp/yr, not monotone in cost, and that was not traced. No ladder return figure carries the recommendation.

## Reading

1. **Today's rules do not hold 25%, and are not meant to.** On this proxy, a fully deployed pot fell 45.4% in 2007–09 and 70.7% in 1929–32. With the core sleeve alone it fell 30.1% in 2007–09. The 25% figure works as an entry stop, which is what the code implements.
2. **The skill's example ladder does not hold the limit.** Fully deployed, it still reached −26.6% in 2007–09 and −34.7% in 1929–32.
3. **Grossman–Zhou held it at every close and after every fee in the 1926–2021 sample.** That is a sample result, not a guarantee: see the failure conditions under Source rule. It costs 170 to 316 bp/yr fully deployed, against a mean exposure of 0.52 to 0.57 instead of 0.80. These are historical return differences, not a forecast price of the bound.
4. **Among the configurations measured, Grossman–Zhou bought the bound most cheaply.** It returned more than the best static target that held 25%: by 0.30 points a year since 1993 (6.39% against 6.09%), and by 2.38 points over 1926–2021 (7.06% against 4.68%). Other Grossman–Zhou calibrations and volatility targeting were not compared.
5. **Against the objective.** Since 1993 SPY returned 10.62%/yr. A fully deployed pot on today's rules trailed it by 1.35 points a year before any selection skill; under Grossman–Zhou it trailed by 4.23 points. Those are pot-level gaps. They are not the alpha an active sleeve would need, which also depends on its weight and on how the rule changes its exposure. No strategy has yet passed the research-first backtest bar: #3609 step 2 failed its pre-registered evaluation (2026-10-09), and nothing trades until a screen passes (#2437 supervisor queue, 2026-10-09).

## Recommendation, and what would change it

Keep the 25% as an entry stop, which is what the code implements. On the measured history a bound cost roughly three points a year at full deployment, and the objective is to beat SPY. The 2026-08-22 decision names the allocation boundary as the only safety net the operator wants, which argues against adding a forced-sale control. Build nothing behind a flag: a disabled control is code with no caller.

If the operator chooses a bound, the measured candidate is Grossman–Zhou with today's buy refusal. Its daily form holds a floor only up to a one-session fall of D / target before costs, and less after them, so the internal floor would sit inside 25% by a margin sized for fees and for the largest one-session fall it must survive. Before building it, measure that tighter calibration, a next-session fill and the eToro cost; the cost quoted here is for D = 0.25 with same-close fills only.

The recommendation would change if either of these appeared:
- the operator states that a hard drawdown bound outranks beating SPY;
- a validated sleeve shows crash betas well above 1, so the stress figures understate its tail.

## Not measured

- Volatility targeting at book level (skill, "Risk limits that act"), the other exposure-scaling alternative.
- Sleeve-specific crash betas; small caps' exceed their calm betas (skill, "Stress library").
- The core's 5% rebalance band and the active sleeves' stops; the proxy rebalances daily.
- Intraday prices and a next-session fill. A rule acting on closes cannot avoid an overnight gap.
- A cash yield below the T-bill.

## Security

No change to any authorisation, order or capital path. A read-only research script and this note.
