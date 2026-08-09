# The gap-down fade — a real effect that died in 2020, and what that teaches

Refs #2437. The most instructive result of the research pass: a strong,
significant, cost-clearing intraday effect that **is not present in the market we
would actually trade in.**

---

## 1. The finding, and the correction that gutted it

**Setup:** a stock gaps down more than 2% from the prior close → **buy the open,
sell the same day's close.** Same session. Outcome measured open-to-close, so the
window is fillable.

**Full sample, 1962-2026, 25.9M bars, year-clustered:**

```text
band          obs      mean bps     t    yrs   round trip   NET
<$5        536,665      160.85   5.81    54       145.0     15.9
$5-20      556,235       59.67   3.60    53        57.1      2.6
$20-100    383,942       83.73   6.11    44        50.9     32.8
>=$100     232,885      156.07   5.73    44        32.2    123.9
```

Significant in every band. Best **net** in the most liquid band — the opposite of
every other effect in this project, which all lived where we cannot trade. The
mirror holds too: gap-**up** continuation is **−59.03 bps, t −5.39**.

### ⚠⚠ Then the operator asked whether old data describes today's market

```text
era                     obs   net bps      t   yrs
1 pre-decimal <2001  65,297     -45.9  -1.58   26
2 decimal 01-06      54,935     131.4  10.74    6
3 RegNMS/HFT 07-14  128,478     178.5   5.41    8
4 mature 15-19       96,642     115.9  15.67    5
5 zero-comm 20+     271,167      11.8   1.39    7
```

```text
2015 +118.2   2016 +91.5   2017 +114.4   2018 +117.4   2019 +137.9
2020  -16.3   2021 +25.3   2022  +27.6   2023  +43.9   2024   +6.6
2025  -14.8   2026  +9.9
```

> ⚠⚠ **The effect paid 100-180 bps net every year from 2001 to 2019 and died in
> 2020.** Post-2020: **+11.8 bps at t 1.39** — nothing, statistically or
> economically.

**The break lands exactly on a structural change.** Zero commissions arrived
Oct-Nov 2019; the retail boom followed; participation exploded. The compensation
for absorbing overnight panic is now competed away by app users doing it free.

⚠ And note the first row: **pre-2001 the effect was NEGATIVE.** It existed only
inside the decimalised, pre-zero-commission window — **opened by one structural
change and closed by another.**

### The lesson, which is bigger than the strategy

**A 64-year average produced `t 5.73` on an effect that has not existed for six
years.** The full-sample number was not wrong; it was *about a market that no
longer trades*.

> **RULE: every candidate effect gets an era split BEFORE anyone gets excited.**
> Not a robustness check afterwards — a gate. Cut at real structural events
> (2001 decimalisation, 2007 Reg NMS, 2019 zero commission), never arbitrary
> dates.

⚠⚠ **And the binding constraint on all future intraday work is now explicit: the
usable sample is 2020+, which is ~6 years.** That is a much weaker statistical
position, and it is the honest one.

---

## 2. ⚠⚠ What a second Codex pass found that I had missed

Ranked by how likely each is to explain the edge rather than merely dent it.

1. ⚠⚠ **Corporate-action contamination — ranked FIRST and I did not control for
   it.** Ex-dividend days appear as gap-downs of 2-5% that are *dividend
   mechanics, not panic*, and buying the open does not capture a reversal that
   was never there. **Exclude ex-dividend, split, reverse-split, special-dividend
   and symbol-change dates, then recompute.**
2. **Opening-auction execution bias.** **Goyal, Jegadeesh & Wu** find opening
   auctions are **materially less liquid than closing auctions**. ⚠ Our cost
   assumption is therefore too low *exactly where the signal fires*.
3. **Bad-open selection bias** — distinct from survivorship. Stocks that gap
   down then **halt, open late, or get suspended** are underrepresented. Test by
   treating a missing same-day close as an adverse fill (−5%, −10%, −20%).
4. **Delisting-return omission.** **Shumway (JF 1997)** documents large negative
   missing delisting returns, especially performance-related. *"Probably lethal
   to `<$5`; less likely to kill `>=$100`."*
5. **Lookahead in the price band** — must bucket on the **prior close, known
   before the open**, not on same-day or adjusted price.
6. ⚠⚠ **Day-clustering, which year-clustering does not fix.** *"A strategy with
   800 names firing on one day is not 800 independent bets."* Gap-downs cluster
   on crash days. **Two-way (day and firm) clustering is required.**
7. **Market beta masquerading as a stock effect** — the fade may be SPY
   open-to-close reversal times beta. Test the residual:
   `stock_oc − beta_60d × SPY_oc`.
8. **Bid-ask bounce / stale prior close** — **Park (JFQA 2009)** shows apparent
   reversals after large moves can vanish when events are selected on midpoints
   rather than transaction prices.
9. **Non-common securities** — ADRs, preferreds, units, warrants, SPACs,
   closed-end funds gap for mechanical reasons. Keep US common stock only.
10. **Vendor stitching / ticker reuse** — audit the top 200 P&L trades by hand.

**Literature:** **Berkman et al. (JFQA 2012)** — positive overnight returns tend
to reverse during the trading day, attributed to **attention-driven buying at the
open**. That is the mechanism for the gap-up side.

---

## 3. Ten more setups, all computable from daily OHLCV

Each has a stated mechanism. ⚠ All must now be tested on **2020+** and all are
declared trials.

| # | setup | rule sketch | mechanism |
| --- | --- | --- | --- |
| 1 | prior-day crash reversal | yesterday C-to-C ≤ −8%, volume ≥ 1.5× 20d median, today's open not another −3% below | liquidity provision after forced selling (Nagel) |
| 2 | three-day exhaustion | 3-day return ≤ −12%, each day above median volume, yesterday close-location < 0.3 | capitulation |
| 3 | failed breakdown | yesterday's low breaks the 20-day low but closes back above it, close-location > 0.6 | stop-loss liquidity absorbed; sellers fail |
| 4 | panic gap + market confirm | stock gaps −2% to −8% **and** SPY gaps ≤ −0.75% | systematic overnight de-risking partly reverses |
| 5 | idiosyncratic-news exclusion | skip if the stock's gap is > 2 ATR worse than its sector | ⚠ a loss filter, not alpha |
| 6 | NR7 breakout | narrowest range of last 7 days, then opens above yesterday's high | volatility compression → imbalance |
| 7 | volume-climax rebound | return ≤ −5%, volume ≥ 3× 60d median, range ≥ 2× ATR20, close-location < 0.25 | capitulation, event disasters excluded |
| 8 | 52-week-high pullback | within 5% of the 252-day high, 3-10 day pullback of −3% to −10% | **George & Hwang (JF 2004)** — 52-week-high proximity explains momentum |
| 9 | turn-of-month | buy last trading day of month, sell 1-2 days into the next | fund flows / scheduled rebalancing; ~**0.15%/day** vs ~flat otherwise |
| 10 | large down day, strong close | yesterday `C/O ≤ −4%` but close-location > 0.75 | intraday absorption |

---

## 4. Confirming metrics, ranked by expected marginal value

1. **Gap size normalised by ATR** — best bucket **−2% to −8% and 0.75-2.5 ATR**.
   ⚠ Avoid beyond −12% or 3 ATR: *"moderate gaps are liquidity shocks; extreme
   gaps are information shocks."*
2. **Market and sector gap alignment** — prefer the stock gapping down *with* the
   market. ⚠ Avoid stock −6% while the sector is flat; that is firm-specific bad
   news.
3. **Corporate event flag** — exclude ex-dividend and splits outright; test
   earnings separately (more liquidity-provider reversal, but more adverse
   selection).
4. **Prior trend** — prefer above a rising 50-day. Avoid 20-day return < −20%
   and below the 200-day: *"gaps in existing downtrends are repricing, not panic."*
5. **Liquidity** — price > \$20, 20-day median dollar volume > \$10m. ⚠ *"Your
   `>=$100` result screams execution quality matters."*
6. **Volatility regime** — high but not crisis-extreme (SPY ATR20/close in the
   60th-90th percentile), consistent with Nagel.
7. **Distance to support** — only if mechanical: open within 0.5 ATR above the
   prior 20/60-day low. ⚠ *"Support as hand-drawn charting is useless in
   research."*
8. **Time since last gap** — prefer no same-direction gap in 5 days; repeats
   signal unfolding information.
9. **RSI** — ⚠ mostly redundant with prior trend and recent returns.
10. **Day of week** — diagnostic only.
11. ⚠⚠ **Volume on the gap day — LOOKAHEAD.** Same-day total volume is not
    available at the open. Use prior volume only.

---

## 5. Universe

**Include:** price ≥ \$20 (test \$100+ separately) · 20-day median dollar volume
≥ \$10m (ideally ≥ \$25m) · **US common stock only** · listing age ≥ 252 trading
days · no corporate action · no missing OHLC in the prior 20 days · beta 0.7-2.0.

**Exclude:** `<$5` entirely for live trading **despite the positive gross
result** · IPOs under a year · biotech binary-event names · distressed (below
200-day and 60-day return < −40%) · SPACs, warrants, preferreds, ADRs,
closed-end funds, leveraged ETFs.

---

## 6. ⚠ The protocol, amended by our own era finding

Codex proposed research/validation/holdout as **1962-1999 / 2000-2014 /
2015-2026**.

⚠⚠ **That split is now wrong and our era result is why.** A 2015-2026 hold-out
**straddles the 2020 regime break**, mixing the strongest years (2015-19 at
+115 bps) with the dead ones (2020+ at +11.8). It would report a healthy average
for a strategy that stopped working.

**Amended:**

- **Research / rule-forming: 2001-2014.** The regime where the effect existed.
- **Validation: 2015-2019.** Same regime, untouched while forming rules.
- ⚠⚠ **Hold-out: 2020-2026 — and this is the only sample that matters**, because
  it is the only one describing the market we would trade in. **A rule that
  works in 2001-2019 and not in 2020+ is a historical observation, not a
  strategy.**
- Pre-2001 is **excluded from rule-forming entirely** — the effect has the wrong
  sign there, so it is a different regime, not more data.

Every rule and every confirming condition is declared to `trial_register.py`
before measurement. ⚠ Testing 10 setups × 10 conditions is **100 trials**, which
raises the deflated-Sharpe bar to roughly the 0.174 level. Budget accordingly, or
test fewer things properly.
