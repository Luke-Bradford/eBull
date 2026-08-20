# ETF first-to-last-half-hour momentum preregistration

Status: formula and source contract frozen before retained eToro outcomes are
opened for #2502. Parent #2469.

Source-contract correction, still before an outcome was loaded: the harvester
stores provenance as `etoro/<universe_version>/nyse_rth`, not the bare `etoro`
label originally written in the candidate module. The first read-only census
therefore returned zero eligible rows. The contract and candidate hash were
corrected to require that namespaced RTH form before rerunning the census; the
formula, instruments, dates, thresholds and outcome fields were unchanged.

## Published hypothesis and adaptation boundary

Gao, Han, Li and Zhou, *Market intraday momentum*, Journal of Financial
Economics 129 (2018), 394–414, DOI
[`10.1016/j.jfineco.2018.05.009`](https://doi.org/10.1016/j.jfineco.2018.05.009),
define the first return as the prior 16:00 price to the 10:00 New York price.
Its sign selects a long or short position at 15:30, closed at 16:00. Their
primary SPY study uses TAQ trade/quote data through 2013. It is prior evidence,
not a current eBull result.

eBull has completed eToro OHLCV candles but no historical 15:30 bid/ask tape or
reproducible shortability. Two arms therefore remain distinct:

- `published_signed`: long when the opening return is positive, short otherwise;
  replication diagnostic only;
- `long_only`: long when positive and abstain otherwise; the only arm compatible
  with the current execution direction, but it cannot inherit the paper's result.

## Frozen causal formula

For a full NYSE session `t`:

```text
r_open[t] = close(09:30-10:00, t) / close(15:30-16:00, prior session) - 1

published side = long  if r_open[t] > 0
                 short otherwise

long-only fire = r_open[t] > 0
```

The signal is known only after the 09:30 candle completes at 10:00. A
prospective entry uses the observed executable quote at/after 15:30 and must
close against an observed executable quote by 16:00. The stored 15:30 candle
open/close can produce a **gross feasibility proxy only**; it is never described
as a fill or after-cost return.

Early-close sessions, missing opening/closing intervals, gaps, forming bars,
stale quotes and halts refuse the session. SPY is the primary instrument. QQQ
and IWM are predeclared robustness instruments and cannot replace a failed SPY
result. Other ETFs require a new trial identity and multiplicity budget.

## Evidence gate

The effect screen remains `sample_immature` until SPY has 60 complete independent
full sessions. This is a minimum descriptive sample, not promotion. Allocation
still requires the standing later prospective interval and at least six months
and 30 independent fired observations.

Report, without selecting a best ticker or regime:

- session/refusal census and firing cadence;
- gross candle-proxy expectancy as feasibility only;
- prospectively observed bid/ask net expectancy and session-blocked interval;
- hit rate, profit factor, worst interval, expected shortfall and drawdown;
- always-long-last-half-hour and same-session random-sign comparators;
- signal-strength calibration and results by month, volatility and volume;
- doubled-cost, one-bar entry-delay, missed-entry and missed-close stress.

Promotion requires a positive lower confidence bound after observed costs and
all standing data, portfolio, broker and forward gates. There is no TP/SL fitted
from these outcomes: the published thesis exits at the session close. Any stop,
target, second-to-last-half-hour input, volatility filter or magnitude threshold
is a separately preregistered trial on later data.

The retained database footprint stays bounded to existing raw intraday bars,
one fired/refused decision snapshot and one terminal outcome. No rolling return,
regression or indicator series is persisted.
