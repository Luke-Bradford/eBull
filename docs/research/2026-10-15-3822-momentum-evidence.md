# #3822 — why the engine's first demo strategy is a momentum book (2026-10-15)

Operator direction (2026-10-15, #2437): prove a strategy in the demo account with data; pivot off dead ends. This note
records the quick tests that chose the strategy. Every figure is printed by a script in `scripts/research_3822/`, run
from the repo root with `PYTHONPATH=. uv run python <script>` against the dev DB.

## Dead ends (measured the same day)

| idea | script | result |
|---|---|---|
| Trend filter on the US market (Faber 10-month SMA), with and without leverage | `index_vol_trend.py` (French daily, snapshot 60) and `spy_trend_2026.py` (SPY 1993-2026) | Halves drawdowns, but trails SPY since 2022: 4.41%/yr against 12.38% (2022-01..2026-10-14) |
| Volatility targeting on the market | `index_vol_trend.py` | Roughly the market's return at lower volatility; leverage gains vanish at cash + 6% financing |
| Turn-of-month effect | `turn_of_month.py` | t = 8.8 for 1927-1986, t = 1.3 since 2006; no overlay gain since 2006 |
| Merger arbitrage (tender offers and merger votes, all SEC filers) | `merger_arb_extract.py`, `merger_arb_sim.py` | Tender offers about zero net (median 0.00%); the merger-vote portfolio returned 6.89%/yr against SPY's 13.50%, beta 0.47 |
| Real strategy funds against SPY | `strategy_etfs_vs_spy.py` | Merger arb (MNA) 2.24%/yr for 2016-2024 against SPY's 14.64%; managed futures (DBMF, KMLM) trailed by 8.7 to 9.2 points a year for 2021-2024; value, low-vol and small caps lost to SPY in every window |

## The one survivor: momentum

**Real funds.** Momentum ETFs beat SPY in every window measured:
- SPMO: +2.05 points a year for 2016-2024, and +2.72 for 2021-2024.
- XMMO: +0.72 for 2006-2024, and +3.45 for 2016-2024.

**Our book.** `momentum_backtest.py` runs on the survivorship-free Intrader archive, 2014-10..2024-09:
- **Universe:** the top 500 operating companies (#3361 linkage) by trailing dollar volume, close at least $5.
- **Signal and book:** risk-adjusted 12-1 momentum (11 monthly returns compounded, divided by their standard deviation), top 30 equal-weighted, monthly.

| | CAGR | max DD |
|---|---|---|
| Book, 0.1% per side | **16.97%** | 22.0% |
| Book, 0.2% per side | 15.87% | 22.7% |
| SPY | 13.19% | |
| Random 30 from the same universe (20 seeds) | median 7.40%, range 1.95%..10.64% | |

- **Control:** the book beats all 20 random baskets.
- **Plain momentum fails:** without risk adjustment, top 30 returns 11.58% at 0.2% per side.
- **Regime dependence:** every size lags SPY for 2014-2019 (top 30: 8.73% against 11.70%) and leads for 2019-2024 (26.64% against 14.82%). Top 40 and top 50 roughly match SPY over the decade.
- **Holding band:** a 1.5N or 2N band lowers the return to 14.2%..15.0%, so the book has no band.
- **Costs:** measured eToro spreads are about 0.03% (AAPL) and 0.17% (median of the quoted top-500 names), so 0.1-0.2% per side is conservative.

**Expected, honestly:** a modest and uncertain premium over SPY with lower drawdowns, and long stretches of lagging. The demo book is scored live against SPY and against a random-30 control. A sustained lag stops it.

## Live rule

`app/services/momentum_book.py`; the live data is eToro `price_daily` (split-adjusted, no dividends, so price momentum).
- **Universe:** tradable canonical US stocks.
- **Formation:** monthly, from the completed month t, traded on the first session after month t.
- **Book:** top 30. Names that leave the book are sold.
