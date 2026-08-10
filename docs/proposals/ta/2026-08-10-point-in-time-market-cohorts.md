# Point-in-time market cohorts — implemented foundation (#2508)

## Outcome

Strategy evidence can now retain the security type, primary listing market,
as-traded price band and causal liquidity/risk context that existed when a
candidate was considered. The implementation fails closed when that context is
not known; it does not fill a historical trade with today's instrument row.

This is attribution infrastructure, not a profitable strategy and not a
promotion of S1–S4. Its purpose is to stop an average from hiding that a rule
worked in one market-structure cell and lost in another.

## What is recorded

`instrument_market_classification_history` is transition-only. The eToro
universe refresh confirms the current row, opens a new row only if provider
exchange or security type changes, and closes it when the instrument leaves the
tradable universe. The initial import starts on the observation date and is
never backdated.

`strategy_decision_contexts` stores one row for a fired or explicitly refused
candidate. Routine `not_fired` scans and rolling indicator values do not enter
this table. The row contains:

- provider security type and normalised common-stock/ETF/other classification;
- primary listing market (NYSE/Nasdaq/other), explicitly not execution venue;
- contemporaneous as-traded price and fixed price band;
- 20 completed causal sessions of mean share/dollar volume (capacity), median
  share/dollar volume (typical-day robustness), relative volume, zero-volume
  frequency, intraday coverage and dollar-volume band;
- spread, realised volatility, gap, market/sector residual z-score and VIX;
- the version hash of all bucket boundaries and semantics;
- a named refusal when any required input is absent or unknown.

The bands are descriptive, frozen diagnostics. Discovering that one band wins
does not allow that band to be selected on the same outcomes. It creates a new
preregistered candidate for a later untouched interval.

## Measured current classification census

Measured against the dev database after migrations 302–303 on 2026-08-10:

| security type | primary listing | current rows |
|---|---:|---:|
| common stock | Nasdaq | 3,618 |
| common stock | NYSE | 2,465 |
| common stock | other | 4,734 |
| ETF | Nasdaq | 110 |
| ETF | NYSE | 390 |
| ETF | other | 732 |
| other | other | 646 |

The 12,695-row classification history occupied 5,600 KiB including indexes.
The empty decision-context relation occupied 48 KiB. This validates the
transition-only storage shape; it does not yet measure context growth under a
live candidate rate.

## Important boundaries

- The imported classification is prospective from 2026-08-10. Historical
  backtests before that date refuse until an authoritative dated source exists.
- Primary listing affects listing rules, auctions and halt identity. It is not
  where an eToro order executed. Spread/slippage and broker execution evidence
  remain separate requirements.
- Exchange IDs 4/5 and instrument type IDs 5/6 are stable provider facts used
  during bootstrap; lookup labels cross-check them but are not required to have
  arrived first.
- Market-cap cohorts remain refused until both as-traded price and a
  point-in-time share count are valid.
- No context rows exist yet because no strategy candidate is allocation-ready.
  Generating placeholder rows would manufacture evidence and bloat the table.

## Next evidence stage

Join completed, identically resolved outcomes to their immutable contexts and
emit unpooled plus type/listing/price/liquidity and declared interaction cells.
Each cell must report trade count, date/name clustered effective sample size,
after-cost expectancy with confidence interval, profit factor, drawdown/tail,
hit rate, holding time, turnover, concentration and cost sensitivity. Sparse or
directionally unstable cells refuse promotion. At least two recent purged
walk-forward folds and a later untouched/prospective interval must agree before
the candidate is exposed as an allocation control.

## Shared cohort verifier

Implemented in `app/services/strategy_cohort_report.py`. It consumes immutable,
already-costed candidate observations and always emits the unpooled population
plus every non-empty, predeclared cell for:

- mechanism, security type, primary listing, price and dollar-volume axes;
- mechanism × price, mechanism × dollar volume;
- listing × price and listing × dollar volume.

The dimensions are a module constant rather than a caller argument. This is the
guard against asking the same outcomes for arbitrary combinations until one
looks attractive. Both positive and losing cells remain in the report.

Each cell reports nominal trades and entry dates, date-clustered effective
sample size and 95% expectancy interval, profit factor, hit rate, median and
mean return, double-cost expectancy, holding time, turnover, worst trade,
five-percent expected shortfall, worst MAE, event-time portfolio max drawdown,
exposure and largest date/name/sector shares. Drawdown is a required,
version-stamped input from the portfolio simulator: it is not reconstructed
from closed trades, because doing so would hide overlapping positions and
intratrade marks. Its absence is a named fail-closed refusal.
A cell refuses when it has fewer than 30 trades, effective sample below 30, an
uncomputable bootstrap, a non-positive lower expectancy bound or non-positive
expectancy at double execution cost. The 30-independent-trade floor is reused
from the prospective activation contract; it is not a newly tuned threshold.

`assess_recent_stability` is the next-level gate. A named, preregistered cohort
must remain economically positive in at least two non-overlapping walk-forward
folds and one later untouched or prospective interval. Reports with different
strategy, context, outcome or cost-model versions cannot be assembled into one
claim.

This completes the pure shared reporter. Loading real candidates remains
blocked honestly where point-in-time classification, causal execution inputs or
completed costed outcomes do not exist; the reporter does not backfill them
from current metadata.
