# Total-return accounting result (#2429)

Status: implemented and full-population validated on 2026-08-11.

## Decision

Backtest execution and backtest wealth are different price domains.

| Consumer | Price basis | Reason |
|---|---|---|
| Signal indicators and candle patterns | split-adjusted raw OHLC | A tradable candle must retain its real high/low/open/close geometry. |
| Entry fills, exits, TP, SL and gap handling | split-adjusted raw OHLC | These are executable market levels. |
| Spread-band selection | raw entry fill | The cost model is price-band keyed; an adjusted historical scale can select the wrong band. |
| Strategy equity marks and realised returns | `adj_close` wealth scale | Includes split and dividend distributions without changing the trade decision. |
| Buy-and-hold entry, exit and marks | `adj_close` wealth scale | A shareholder receives distributions; a price-only comparator understates the hurdle. |

For a raw executable price `p(d)` on date `d`, its value on the wealth scale is:

`p_wealth(d) = p(d) × adj_close(d) / close(d)`

The raw fill is spread-costed first, then mapped with the contemporaneous
factor. Percentage spread is scale invariant. Missing, non-finite or
non-positive factors are excluded and counted as `total_return_price_missing`;
they never become zero-return observations.

This does **not** move S-1/S-2/S-3/S-4 indicators to adjusted close. Such a
change would alter the strategy rather than repair its accounting and must be a
separate registered trial with a new strategy identity.

## Evidence before implementation

The source corpus documentation describes adjusted prices with split/dividend
handling. CRSP's methodology independently distinguishes raw prices,
distribution adjustments and total-return calculations. Blume and Stambaugh
also show why a buy-and-hold portfolio must not be silently implemented as a
frequently rebalanced return average.

Primary references:

- Papers With Backtest, `Stocks-Daily-Price` dataset documentation:
  <https://paperswithbacktest.com/docs/datasets/stocks-daily-price/>
- CRSP, *US Stock & Indexes Calculations and Index Methodologies*:
  <https://www.crsp.org/wp-content/uploads/guides/CRSP_Calculations_and_Index_Methodologies.pdf>
- Blume and Stambaugh (1983), *Biases in computed returns: An application to
  the size effect*, Journal of Financial Economics 12, 387–404:
  <https://ideas.repec.org/a/eee/jfinec/v12y1983i3p387-404.html>

Corpus census, validated universe only:

| Window | Series | Series changed by dividends | Result |
|---|---:|---:|---|
| 1962–2026 | 5,266 | 2,351 | No series had a lower total return than price return. |
| 2022–2026 | 5,264 | 2,094 | Mean wealth uplift 7.5989%; median uplift among changed series 12.4232%. |

In the recent window, 184 instruments changed from non-positive price return to
positive total return and none changed in the favourable-to-unfavourable
direction. All 23,339,583 bars in the complete evaluation corpus had a present
`adj_close` at the time of measurement. This is a material, directionally
dangerous bias rather than a missing-data limitation.

## Identity and storage

- Historical rows are explicitly labelled `raw-close-price-return-v1` and keep
  their original `strategy-result-v1` hash. Their metrics are not rewritten.
- Corrected rows are labelled `split-dividend-adjusted-wealth-v1` and use a
  distinct `strategy-result-v2` hash that includes `return_basis`.
- The monitoring API selects the corrected basis explicitly, so old evidence
  cannot satisfy a current evidence window.
- Storage impact is one short text column per aggregate result row. No new bar,
  signal, position or payload table is created.

## Reproduction

The production-shaped comparison is:

```bash
PYTHONPATH=. uv run python scripts/verify_2429_total_return.py --window primary-2022-plus
```

It runs the corrected writer over every validated-universe instrument, pairs
each corrected result with the exact immutable raw-price result sharing every
other identity member, prints both values and their delta, and rolls back the
corrected rows. A missing or duplicate control arm is a hard failure.

## Production-shaped A/B result

The pinned primary recent window (`2022-01-01` through `2026-07-08`) completed
over 5,266 series and 5,298,638 bars with zero missing `adj_close` observations.
All 16 corrected strategy × ambiguity × quarantine rows paired one-to-one with
their exact immutable raw-price controls. The run took 2,577.59 seconds and
rolled every corrected row back.

Representative conservative (`worst_case`, `masked`) results:

| Strategy | Raw strategy return | Total-return strategy | Total-return buy/hold | Relative return | Total Sharpe | Verdict |
|---|---:|---:|---:|---:|---:|---|
| S-1 time-series momentum | -84.02% | -82.94% | +32.93% | -115.87 pp | -2.07 | fails |
| S-2 cross-sectional momentum | +41.47% | +47.83% | +73.13% | -25.31 pp | +0.48 | fails hurdle |
| S-3 mean reversion in trend | -49.73% | -47.43% | +57.25% | -104.68 pp | -0.44 | fails |
| S-4 volatility compression breakout | -55.72% | -51.93% | +28.05% | -79.99 pp | -0.73 | fails |

The direction is consistent across the arms: distribution accounting improves
each strategy's absolute result, but raises its matched buy-and-hold hurdle by
8.30–11.31 percentage points. Relative performance therefore worsens. This is
exactly why the defect was dangerous: price-only accounting made the controls
look closer to viability than they are.

## Interpretation guard

Correct accounting can improve or worsen an individual strategy relative to
buy-and-hold. It does not turn the four harness controls into capital
candidates, prove alpha, or justify paper allocation. Promotion remains closed
until recent hold-out, bootstrap, multiple-testing, synthetic-control, carry
and execution gates all pass.

Measured conclusion: **none of S-1 through S-4 is a capital candidate**. S-2 is
the only positive recent control, but positive absolute return is insufficient:
it trails passive exposure materially and its Sharpe is weak. The next research
stage must test pre-registered conditional opportunities/regimes rather than
tuning these broad controls until one looks favourable.
