# R6 cost-by-size-band kill-check declaration (#2907)

Status: **FROZEN BEFORE THE FIRST VALUE QUERY**.

This is a live operational cost diagnostic, not a strategy backtest. It cannot
admit a Tier 2 arm while #2900 refuses every historical ranking family.

## Question

On the full current §4.0 validated universe, what round-trip cost does the
existing frozen cost model charge in each company-size band, and is the
microcap charge already too large for the published return ceiling that
motivated #2907?

## Population and fields

- Population: every ID returned by
  `app.services.strategies.validated_universe.load_validated_universe`, version
  `VALIDATED_UNIVERSE_RULE_VERSION`, on the measurement transaction.
- Size field: `instrument_valuation.market_cap_live`, without imputation.
- Nominal entry price: the latest stored `quotes.last`, only when finite and
  positive. This is a live-snapshot diagnostic with no freshness claim. The
  quote timestamp range and stale tail are reported because an old nominal
  price can select the wrong band, especially after a split. A split-adjusted
  research price is not a substitute.
- Cost: `app.services.cost_model.COST_MODEL_ID` and its `as_traded` band for the
  model's one declared lane: long, x1, real settlement, USD order/account/
  instrument. The output is explicitly a **spread diagnostic**. Carry and FX
  are structural-zero closures only inside that lane; the verifier records the
  lane and closure constants and refuses if either is not the frozen model's
  declared state. No bespoke microcap spread is fitted after seeing the data.
- Every population member appears in exactly one Cartesian census cell:
  `(micro|small|mid|large|unknown_market_cap) × (priced|unpriced)`. The joint
  total, both marginal totals and distinct-ID count must each equal the universe
  size. NULL, non-finite, zero and negative cap/price states are separately
  counted; all unavailable caps map to `unknown_market_cap` and all unavailable
  prices map to `unpriced`. These rows are reported, never silently dropped or
  sampled.

Size bands are fixed from #2907 and are half-open at the upper edge:

| Band | Market cap |
|---|---:|
| micro | `< $300m` |
| small | `$300m–< $2bn` |
| mid | `$2bn–< $10bn` |
| large | `>= $10bn` |

## Cost arithmetic

For a model band with round-trip spread `s` percent, `h = s / 200` and an
unchanged-price round trip retains `(1-h)/(1+h)` of capital. Report, by size
band, the full-population count plus p50/p75/p95/maximum of:

1. the frozen model spread percent;
2. the exact unchanged-price round-trip loss percent;
3. one-round-trip loss in pounds per £1,000 and £10,000;
4. three-round-trip compounded annual loss, representing the most frequent
   quality cadence already recorded by #2899 (one full replacement every four
   months).

Each size band reports `N_total` and `N_priced`; cost percentiles use only its
priced members. Also report the micro-minus-large difference and micro/large
ratio for p75 three-round-trip compounded loss, in percentage points and a
dimensionless ratio respectively, plus quote timestamp range and age at the
measurement timestamp, market-cap coverage, nominal-price coverage, and the
distribution of underlying price-cost bands inside every size band. Percentiles
use discrete nearest-rank observations (`ceil(n*p)-1`), never interpolation.
An empty priced band has null percentiles. A missing priced micro band is
`DATA-FAIL`; a missing priced large band makes only the comparison null.

## Frozen comparison and verdict

#2907 cites 20.3%/year for quality-adjusted microcap value. That is a vendor's
**total return**, not a measured excess return, so it is used only as a generous
upper ceiling for falsification. The two mandatory literature haircuts produce:

- 15% haircut: `20.3% × 0.85 = 17.255%`;
- 58% haircut: `20.3% × 0.42 = 8.526%`.

For haircut return `r` and three-round-trip loss `L3`, net wealth is
`(1 + r) × (1 - L3)`. The verifier computes every threshold from this formula;
it does not compare return and loss additively. The primary statistic is the
microcap p75 **three-round-trip compounded loss**, classified by net wealth:

- `COST-KILLED`: net wealth at the 15% haircut is less than or equal to 1;
- `CONTINGENT`: net wealth at the 15% haircut is greater than 1, but at the
  58% haircut is less than or equal to 1;
- `COST-SURVIVES-ROBUST`: net wealth at both haircuts is greater than 1.

The microcap p95 is a mandatory adverse sensitivity and receives the same
classification. Verdict severity is robust < contingent < killed. Append
`TAIL-WARNING` when p95 has any worse classification than p75; do not relabel
the primary verdict. If no microcap has both an admissible cap and nominal
price, verdict `DATA-FAIL`.

`COST-SURVIVES-*` means only that costs did not kill the avenue. It is not a
positive-return claim, not evidence versus buy-and-hold, and not permission to
start #2901 while #2900 fails. The standalone “buy microcaps” framing is also
superseded by #2899: any future size hypothesis belongs inside preregistered
quality arm #2901.

## Reproduction and contamination control

The verifier is read-only, requires a clean worktree, records the execution
commit and exact query/source hashes, and performs one database transaction that
emits canonical JSON and Markdown from one typed evidence object. It refuses if
the universe is empty, census conservation fails, the frozen lane/closures have
changed, or any priced row cannot resolve exactly one frozen cost band.
