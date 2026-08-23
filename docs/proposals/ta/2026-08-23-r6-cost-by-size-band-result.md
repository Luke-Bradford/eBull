# R6 cost by size band result (#2907)

Verdict: **COST-SURVIVES-ROBUST**

Declaration SHA-256: `1dfd13d6835dc3370dd4cabe9828ff8bfaa8a00de2cb68a7972f7cb00e44d559` at commit `538d5b3db7fe6088110c91fd96b6e9a0b62ef460`.
Execution commit: `ce0bbc5db4b5b783aac810451b9ea65e434f4276`. Measured: `2026-08-23T21:22:47.094309+00:00`.
Universe: `validated-universe-us-stocks-v1`; cost model: `static-p75-insession-v3+split-adjusted-max+carry-fx-structural-zero-long-x1-real-usd`.
This is a live-snapshot long/x1/real/USD spread diagnostic, not a backtest or return claim.

## Population and coverage

- Full population: 6773; distinct IDs: 6773.
- Instrument currencies: `{'USD': 6773}`.
- Market-cap coverage: 43.141887%.
- Latest-stored nominal-price coverage: 14.262513%.
- Unavailable cap reasons: `{'null': 3851}`.
- Unavailable price reasons: `{'null': 5807}`.
- Cartesian cells: `{'micro|priced': 104, 'micro|unpriced': 953, 'small|priced': 195, 'small|unpriced': 582, 'mid|priced': 197, 'mid|unpriced': 453, 'large|priced': 162, 'large|unpriced': 276, 'unknown_market_cap|priced': 308, 'unknown_market_cap|unpriced': 3543}`.
- Quote range: `2026-07-02 19:59:52.462345+00:00` to `2026-08-21 19:59:56.868264+00:00`; oldest/newest age seconds: `4497774.631964` / `177770.226045`.

## Size-band distribution

| Size | N total | N priced | Statistic | Spread % | 1 RT loss % | £/£1k | £/£10k | 3 RT loss % |
|---|---:|---:|---|---:|---:|---:|---:|---:|
| micro | 1057 | 104 | p50 | 0.571000 | 0.569374 | 5.693744 | 56.937444 | 1.698416 |
| micro | 1057 | 104 | p75 | 1.450000 | 1.439563 | 14.395632 | 143.956317 | 4.256818 |
| micro | 1057 | 104 | p95 | 1.450000 | 1.439563 | 14.395632 | 143.956317 | 4.256818 |
| micro | 1057 | 104 | maximum | 1.450000 | 1.439563 | 14.395632 | 143.956317 | 4.256818 |
| micro cost bands |  |  | counts | `{'$20-100': 22, '$5-20': 50, '<$5': 32, '>=$100': 0}` |  |  |  |  |
| small | 777 | 195 | p50 | 0.509000 | 0.507708 | 5.077079 | 50.770788 | 1.515404 |
| small | 777 | 195 | p75 | 0.571000 | 0.569374 | 5.693744 | 56.937444 | 1.698416 |
| small | 777 | 195 | p95 | 1.450000 | 1.439563 | 14.395632 | 143.956317 | 4.256818 |
| small | 777 | 195 | maximum | 1.450000 | 1.439563 | 14.395632 | 143.956317 | 4.256818 |
| small cost bands |  |  | counts | `{'$20-100': 113, '$5-20': 66, '<$5': 10, '>=$100': 6}` |  |  |  |  |
| mid | 650 | 197 | p50 | 0.509000 | 0.507708 | 5.077079 | 50.770788 | 1.515404 |
| mid | 650 | 197 | p75 | 0.509000 | 0.507708 | 5.077079 | 50.770788 | 1.515404 |
| mid | 650 | 197 | p95 | 0.571000 | 0.569374 | 5.693744 | 56.937444 | 1.698416 |
| mid | 650 | 197 | maximum | 0.571000 | 0.569374 | 5.693744 | 56.937444 | 1.698416 |
| mid cost bands |  |  | counts | `{'$20-100': 129, '$5-20': 15, '<$5': 0, '>=$100': 53}` |  |  |  |  |
| large | 438 | 162 | p50 | 0.322000 | 0.321482 | 3.214824 | 32.148241 | 0.961350 |
| large | 438 | 162 | p75 | 0.509000 | 0.507708 | 5.077079 | 50.770788 | 1.515404 |
| large | 438 | 162 | p95 | 0.509000 | 0.507708 | 5.077079 | 50.770788 | 1.515404 |
| large | 438 | 162 | maximum | 0.571000 | 0.569374 | 5.693744 | 56.937444 | 1.698416 |
| large cost bands |  |  | counts | `{'$20-100': 48, '$5-20': 2, '<$5': 0, '>=$100': 112}` |  |  |  |  |

## Haircut test

| Haircut | Return ceiling | Micro p75 net | Micro p95 net |
|---:|---:|---:|---:|
| 15.000000% | 17.255000% | 12.263669% | 12.263669% |
| 58.000000% | 8.526000% | 3.906246% | 3.906246% |

- p75 classification: `COST-SURVIVES-ROBUST`; p95: `COST-SURVIVES-ROBUST`.
- Micro-minus-large p75 three-round-trip loss: `2.741413845985802585233567670` percentage points; ratio `2.809032017341443619634250813`.

## Consequence

A surviving verdict means only that the frozen spread table does not itself falsify the avenue under the vendor total-return ceiling. It establishes no absolute return and no edge versus buy-and-hold. #2900 still blocks every Tier 2 arm; any future size hypothesis belongs inside #2901 rather than a standalone microcap sleeve.
