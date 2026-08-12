# Split-adjusted research prices and nominal transaction-cost bands

**Status:** implemented as a conservative research correction  
**Issue:** #2400  
**Scope:** research/backtest costing only; no live-order path and no schema change

## Decision

Every costing caller declares one of two price bases:

- `as_traded`: the entry price may select the frozen nominal-price spread band;
- `split_adjusted`: the historical nominal price is unknown, so the maximum
  calibrated spread band (`<$5`, 1.450% round trip) is charged.

The policy is part of
`cost_model_id = static-p75-insession-v2+split-adjusted-max`, so every strategy
identity moves. A result produced under v1 cannot be mistaken for a corrected
result.

This is an **adverse falsification arm**, not reconstructed historical cost.
It deliberately prefers false negatives to a favourable cost assumption. A
strategy unable to survive it is not worth advancing; survival does not prove
that its actual fills were obtainable.

## Evidence and rejected alternatives

The linked corpus contains 5,269 split-adjusted series and zero
`price_adjustments` rows. There is therefore no point-in-time split factor with
which to reconstruct as-traded price. The issue census found both directions of
the error: reverse splits can undercharge distressed microcaps, while forward
splits can overcharge old large-cap history. Reading the adjusted number as a
nominal level is invalid regardless of aggregate direction.

- Reconstructing nominal prices was rejected because the required source data
  is absent; inferring factors would manufacture evidence.
- Continuing to select a band from adjusted price was rejected because it
  silently assigns precise but economically meaningless costs.
- Dropping only visibly extreme series was rejected because an arbitrary
  threshold leaves the same category error below the threshold.

## Boundaries that remain

This correction does not make the frozen quote sample representative. It is
mostly closing-hour data from nine summer dates; carry and FX remain unknown.
The promotion evidence contract therefore still requires fresh, dated broker
cost evidence before a strategy can become capital-eligible.

It also does **not** repair strategy eligibility rules expressed as nominal
price thresholds (for example S-2's `$1` floor). Those must either receive a
point-in-time as-traded price source or be replaced with a scale-invariant,
point-in-time liquidity rule. Until then they remain declared limitations, not
supporting evidence.

## Storage and operations

No database table, column, index, or retained observation is added. The basis
is an in-memory property of each costed position and the policy is carried by
the immutable cost-model identity stored on the existing result. Database size
impact is zero; old results remain historically attributable to v1 and are not
rewritten.

The whole-corpus verification command is:

```bash
PYTHONPATH=. uv run python scripts/verify_2240_cost_model.py --positions
```

It must cover every validated linked research series, report zero costing
property violations, and show the v2 model identity before corrected backtests
are accepted.

## Whole-corpus result (2026-08-12)

The command above completed over all 5,266 validated linked series and
3,163,173 positions with zero property violations.

| strategy | prior v1 net win rate | corrected adverse-arm net win rate | median net trade |
| --- | ---: | ---: | ---: |
| S-1 time-series momentum | 31.628% | **19.264%** | **-143 bp** |
| S-3 mean reversion in trend | 51.591% | **46.879%** | **-50 bp** |

S-1's 3,133,100 realised closes lost 22.978 percentage points of win rate
relative to gross. S-3's 27,782 realised closes lost 8.131 points. These are not
viable promotion results and must not be shown as capital-ready metadata.

The comparison is diagnostic rather than an estimate of true historical cost:
v1 scrambled bands in both directions, while v2 applies the maximum band to
every adjusted trade. Its useful conclusion is narrower and strong: neither
candidate survives the adverse cost arm, so obtaining exact historical nominal
prices cannot be deferred in order to promote either one.
