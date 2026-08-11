# Point-in-time SEC filing-drift result

Status: **inconclusive primary / not promotable**. Sealed outcome for #2476,
opened once on 2026-08-10. This result implements the frozen contract in
`2026-08-10-pead-preregistration.md`; it does not authorise a manifest entry,
capital allocation, TP/SL bracket, or neighbouring parameter search.

## Reproducibility identity

- Preregistration commit: `12dff916`
- Initial implementation commit: `1bf78256`
- Retained implementation: `07b5604d` (PR #2494)
- Source archive SHA-256:
  `126056a91f8d0446bd0f9c04f7db84da7e405d171c541fe72c7aae70d5b6c02b`
- Latest entry with a complete 62-session outcome: 2026-04-06
- Primary evidence interval: 2022-01-01 through that sealed frontier

Reproduce the source census with
`uv run python scripts/verify_2476_pead_source.py`. Reproduce the retained
outcome report with
`uv run python scripts/verify_2476_pead_outcomes.py --open-sealed-holdout`.
The latter flag now acknowledges access to an already-opened interval; reruns
are reproducibility checks, never new holdout evidence.

## Preregistered primary result

The equal-gross long/short historical-SUE arm returned **+0.440%** after the
declared spread and slippage per 62-session event. Its date-clustered,
Bonferroni 95% joint confidence interval was **[-2.866%, +3.763%]**. The lower
bound is not positive, so the primary gate failed.

The pooled result retained 2,427 issuer-deduplicated events across 508 entry
dates. Win rate was 48.78%, profit factor 1.047, worst event -140.37%, and 5%
expected shortfall -46.72%. These tails alone make the measured effect
unsuitable for autonomous capital.

| arm | events | net mean | confidence interval | win rate | profit factor |
| --- | ---: | ---: | ---: | ---: | ---: |
| Long | 1,162 | +2.676% | [+0.141%, +5.005%] | 50.17% | 1.448 |
| Short | 1,265 | -1.797% | [-5.330%, +1.683%] | 47.51% | 0.792 |
| Matched middle-SUE control | 2,558 | -0.407% | [-1.189%, +0.432%] | — | — |

The positive long arm is follow-up evidence only. It cannot inherit a pass
from the failed, preregistered primary comparison, and the opened interval may
not be reused to tune a long-only rule.

## Recency and horizon diagnostics

- trailing 24 months pooled: +1.269%, CI [+0.233%, +2.392%]
- trailing 36 months pooled: +0.652%, CI [-0.368%, +1.670%]
- declared 5/20/40-session pooled diagnostics: -0.566% / -0.263% / -0.098%

The favourable 24-month slice does not replace the primary interval after it
has been observed. Likewise, the three shorter horizons cannot replace the
frozen 62-session horizon. Doing either would turn diagnostics into an
undeclared search.

## Coverage and refusals

The causal source produced 29,557 SUE observations from 2022 onward: 2,546
long, 2,740 short, and 24,271 middle controls. Outcome filtering retained 2,427
signal events. The largest refusal counts were:

| refusal | events |
| --- | ---: |
| Prior-20-session median dollar volume below USD 10m | 1,577 |
| Entry price below USD 5 | 598 |
| Incomplete 62-session outcome | 343 |
| Missing price series or entry | 275 |
| Incomplete prior liquidity window | 131 |

## Decision

Retain this as negative/inconclusive evidence and count it in the global trial
register. Do not expose it as a selectable strategy and do not allocate demo or
live capital. Independent blockers also remain: survivor-only equity history,
unmodelled CFD carry and FX, no portfolio sizing/drawdown/turnover/capacity
trial, and no preregistered stop-loss/take-profit policy.

Any future filing-text hypothesis or prospective long-only replication must
have a distinct mechanism, identity, untouched interval, and preregistration.
It is not a continuation of this trial.
