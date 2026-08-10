# Strategy evidence refresh and capital semantics

Status: implementation contract for #2469. This narrows, and does not weaken,
the validation and execution gates in the existing TA specifications.

## Operator promise

The Strategies page separates three facts:

1. live signal observation, which advances with the validated market-data
   frontier;
2. frozen research evidence, currently ending **2026-07-08** under
   `CORPUS_VERSION`; and
3. allocation/execution approval, which remains unavailable until every
   evidence, cost, risk and broker-contract gate passes.

“Refresh evidence” means complete the declared recent-regime denominator. It
does not append today's data to the frozen corpus, select arbitrary dates, or
replace an existing result. Advancing the frozen date is a deliberate corpus
re-freeze: the corpus version moves and old results remain attributable to the
old version.

## Recent denominator

The required windows remain code-pinned:

- 2022 onward;
- rolling 36 and 24 months;
- calendar 2022, 2023, 2024 and 2025; and
- 2026 year-to-frozen-date.

One refresh evaluates the full runnable strategy set in each missing window so
Deflated Sharpe uses the registered measured trials. Each completed window is
committed as a restart boundary. A later run skips exact completed result
identities. A partial immutable window is an operator-repair condition, never
an invitation to overwrite the audit trail.

Only compact aggregate results and hold-out access records are retained.
Indicators, positions and returns are recomputed from the existing research
bars while the job runs; no additional per-bar metric store is introduced.

## What qualifies a strategy

Win rate is descriptive, not a promotion target. A 75% win rate can be created
by taking tiny profits against rare catastrophic losses and is therefore not a
safety claim. Promotion continues to require, at minimum:

- positive after-cost expectancy whose confidence interval excludes zero;
- declared-trial/Deflated-Sharpe evidence and random-entry control;
- stable recent-window performance, drawdown and tail-loss bounds;
- point-in-time universe, quarantine and ambiguity-arm completeness;
- measured broker costs, financing/borrow where applicable, and executable
  stop/take-profit semantics; and
- current signal scan plus approved paper execution policy.

No rule is enabled merely because the UI can display it or a historical win
rate exceeds a threshold.

## Capital model

The shared pot carries an explicit, audited mode:

- `fixed`: the configured principal is the hard strategy-system ceiling;
  net realised profits do not increase it and net realised losses reduce what
  remains available;
- `compound`: the risk base is configured principal plus reconciled realised
  strategy P&L.

Open marks never increase buying power. In either mode, incomplete realised
P&L reconciliation blocks new entries because an unknown loss must not be
subsidised by the wider broker account.

Each approved strategy carries an explicit, revisioned ticket mode:

- `fixed`: one configured currency amount per accepted signal; or
- `percent`: one configured fraction of its effective allocated base, still
  bounded by the shared remaining pot, per-instrument exposure, drawdown and
  broker cash limits.

These settings configure sizing only. They cannot bypass approval, health,
cost, freshness, ownership or kill-switch gates. Manual portfolio positions
remain outside strategy capital ownership and lifecycle decisions.

## Candidate expansion

The four current manifest rows are research controls, not a claim that only
four trading ideas exist. New short-horizon candidates enter as registered,
disabled trials. Current evidence supports investigating intraday residual
reversal and carefully defined extreme-move reversal; the internal post-drop
short result is a discovery candidate, not a clean hold-out result. Opening
range breakout remains a cost-sensitivity spike. Order-flow imbalance is out
of scope until a free source provides causal depth/aggressor data.

Long/short support also requires borrow availability, financing, gap-through
loss treatment, broker short eligibility and direction-aware signal/outcome/
execution schemas. Leverage remains out of scope until the unlevered demo
lifecycle passes forward observation and kill drills.
