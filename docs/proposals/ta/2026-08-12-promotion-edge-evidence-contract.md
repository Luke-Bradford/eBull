# Promotion viability and edge-attribution contract (#2505)

Status: implemented 2026-08-12. This is a promotion refusal contract, not a
claim that any current candidate passes it.

## Decision

A reproducible backtest result is necessary but no longer sufficient to cross
from `research_candidate` to `historical_validated`. The transition must pin one
or more immutable result rows and every pinned result must carry a complete,
passing `promotion-edge-evidence-v1` record.

This closes a real control-plane gap. `promote_strategy()` previously verified
that result IDs belonged to the exact strategy version, but it did not require
economic viability or edge-attribution evidence. There are currently zero
registered capital candidates, so this did not authorise a bad allocation. It
would have become fail-open as soon as a future candidate was registered.

## Required viability evidence

The compact record carries and the shared gate enforces:

- after-cost clustered-bootstrap expectancy lower bound strictly above zero;
- measured profit factor strictly above one;
- maximum drawdown, expected shortfall, worst gap, expectancy excluding the
  best 1%, portfolio concurrency and USD capacity;
- the best-1%-excluded expectancy remaining strictly positive;
- at least two recent years evaluated, with each year's population,
  after-cost expectancy, lower confidence bound and risk verdict—not only a
  declared recent-year stability boolean;
- actual maximum date, instrument and sector contribution percentages;
- a versioned candidate/mandate risk-limit verdict covering drawdown, tails and
  concentration;
- complete target-first, stop-first and timeout counts, with ambiguity count,
  and a non-empty resolved population;
- probability calibration and at least three ordered forecast-EV buckets whose
  realised expectancy is monotonic and discriminating; and
- explicit spread, slippage, daily financing and FX values, their source and
  observation/valid-through dates, and current broker eligibility; and
- the complete profitable-versus-losing population and eight bounded aggregate
  contrasts: model feature score, execution cost, entry gap, liquidity, market
  stress, and sector/date/instrument concentration.

Missing is not zero and does not reduce a score. Each dimension has a distinct
refusal code so an operator can see what must be measured or repaired.

## Required edge attribution

Every candidate must carry exactly one of each predeclared challenger role:

1. raw instrument shock;
2. market-only residual;
3. market-plus-sector residual;
4. matched random entries; and
5. unfiltered eligible signals under the same overlap rule.

Each challenger records population size, expectancy, candidate-minus-
challenger expectancy and whether it used the exact same causal observations
and fills. Candidate and challenger records pin the causal observation, fill
and overlap rule versions; a label alone cannot assert comparability. Matched
challenger populations must equal the candidate's resolved outcome count. The
unfiltered-eligible population may be larger, but must use the identical
overlap rule and cannot be smaller. A missing challenger, rule mismatch,
population mismatch or non-positive candidate delta refuses promotion. The
ordered EV buckets must likewise partition the whole candidate population; a
favourable subset cannot stand in for ranking evidence. A model therefore
cannot claim edge merely because a broad market move, selection filter,
overlap rule or random timing also made money.

Every contrast carries both population counts, both means and their exact
difference. All eight must use the evidence record's complete profitable and
losing populations. This makes differences diagnostic without persisting a
feature matrix or allowing a favourable feature subset to masquerade as the
whole comparison.

The challenger vocabulary is intentionally strict for the current short-
horizon residual/shock candidate family. A materially different mechanism must
declare an equivalently strong versioned challenger contract rather than fill
these roles with labels that do not apply.

## Storage and database impact

Migration `327_strategy_promotion_evidence.sql` creates one append-only row per
`strategy_results_store.result_id`:

- primary-key/FK lookup only; no secondary or JSON index;
- canonical SHA-256 payload identity;
- update and delete rejected by a database trigger;
- hard 64 KiB JSON ceiling in both writer and database; and
- aggregate diagnostics only—no bar features, rolling indicators, non-firing
  scans, polling snapshots or raw broker payloads.

Measured before implementation, dev held 196 result rows and the result store
used 548,864 bytes. A representative complete evidence record serialises to
4,357 compact bytes, about 0.85 MB if every existing result had one. Even the artificial
hard ceiling would bound the current population near 12.25 MiB. Normal operation
should remain close to the representative shape.

## Integrity properties

- The Python constructor refuses non-finite metrics, invalid loss signs,
  malformed path partitions, duplicate challengers, unknown cost inputs,
  non-contiguous EV ranks and more than ten aggregate EV buckets.
- The loader verifies the canonical SHA-256 and strictly checks JSON scalar
  types; a raw string such as `"false"` cannot become truthy evidence.
- A result can receive one record only. Corrected evidence requires a new result
  identity, preserving the old failed/incomplete record.
- The historical promotion transition loads the exact pinned record and returns
  all economic refusal codes. Forward-observation promotion rechecks every
  newly pinned result as well. A prose `evidence_ref` cannot substitute for it.
- No evidence endpoint or Strategies-page panel is added. Failed controls and
  research candidates remain out of the money-first product surface.

## Validated universe scope (#2605)

Everything above judges a candidate's evidence. This section fixes the
population that evidence is allowed to describe.

A v1 capital candidate's evaluated instruments must all lie inside the §4.0
validated universe — US listing venue, eToro `Stocks` type, tradable. The axis
is the listing venue, not issuer domicile and not quote currency, so a US-listed
ADR is in and a UK-listed issuer is out. Settled at
`docs/settled-decisions.md` → "v1 strategy capital universe is US-only".

The pure result gate enforces this at RESULT-PRODUCTION time: `check_promotable`
refuses `instrument_outside_validated_universe` when
`evaluated_instrument_ids - validated_universe_ids` is non-empty
(`app/services/strategy_result.py:850`), and `run_backtest` — the sole writer of
`strategy_results_store` — fills that set from `load_validated_universe`
(`app/services/backtest_run.py:1204`, reaching the candidate at `:2837`). The
scope definition lives in one place,
`app/services/strategies/validated_universe.py`, pinned by
`tests/test_validated_universe.py`.

⚠ **An empty evaluated set is refused separately** (`no_instruments_evaluated`).
`set() - anything` is empty, so a result over no instruments would otherwise
satisfy the subset test while being no evidence at all.

**The promotion transition re-checks this since #2621 (2026-08-12).**
`run_backtest` freezes each result's universe inputs in
`strategy_result_universe` (evaluated ids + the run's loaded universe,
immutable + hashed, in the pair's own transaction), and `promote_strategy`
replays `evaluated ⊆ validated` from the frozen record for every pinned result
at the evidence stages — a result without a record refuses
`evaluated_universe_unrecorded`, closing the same "fail-open as soon as a
candidate is registered" shape #2505 closed for viability evidence. ⚠ The
replay is against the universe FROZEN at result time, never today's
`load_validated_universe`; the reasons are recorded in
`app/services/strategy_result_universe.py`. The remaining non-persisted gate
inputs are the scope boundary below, unchanged.

**What this bounds for #2363, and what it does not.** Every instrument in the
validated universe is quoted in USD — asserted, not merely displayed, on the
full population by `scripts/measure_2605_universe_scope.py`, which exits
non-zero if that stops being true. So on the strategy path the **venue
quote-conversion** component of FX is closed by construction.

⚠ That is one component, not the FX question. It says nothing about account /
settlement currency, dividend and corporate-action currency, withholding, ADR
depositary conversion and fees, or the issuer's own currency exposure — and it
is a corpus census, not a schema constraint, so an exchange reclassification or
a newly admitted `us_equity` venue can invalidate it while every test stays
green. #2363's other half — the live execution path, which is not restricted to
this universe — is out of scope here and stays #2363's to bound.

**Not restricted by this section:** core allocation (#2603), whose instrument is
a mandate/eligibility question rather than a strategy-validation one, and the
advisory surfaces, which never place trades autonomously.

## Scope boundary

This contract closes #2505's viability/attribution gap. It does not create
alpha, repair survivor-only history, supply broker cost data, or promote any of
S-1 through S-4. The pure result gate continues to enforce holdout access,
trial count, effective sample size, ambiguity, quarantine and synthetic
controls when results are produced. The control-plane transition now
additionally enforces this persisted contract. This change does not pretend
that the older gate's non-persisted set-valued inputs can be reconstructed from
a result ID; making every one of those inputs independently replayable at the
transition remains a separate control-plane hardening task.

## First worked example: #2499

The rejected residual-confluence development result is the first contract
walk-through, not a passing example. Its published evidence maps to these
independent refusals:

- `expectancy_lower_bound_not_positive`: all four development intervals cross
  zero and conservative 2025 expectancy is -1.332%;
- `recent_year_instability` and `probability_calibration_failed`: the action
  boundary loses in conservative 2025 despite positive predicted EV;
- `tail_or_concentration_limits_failed`: 36.1% of accepted 2025 trades enter on
  one date, while portfolio drawdown, expected shortfall and capacity were not
  produced;
- `executable_cost_inputs_missing`: slippage, financing, FX and current quote
  evidence were explicitly absent;
- `challenger_evidence_incomplete`: raw-shock, market-only and matched-random
  arms were not run; and
- `outcome_contrast_evidence_incomplete`: feature differences were reported,
  but the cost, gap, sector/date/name and other required same-population
  contrasts were not.

The source result also records the material integrity incident: diagnostic
processes loaded bars through the intended 2026 holdout, so that interval is
permanently contaminated and cannot validate a rescued variant. No evidence
row is fabricated for #2499 because its verifier deliberately wrote no strategy
result and several mandatory populations were never measured. The immutable
audit remains in `2026-08-10-residual-confluence-development-result.md`.

The long-only historical-SUE follow-up (#2493) was reviewed as the next possible
candidate and deliberately not pulled forward: its positive observation is at
62 sessions, while its 5/20/40-session diagnostics are negative and the recent
interval is already opened. Building it as the short-horizon answer would be
another instance of selecting the next favourable number. It remains a
separate long-horizon, prospective-only research option.
