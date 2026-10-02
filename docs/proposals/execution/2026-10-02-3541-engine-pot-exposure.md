# #3541 slice 2 — exposure capacity measured on the engine pot, not the demo account

Slice 1 (`docs/proposals/execution/2026-10-02-3541-engine-pot-drawdown.md`, #3574) moved drawdown onto
the pot. This slice moves the two exposure caps. Operator scope (2026-10-01): *the engine measures only
its own book*. Revised after Codex ckpt-1 (33 findings): the first draft priced exposure from snapshot
amounts of point-in-time-held positions plus `_PENDING_RISK_SQL`, which leaves a resolved-after-snapshot
entry in neither half, double-counts a filled-but-unresolved one, and omits an allocation with no order
yet. The sandbox's own population has none of those gaps, so it is used instead.

## Problem

`strategy_paper_executor._capacities` (`:1018`) computes, from the broker ACCOUNT snapshot:

- `portfolio = equity × max_portfolio_exposure_pct/100 − total_invested − pending_total`
- `instrument = equity × max_instrument_exposure_pct/100 − current_instrument − pending_instrument`

`equity`, `total_invested` and `current_instrument` (`risk.instrument_investments`) include the
operator's VOO/QQQ/NXH/IEP, two copy mirrors and SVXY/SVOL. So an operator holding in the same
instrument consumes the engine's room, and the cap scales with money the engine was never assigned.

## Source rule

- **Denominator = `pool_base`**, the sandbox bound. The mandate is stored on
  `strategy_paper_pool_events` (sql/311) and every other mandate limit in `_capacities` — cash reserve,
  active risk, loss-at-stop, daily loss — is denominated on `pool_base` (executor comment at
  `:872-876`). The exposure pair was the only one on account equity, an artefact of where the number
  came from, not a policy (same comment).
- **Numerator = the sandbox's `committed`** (`strategy_engine_capital.resolve_engine_capital_usage`):
  alpha funding amounts of every allocated, non-terminal lifecycle since the pot epoch (including an
  allocation with no trade or unresolved order yet), core pending requested amounts, and core active
  snapshot amounts. Alpha is counted from allocation to terminal state, one row per funding decision
  (`strategy_trades.funding_decision_id` is UNIQUE, sql/281), so alpha has no snapshot/claim timing
  window and no pending/held double count. It is a COMMITMENT cap at cost, as the account basis was
  (eToro `amount`): appreciation does not consume room, and a partial alpha close leaves the funded
  amount counted.
- **Per instrument**: the same population restricted to one instrument — alpha by the funded signal's
  `strategy_signals.instrument_id` (NOT NULL, FK; sql/255), and an alpha trade whose
  `instrument_id` differs from its signal's refuses `engine_capital_population_incomplete`; core
  pending by `strategy_trades.instrument_id`; core active by the snapshot row (already validated equal
  to the configured core instrument).
- **Units**: the mandate bars shorts and leverage (`shorts_allowed`/`leverage_allowed`, sql/311), so
  every engine position is long ×1 and its amount is its notional exposure.

## Design

1. `EngineCapitalAuthority` gains `alpha_committed_by_instrument` and `core_pending_by_instrument`
   (sorted `tuple[tuple[int, Decimal], ...]`, immutable), filled by the queries that already sum them.
   `EngineCapitalUsage.committed_in(instrument_id)` = alpha + core pending for that instrument, plus
   core active when it is the core instrument. Invariant (tested): the per-instrument figures sum to
   `committed`.
2. `_capacities` is split. The arithmetic moves to `_capacity_terms(..., cash, portfolio, instrument,
   ...)` taking the three account-or-pot terms as inputs; `_capacities` keeps its exact keyword
   signature as the account-basis adapter, because the frozen v1 start gate calls it (Constraint).
3. `_risk_and_amount` calls `_capacity_terms` with
   `portfolio = pool_base × pct/100 − usage.committed`,
   `instrument = pool_base × pct/100 − usage.committed_in(intent.instrument_id)`,
   `cash = available_cash − pending_total` (unchanged: the account's ability-to-pay ceiling, only
   narrows). Each floored at 0, as today. `pending_instrument` is no longer read by the executor.
4. `ranking_pot_activation.preview_capital` calls `_capacity_terms` with the pot portfolio term from
   the `PreviewShared.pool_base`/`committed` it already carries (instrument stays its documented
   favourable `pool_base × pct/100`, no holdings). `PreviewShared` is frozen and not changed.
5. No migration. The preflight's `account_equity`/`account_invested`/`instrument_invested` stay the
   account facts their names state. Recording the sizing inputs (`pool_base`, `committed`,
   per-instrument committed) — which no preflight has ever recorded for the sandbox, reserve or
   active-risk terms either — belongs to the per-entry trade ticket, #3542 (next in queue).

## Review round 2 (ckpt-1, 30 findings) — disposition

- Concurrency: every executor sizes inside `_allocator_lock` (`strategy_paper_executor.py:256`), the
  advisory lock that serialises read-risk-reserve-submit; two entries cannot spend the same room.
- New in this slice and handled: signal↔trade instrument agreement (refusal above); the
  per-instrument sum equals `committed` (tested).
- Everything else raised — stranded allocations, terminal-release proof, core partial resolution, core
  instrument rotation, cross-arm ownership overlap, poison history, policy-per-intent percentages —
  is a property of the population the #2844 sandbox term ALREADY enforces on every entry. This slice
  adds no new dependence on it; findings stand for #3541 slice 3 / the P2 provenance work.
- Caps here bind ALPHA entries only (paper, trial, ranking pot). Core buys are not sized by
  `_capacities` and can raise exposure; that is unchanged.

## Constraint — frozen code is not edited

`ai_trial_start_gate.py` is in v1's `POLICY_MODULES`; declaration 16 is live. #3574 edited it and broke
the hash (restored by #3575; a fast-tier test now pins it). It keeps the account basis in its step-0
PREVIEW. The executor is authoritative, so the preview can disagree both ways — admit a pair the
executor then sizes down or refuses, or refuse one the pot would admit. That is v1's frozen preview
being imprecise; the trial gates nothing (#2437, 2026-10-01). `ranking_pot_activation.py` is
ranking-pot-hashed but no declaration exists (`ranking_pot_declarations` empty, 2026-10-02 13:00Z).
Re-check immediately before merge; if one has been frozen, do not merge item 4 — split it out.

## Consequences

- At 2026-10-02 figures (`pool_base` 40,000; live policies on deployments 8/9 = portfolio 100%,
  instrument 10%): instrument room 4,000 less the engine's own committed in that instrument, against
  ~10,500 of account equity before. Tickets: trial 125/250; pot slots ≤ 480 (12,000 / 25).
- An alpha entry in the core instrument (SPY.RTH, ~16k committed) gets zero instrument room — the
  intended concentration rule. Core itself is sized by `strategy_core_executor` under its own mandate
  target and the sandbox, never `_capacities`; a later core buy is unaffected by this slice.
- At 100% the portfolio term equals `pool_base − committed`, the fixed-mode sandbox headroom; it binds
  separately only for a policy below 100%.
- Caps prevent additions only. A pot already over a cap (after a principal cut or policy tightening)
  refuses further entries in that scope and does not force exits — unchanged from today.

## Not in this slice

Per-deployment risk state → live gate, and the monitoring display label (slice 3). Slice-1 observer
findings raised again by this review (principal not snapshot-timed; duplicate ownership rows) are
noted on #3541 for slice 3, which re-reads that observer.

## Tests

Pure: `_capacity_terms` with pot inputs (instrument 10% of 40,000 less 3,000 committed → 1,000);
`_capacities` adapter reproduces the old account arithmetic exactly. `committed_in` sums alpha + core
pending + core active per instrument and the per-instrument sum equals `committed`. DB (one): the
authority loader's per-instrument split, including an allocation with no trade yet.
