# #3541 slice 1 — drawdown measured on the engine pot, not the demo account

Gap register P2 (`docs/proposals/2026-10-01-gap-register.md`), narrowed by the operator on
2026-10-01: *the engine measures only its own book*. Inventory on #3541 (2026-10-01 17:29Z).

## Problem

One row, `strategy_paper_account_risk_state` (sql/288), holds a high-water mark on the
broker ACCOUNT's `equity`. The account also holds the operator's VOO/QQQ/NXH/IEP, two copy
mirrors and the SVXY/SVOL experiment. Measured 2026-10-02 12:00Z: account equity
104,994.41, high water 108,198.42 (2.96%); the pot's assigned capital is 40,000
(`strategy_paper_pool_events` id 5). Five readers gate on that row:

| site | effect |
| --- | --- |
| `strategy_paper_runtime.refresh_strategy_health` | sets the global `drawdown` execution block |
| `strategy_paper_executor._observe_local_mandate_risk` → `_risk_and_amount` | refuses `account_drawdown_limit` / `portfolio_drawdown_limit` |
| `strategy_core_executor._observe_core_portfolio_drawdown` | refuses `portfolio_drawdown_limit` on core buys |
| `ai_trial_start_gate._preview` | same two refusals, read-only |
| `ranking_pot_activation` preview | same, read-only |

An operator GME move can therefore pass or fail an engine entry. `mandate_max_drawdown_pct`
is a POT mandate (stored on `strategy_paper_pool_events`, every other limit denominated on
`pool_base`), so measuring it on the account is a population error, not a policy choice.

## Source rule

- **Pot NAV** — the population `strategy_wealth` and the #3540 bridge already use:
  `NAV = principal + P&L`, principal = latest `strategy_paper_pool_events.capital_limit`,
  `P&L = realised + unrealised` over exact-owned positions of eligible trades (eligibility =
  `_load_realised_delta`'s: allocated paper-deployment trade or core-arm trade, created at or
  after the pot epoch = first pool event). Realised = `trade_events` close `realized_pnl_usd`
  with `executed_at <= snapshot.observed_at` (cutoff so a close recorded after the snapshot is
  not counted alongside the snapshot's own unrealised for that position). Unrealised = the
  snapshot's `direct_positions[].unrealized_pnl` for every row HELD at the snapshot. Identity is
  `broker_position_id` (UNIQUE, sql/281); no instrument matching. A partial close is realised
  for the closed slice and unrealised for the remainder, by construction.
- **Fees** are a memo, exactly as in #3540 (`totalFees` inclusion in `pnL` is undocumented and
  every stored value is 0); not a term here either.
- **Drawdown** — peak-to-trough decline of a time-weighted NAV index, sub-periods linked per
  GIPS 2020 2.A.24(f). The NAV separates exactly into principal and P&L, so a sub-period's P&L
  is exact; only WHEN a principal flow landed inside the span is unobserved, and the return
  depends on it (withdrawing half before a loss doubles the loss's percentage — Codex ckpt-2).
  Flow-timing policy (2.A.24(e), firm policy): the fail-closed bound,
  `r_t = (P&L_t − P&L_{t−1}) / min(NAV_{t−1}, NAV_{t−1} + F)`, `F = Δprincipal`. The start- and
  end-of-span conventions bound the truth; the smaller base enlarges a loss and, on a gain,
  raises the high water so every later drawdown is measured from a higher peak. With no flow
  it is exact. Pool events are rare operator acts, so the bound rarely binds.
  `index_t = index_{t−1} × (1 + r_t)`; `drawdown = (HWM − index_t)/HWM × 100`.
  Verdict comparison is `>=` at every site (the runtime's `>` is aligned to the executors').
- **Membership** is point-in-time at the snapshot: an ownership row claimed after it is not yet
  in the book, one released after it is still held (needs its mark); every close counts only
  up to `observed_at`. A released-by-then position needs ≥ 1 close by then.

## Design

1. **Migration** `sql/456_strategy_engine_pot_risk_state.sql`: singleton
   `strategy_engine_pot_risk_state(id BOOLEAN PK DEFAULT TRUE CHECK (id), epoch_started_at,
   nav_index, index_high_water, last_nav, last_pnl, last_drawdown_pct, observed_at)`, all
   NOT NULL, `nav_index > 0`, `index_high_water >= nav_index`, `last_nav > 0`,
   `last_drawdown_pct >= 0`; NUMERIC(30,16) for the index pair. The old table is left in place
   and no longer written (no reader remains); its HWM is in account dollars and cannot seed a
   pot index.
2. **`app/services/engine_pot_risk.py`**:
   - `observe_pot_nav(conn, snapshot) -> PotNav(principal, epoch, realised, unrealised, nav,
     observed_at)`: one read of pool + population in the caller's transaction. Raises
     `EngineCapitalObservationError` (existing vocabulary): account currency missing or not
     USD, or duplicate snapshot position id → `engine_capital_snapshot_unusable` (checked even
     with an empty book); an active owned position absent from the snapshot →
     `engine_capital_ownership_unwitnessed`; its snapshot instrument ≠ the trade's instrument,
     or a non-finite pnl → `engine_capital_ownership_mismatched`; no pool, an eligible trade in
     `open`/`closing`/`closed` with no ownership row (the #3540 `trade_without_ownership`
     census, scoped to the eligible population), a released row with no close event, a close
     with NULL pnl, or non-finite/≤0 NAV → `engine_capital_population_incomplete`. Naive
     `observed_at` → `engine_capital_snapshot_unusable`.
   - pure `link(state, pot) -> (index, hwm, drawdown)`.
   - `advance_pot_drawdown(conn, pot) -> Decimal | "engine_pot_risk_stale"`: caller holds the
     transaction. `INSERT … ON CONFLICT DO NOTHING` with the observation as base (index 1,
     drawdown 0), then `SELECT … FOR UPDATE`, so a concurrent first observation cannot race
     (the loser links against the winner; same P&L links r = 0). Stored `epoch_started_at` ≠
     the pot's → refuse `engine_pot_epoch_mismatch`. `observed_at` older than stored → stale.
     Equal or newer → link and write.
   - `preview_pot_drawdown(conn, pot)`: same arithmetic, no write, same refusals.
3. **Wire the five sites.** Observation failures never raise past the existing boundaries:
   - runtime: catch → `drawdown` block active with the refusal code in the reason; a runtime
     snapshot older than the stored state uses the stored `last_drawdown_pct` (newer evidence,
     and the runtime makes no decision on its own snapshot). Persist only when the broker probe
     is fresh, as today.
   - alpha executor: observed inside `_observe_local_mandate_risk`'s own committed
     transaction (as today); refusal codes returned to `_persist_rejection`; stale refuses.
   - core executor: `_observe_core_portfolio_drawdown(conn, snapshot=…)` keeps its limit
     validation, returns `core_account_risk_stale` for stale and the observation code for a
     refusal, so `sell_core` still passes (only non-sell actions are refused) and every
     evaluation still advances the peak.
   - both previews: catch → their existing refusal prefix + code.
   Refusal code strings are unchanged (`account_drawdown_limit` = the per-deployment policy
   limit, `portfolio_drawdown_limit` = the mandate). Preflight column `account_drawdown_pct`
   holds the pot figure from this deploy on; rows before the merge SHA are account-basis
   (rename/discriminator belongs to slice 3, which owns the display).

## Not in this slice (later slices of #3541, stated on-issue)

- per-deployment `strategy_paper_deployment_risk_state` → live gate (needs a per-deployment
  NAV: deployment principal + its own owned positions);
- exposure capacity (`equity × max_portfolio_exposure_pct − total_invested`, instrument term
  from `instrument_investments`) → pot-based;
- `strategy_monitoring.max_observed_account_drawdown_pct` display label.

## Consequences

- The gate becomes LESS conservative when the operator's holdings fall and the engine's do
  not; that is the operator's stated intent. The #2844 sandbox bound, kill switch and every
  capacity term are unchanged.
- Partial isolation only: exposure capacity and the instrument term still read the account
  until slice 2; this slice isolates drawdown.
- A deleted state row re-bases at the next observation (index 1). Deletion is an operator act;
  the module is the only writer.
- Missed peaks: alpha observes after its sandbox/concurrency refusals (as today); the runtime
  health tick observes every cycle while a deployment is enabled.
- The HWM starts at the first post-deploy observation: no history is reconstructed (the
  pot's past intraday NAV was never stored). Stated, not hidden.
- `available_cash` stays the ability-to-pay ceiling (deliberate, only narrows).

## Tests

Pure (`link`): first observation (index 1, dd 0); gain then loss of 10% of NAV → dd 10;
loss of 10 on NAV 100 then +100 principal with no P&L change → dd stays 10 (not 5);
withdrawal with no P&L change → dd unchanged; same-P&L re-observation → r = 0; stale →
refusal; NAV ≤ 0 → refusal. Observer (fake rows): absent / instrument-mismatched / duplicate
position, non-USD and missing currency with an empty book, operator-only position movement
does not change NAV. DB (one per new SQL mechanism): population query (alpha + core, released
excluded, pre-epoch excluded, close after cutoff excluded, trade without ownership refused) and
advance (bootstrap, epoch mismatch, stale). Existing tests of the five sites updated to the
new table; core `sell_core` under a refused observation still proceeds.
