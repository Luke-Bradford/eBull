# #3543 — daily engine-book risk snapshot vs mandate (gap register P4)

Status: proposal (slice 1). Measurement only.

## Problem

Entry-time limits are enforced (`strategy_paper_executor.py` capacity / reserve / concurrency / daily loss /
drawdown; `strategy_paper_runtime.py` health blocks), and the pot's NAV drawdown is tracked (#3541,
`engine_pot_risk.py`). Nothing persists the book's volatility, beta, concentration, stress loss or stale marks,
and the mandate's `target_volatility_pct` (sql/311) is displayed but read by nothing.

## Scope

One row per completed NYSE session and policy version for the **engine book as measured at `measured_at`,
marked at that session's close**, compared with the mandate in force at `measured_at`. Measurement only: no
refusal authority, no whole-account authority (#2844), no operator page. #2843 makes an operator alert a
REFUSAL surface ("may fire only when the system cannot proceed under policy"); a measurement that refuses
nothing is not one. Comparisons are persisted as per-check results the app can read.

## Source rule

| Measure | Rule | Source |
|---|---|---|
| Book population | exact-owned positions of eligible trades (allocated paper deployment or core arm) created at/after the pot epoch, held at `measured_at` | `engine_pot_risk.observe_pot_nav` / `_price_book` (#3541) — same query, same completeness refusals |
| Denominator | effective pot capital: `sandbox_bound(capital_limit, capital_mode, realised_delta)` | `strategy_capital_sandbox.sandbox_bound` (#2844); sql/311 states the mandate's percentages are of "effective pot capital" |
| Position mark | mark-to-market first; cost basis only when no mark exists | settled-decisions "AUM basis" |
| Returns | simple, between CONSECUTIVE sessions only | `risk_metrics.py` math contract (no gap-spanning return) |
| Historical vol | sample (n-1) std × √252 | `risk_metrics.annualized_vol` |
| Forecast vol | EWMA, λ = 0.94, zero mean | RiskMetrics Technical Document (J.P. Morgan/Reuters, 4th ed. 1996), ch. 5: zero-mean EWMA, λ = 0.94 for daily data |
| Beta | OLS on SPY, date-intersected | `risk_metrics.ols_beta`; SPY via `_resolve_benchmark_instrument_ids` (primary-listing tiebreak) |
| Concentration | largest / top-5 instrument share of gross; HHI = Σ sᵢ² with sᵢ in percent (0–10,000) | US DoJ/FTC Horizontal Merger Guidelines 2010 §5.3 (percent-share convention) |

**Fixed by construction** (no published rule; frozen in `ENGINE_BOOK_RISK_POLICY = "engine-book-risk-v1"`):

- **Window:** the 252 most recent session returns to the mark session (252 = `risk_metrics.TRADING_DAYS`, the
  annualisation constant, so window and annualisation agree). Minimum 60 aligned returns
  (`risk_metrics.MIN_RETURNS_VOL_BETA`) for any vol or beta, book or per-position.
- **EWMA adaptations:** simple returns (RiskMetrics uses log; the rest of this module and `risk_metrics` use
  simple); seeded with the first squared return (RiskMetrics' own recursion start; after 252 updates the seed's
  weight is λ²⁵² ≈ 1.7e-7); one update per session in the window, chronological. Output = the next-session
  conditional σ annualised by √252 — a scaled one-day forecast, not a one-year forecast.
- **Stress:** a linear single-factor approximation, not a historical book loss: per instrument
  max(−1, βᵢ · R_SPY), weighted by wᵢ, summed — signed (a loss is negative). Instruments whose βᵢ is undefined
  use βᵢ = 1 and are counted, with their exposure, as `beta_defaulted`. Scenarios: SPY close 2020-02-19 → 2020-03-23
  and 2022-01-03 → 2022-10-12 — the S&P 500's closing peak and trough of each episode. **Price** return (close),
  matching the price-return betas it multiplies:
  - `STRESS_2020 = 222.95 / 338.34 − 1 = −0.34104747` (8 dp),
  - `STRESS_2022 = 356.56 / 477.71 − 1 = −0.25360574`.
  Frozen constants; reproduce with
  `SELECT bar_date, close FROM research_price_daily WHERE series_id = 7694 AND bar_date IN ('2020-02-19','2020-03-23','2022-01-03','2022-10-12')`
  (series 7694 = `icyDenev/Intrader` SPY). The four dates are also the max/min `close` of series 7694 over
  2020-01-01..2020-06-30 and 2021-12-01..2022-12-31 — checked over every bar in those ranges, not a sample.

## Design

1. **Inputs, one read.** In one `snapshot_read` (REPEATABLE READ): the engine capital authority, the latest pool
   event's mandate (sql/311), the book population (`observe_pot_nav`'s query, its refusals reused verbatim: an
   open/closing/closed trade with no ownership → refuse), `broker_positions` for every held position, and closes.
   Held = claimed ≤ `measured_at` and not released by it.
2. **Session.** `latest_completed_us_session(measured_at)` (market_calendar). SPY must have a valid close for it,
   else the run refuses `benchmark_not_ready` and writes nothing (the next fire retries).
3. **Book shape.** Every held position must have a `broker_positions` row, `is_buy`, `leverage = 1`, and an
   instrument with currency `USD`; otherwise the run refuses `book_shape_unsupported` naming the position. No
   coercion. Effective pot capital ≤ 0 → refuse `pot_exhausted`.
4. **Marks.** Per position, mark = the instrument's last valid (finite, > 0) close on or before the session;
   MV = units × mark. No valid close at all → cost basis `initial_amount_in_dollars × units / initial_units`
   (remaining cost after partial closes; `initial_units` NULL → `initial_amount_in_dollars`), counted
   `cost_marked`. **Stale** = cost-marked, or mark date < session.
5. **Units of aggregation.** Positions aggregate to instruments for concentration, weights and returns (four
   SPY.RTH lots are one 100 % instrument). wᵢ = MVᵢ / effective capital. Concentration shares are of gross.
6. **Return series.** The session calendar is SPY's valid-close dates, last 253 up to the session (252 returns).
   An instrument's return on session t exists only if it has valid closes on t AND on the previous calendar
   session; an invalid or missing close removes both adjacent returns. Book return rₚ,ₜ = Σ wᵢ rᵢ,ₜ on sessions
   where every held instrument has a return. `vol_n_obs` and `beta_n_obs` are stored separately.
7. **Statuses** (`history_status`): `ok`, `insufficient_history` (< 60), `degenerate` (zero SPY variance),
   `empty_book` (no held position: gross 0, vols/beta/stress 0, concentration NULL).
8. **Checks** (`checks JSONB`, one entry per check: `status ∈ {evaluated, unknown, no_limit}`, `value`, `limit`,
   `flagged`). `unknown` when the measure is NULL; `no_limit` when the mandate is unconfigured or that limit is
   NULL (sql/311's CHECK admits a NULL limit on a configured row). The set:
   - `forecast_vol_vs_target` — flagged when EWMA vol > `target_volatility_pct`. A fact under any reading of
     "target"; it does not define the target's semantics, which stay open (P4).
   - `stress_2020_vs_max_drawdown`, `stress_2022_vs_max_drawdown` — flagged when the signed stress return
     < −`max_portfolio_drawdown_pct`. Reads "this scenario alone, from today's book, would exceed the drawdown
     limit"; it is not a running-peak drawdown.
   - `positions_vs_max_concurrent` — open engine trades (the executor's lifecycle unit) vs the effective
     concurrency cap (`EFFECTIVE_MAX_CONCURRENT_SQL`).
   - `stale_marks` — flagged when any position is stale; always evaluated, mandate or not.
   **Not measured in slice 1** (stated so absence is not read as compliance): `active_risk_budget_pct` (an ALPHA
   budget; the book includes core), `cash_reserve_pct`, `max_daily_loss_pct`, `max_loss_per_position_pct`.
9. **Persistence** `sql/460` `engine_book_risk_snapshots`, append-only (UPDATE/DELETE refused). PK
   `(session_date, policy_version)`; a second run for the same key is a no-op (`ON CONFLICT DO NOTHING`), so a
   policy change re-measures under a new key. Columns: `measured_at`, `capital_usd` (effective), `pool_event_id`,
   `gross_usd`, `position_count`, `instrument_count`, `open_trade_count`, `cost_marked_count`, `stale_count`,
   `largest_share_pct`, `top5_share_pct` (fewer than five → all), `hhi`, `hist_vol_pct`, `ewma_vol_pct`, `beta`,
   `vol_n_obs`, `beta_n_obs`, `sample_first`, `sample_last`, `history_status`, `beta_defaulted_count`,
   `beta_defaulted_weight_pct`, `stress_2020_pct`, `stress_2022_pct`, `checks JSONB`, `positions JSONB` (per
   position: trade id, position id, instrument, units, mark, mark date or `cost`, MV, weight of capital, βᵢ with
   its n_obs or `defaulted`). Every NUMERIC finite (`<> 'NaN'`); percentages in percent units, 8 dp; comparisons
   made on the unrounded values. A refused run writes no row; its reason is the job's recorded error.
   Known limit: closes revised later make a row non-reproducible from today's prices; the row stores what it used.
10. **Job** `engine_book_risk_snapshot`, daily 09:00 UTC with boot catch-up, lane `risk_metrics` (DB-only; the dev
    cluster has no connection headroom for a new lane). Missed sessions stay gaps: membership and units are
    current-state, so a past session cannot be re-measured honestly.
11. **Pure core.** `app/services/engine_book_risk.py::compute_snapshot(inputs) -> Snapshot`; `inputs` carries
    the session, positions, closes, SPY closes, capital, mandate and the frozen policy constants. No I/O; table-tested.

## Slices

- **Slice 1:** sql/460, the pure core, loader/writer, job + manual trigger.
- **Slice 2:** read endpoint + Portfolio-tab panel; FX-converted marks; own-history stress where an instrument's
  series covers both scenario endpoints.

## Tests (slice 1)

Pure: consecutive-session rule (a gap drops both returns), empty book, < 60 obs, zero-variance SPY, stress sign
and the −100 % floor, β-default counting, cost-mark fallback incl. partial close, stale classification, every check
status (`no_limit` on unconfigured and on NULL-limit configured mandates), HHI scale, top-5 with fewer than five.
DB: one end-to-end run over a seeded book writes one row; a second run is a no-op; UPDATE/DELETE refused; a short
or non-USD position refuses with no row.

## Out of scope

Any refusal, sizing or rebalancing driven by these measures. Defining the vol-target semantics. Sector and
look-through concentration.

## Codex ckpt-1 (64 findings) — dispositions

Applied: as-of (#2, #5 → measured_at membership, session-close marks, one snapshot read); population refusals
reused (#3, #4); denominator → effective pot capital, no implied cash (#6–#9); alpha budget not checked (#10);
mark policy, partial-close cost, invalid closes, stale incl. cost-marked (#11–#14); unsupported shapes refuse
(#16, #17 — the USD claim is now a check, not a census); calendar session + SPY readiness (#18, #19, #61); Basel
citation dropped, window fixed by construction (#21–#23); constant-weight series named (#24); consecutive-session
returns (#25, #26); separate n_obs (#28); per-position minimum (#29); `degenerate` status (#30); EWMA recursion
fixed (#31–#34); price-return scenarios to match betas (#35); default-β exposure stored (#37); instrument unit
(#38); HHI scale (#39); empty book (#40); share naming (#41); exact constants (#42, #43); full-range extremum
check (#44); stress labelled an approximation, signed, floored (#45–#48); vol flag is descriptive (#49);
per-check status incl. NULL limits (#50, #54); unmeasured clauses listed (#51); executor concurrency unit (#52);
stale independent of mandate (#53); per-scenario checks (#55); PK includes policy version (#57); audit fields
(#59); precision rules (#60); pure inputs (#62); tests (#63); sector claim removed (#64).
Accepted as stated limits: missed sessions stay gaps (#20, #56, #58); intersection on all held instruments (#27);
book β ≠ Σwᵢβᵢ by design — book β is the OLS, stress uses per-position βᵢ (#36); revised prices (#59).
#15 (corporate actions): the mark is the latest close, normally the session's own; price_daily is
back-adjusted by the provider, so units × close is consistent for a current mark.
