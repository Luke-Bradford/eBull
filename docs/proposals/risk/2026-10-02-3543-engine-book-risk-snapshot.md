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

**Fixed by construction** (no published rule; frozen in `ENGINE_BOOK_RISK_POLICY`, `"engine-book-risk-v1"` at slice 1,
`"engine-book-risk-v2"` since #3612 added the 2008 scenario, `"engine-book-risk-v3"` since #3612 measured x1
shorts instead of refusing them — see "v3: signed weights"):

- **Window:** the 252 most recent session returns to the mark session (252 = `risk_metrics.TRADING_DAYS`, the
  annualisation constant, so window and annualisation agree). Minimum 60 aligned returns
  (`risk_metrics.MIN_RETURNS_VOL_BETA`) for any vol or beta, book or per-position.
- **EWMA adaptations:** simple returns (RiskMetrics uses log; the rest of this module and `risk_metrics` use
  simple); seeded with the first squared return (RiskMetrics' own recursion start; after 252 updates the seed's
  coefficient is λ²⁵¹ ≈ 1.8e-7 over a full window, but λ⁵⁹ ≈ 2.6 % at the 60-return minimum); one update per session in the window, chronological. Output = the next-session
  conditional σ annualised by √252 — a scaled one-day forecast, not a one-year forecast.
- **Stress:** a linear single-factor approximation, not a historical book loss: per instrument
  max(−1, βᵢ · R_SPY), weighted by wᵢ, summed — signed (a loss is negative). Instruments whose βᵢ is undefined
  use βᵢ = 1 (v3: βᵢ = 0 when the instrument's net exposure is short) and are counted, with their exposure, as
  `beta_defaulted`. Scenarios: SPY close 2007-10-09 → 2009-03-09
  (v2, #3612), 2020-02-19 → 2020-03-23 and 2022-01-03 → 2022-10-12 — the S&P 500's closing peak and trough of each episode. **Price** return (close),
  matching the price-return betas it multiplies:
  - `STRESS_2008 = 68.110001 / 156.479996 − 1 = −0.56473669` (8 dp),
  - `STRESS_2020 = 222.95 / 338.34 − 1 = −0.34104747`,
  - `STRESS_2022 = 356.56 / 477.71 − 1 = −0.25360574`.
  Frozen constants; reproduce with
  `SELECT bar_date, close FROM research_price_daily WHERE series_id = 7694 AND bar_date IN ('2007-10-09','2009-03-09','2020-02-19','2020-03-23','2022-01-03','2022-10-12')`
  (series 7694 = `icyDenev/Intrader` SPY). The six dates are also the max/min `close` of series 7694 over
  2007-01-01..2009-12-31, 2020-01-01..2020-06-30 and 2021-12-01..2022-12-31 — checked over every bar in those ranges, not a sample.

## Design

1. **Inputs, one read.** In one `snapshot_read` (REPEATABLE READ): the engine capital authority, the latest pool
   event's mandate (sql/311), the book population (`observe_pot_nav`'s query, its refusals reused verbatim: an
   open/closing/closed trade with no ownership → refuse), `broker_positions` for every held position, and closes.
   Held = claimed ≤ `measured_at` and not released by it.
2. **Session.** `latest_completed_us_session(measured_at)` (market_calendar). SPY must have a valid close for it,
   else the run refuses `benchmark_not_ready` and writes nothing (the next fire retries).
3. **Book shape.** Every held position must have a `broker_positions` row, `leverage = 1`, and an
   instrument with currency `USD`; otherwise the run refuses `book_shape_unsupported` naming the position. No
   coercion. Effective pot capital ≤ 0 → refuse `pot_exhausted`. (v1/v2 also refused `is_buy = false`; v3 measures
   it, see "v3: signed weights".)
4. **Marks.** Per position, mark = the instrument's last valid (finite, > 0) close on or before the session;
   MV = s × units × mark with s = +1 long, −1 short (v3; v1/v2 had longs only). No valid close at all → cost basis
   s × `initial_amount_in_dollars × units / initial_units`
   (remaining cost after partial closes; `initial_units` NULL → `initial_amount_in_dollars`, which overstates the
   remaining cost after a partial close — an approximation; 0 of 10 dev rows had NULL `initial_units` on
   2026-10-12, `SELECT count(*) FILTER (WHERE initial_units IS NULL), count(*) FROM broker_positions`), counted
   `cost_marked`. **Stale** = cost-marked, or mark date < session.
5. **Units of aggregation.** Positions aggregate to instruments for concentration, weights and returns (four
   SPY.RTH lots are one 100 % instrument). Two quantities per instrument (v3): net exposure MVᵢ = the signed sum
   of its positions' MV, used for weights wᵢ = MVᵢ / effective capital, returns, beta and stress; and gross
   exposure Gᵢ = Σ|MV| over its positions. Gross = Σ Gᵢ, net = Σ MVᵢ. Concentration shares are Gᵢ / gross. For a
   long-only book MVᵢ = Gᵢ, so every v2 figure is unchanged.
6. **Return series.** The session calendar is the NYSE calendar (`nyse_sessions`), last 253 sessions up to the session (252 returns);
   a session with no valid SPY close drops SPY's two adjacent returns like any other missing close. (A text
   correction in v3's amendment, not a policy change: `load_inputs` has built this calendar with `nyse_sessions`
   since v1; earlier text said "SPY's valid-close dates".)
   An instrument's return on session t exists only if it has valid closes on t AND on the previous calendar
   session; an invalid or missing close removes both adjacent returns. Book return rₚ,ₜ = Σ wᵢ rᵢ,ₜ on sessions
   where every held instrument with non-zero net exposure has a return (v3; v1/v2 held longs only, so every held
   instrument). `vol_n_obs` and `beta_n_obs` are stored separately.
7. **Statuses** (`history_status`): `ok`, `insufficient_history` (< 60), `degenerate` (zero SPY variance),
   `empty_book` (no held position: gross 0, vols/beta/stress 0, concentration NULL). A non-empty book whose every
   instrument nets to MVᵢ = 0 (v3) is not `empty_book`: its book return is 0 on every session with an SPY return,
   so vol, EWMA vol, β and stress are 0 under `ok`, `insufficient_history` below 60 SPY returns, or `degenerate`
   on zero SPY variance; gross and concentration come from Gᵢ as usual.
8. **Checks** (`checks JSONB`, one entry per check: `status ∈ {evaluated, unknown, no_limit}`, `value`, `limit`,
   `flagged`). `unknown` when the measure is NULL; `no_limit` when the mandate is unconfigured or that limit is
   NULL (sql/311's CHECK admits a NULL limit on a configured row). The set:
   - `forecast_vol_vs_target` — flagged when EWMA vol > `target_volatility_pct`. A fact under any reading of
     "target"; it does not define the target's semantics, which stay open (P4).
   - `stress_2008_vs_max_drawdown` (v2), `stress_2020_vs_max_drawdown`, `stress_2022_vs_max_drawdown` — flagged when the signed stress return
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
   `beta_defaulted_weight_pct`, `stress_2020_pct`, `stress_2022_pct`, `stress_2008_pct` (sql/481; NULL only on v1
   rows), `net_usd` (sql/483; NULL only on v1/v2 rows), `checks JSONB`, `positions JSONB` (per
   position: trade id, position id, instrument, side (`long`/`short`, v3), units, mark, mark date or `cost`, signed
   MV, signed weight of capital, βᵢ with its n_obs or `defaulted`). Every NUMERIC finite (`<> 'NaN'`); percentages in percent units, 8 dp; comparisons
   made on the unrounded values. A refused run writes no row; its reason is the job's recorded error.
   Known limit: closes revised later make a row non-reproducible from today's prices; the row stores what it used.
10. **Job** `engine_book_risk_snapshot`, daily 09:00 UTC with boot catch-up, lane `risk_metrics` (DB-only; the dev
    cluster has no connection headroom for a new lane). Missed sessions stay gaps: membership and units are
    current-state, so a past session cannot be re-measured honestly.
11. **Pure core.** `app/services/engine_book_risk.py::compute_snapshot(inputs) -> Snapshot`; `inputs` carries
    the session, positions, closes, SPY closes, capital, mandate and the frozen policy constants. No I/O; table-tested.

## v3: signed weights (#3612)

The mandate allows an unleveraged x1 CFD short for research and paper. v1/v2 refused the whole snapshot on one,
so a paper short book would have had no risk row at all. v3 measures it; leverage ≠ 1 and non-USD still refuse
(no leverage until validated; a leveraged position's `initial_amount_in_dollars` is margin, not exposure, so the
cost fallback would be wrong).

- **Sign.** `broker_positions.units` is a positive quantity on both sides; direction is `is_buy`, the same
  convention as `engine_nav_bridge._sign` (unrealised = s × units × (close − open)). Holding today's units, a short's
  P&L over session t is −units × Δpriceₜ = (s × units × priceₜ₋₁) × rₜ, so Σ wᵢ rᵢ with today's signed weights is the
  same constant-weight hypothetical series as for longs — not the book's realised path. CFD financing and borrow fees are not in MV or in the return.
- **Long and short lots of one instrument net** in MVᵢ (weights, returns, beta, stress), but not in gross or
  concentration: Gᵢ sums each lot's |MV|, so long $100 A + short $100 A + long $100 B is gross $300, net $100,
  largest share 66.67 %. An instrument whose lots net to MVᵢ = 0 contributes nothing to the book return and is
  not required to have history. Counts (positions, cost-marked, stale) stay per position.
- **Side** comes from `broker_positions.is_buy`, which is `NOT NULL` (sql/024), so there is no unknown side to
  coerce.
- **Stress floor** stays on the instrument's return, max(−1, βᵢ · R_SPY): a price cannot fall below zero, which bounds
  a short's scenario gain at |wᵢ|. Nothing bounds its loss.
- **Undefined β on a net-short instrument defaults to 0, not 1** (fixed by construction; no published rule). Every
  scenario is a market fall, so β = 1 would credit an unmeasured short with a hedge gain; β = 0 credits none. It
  is not a loss bound either (a true β of −1 would lose), which is why the defaulted exposure is persisted beside
  every stress figure.
- **`beta_defaulted_weight_pct`** (v3) = Σ Gᵢ / effective capital × 100 over defaulted instruments: a non-negative
  gross magnitude, so a +50 % and a −50 % defaulted name report 100 %, not 0 %. The count stays per instrument.
- **`net_usd`** (sql/483) is stored beside `gross_usd`; v3 rows must carry it, v1/v2 rows keep NULL. For a long-only
  book every v2 figure is unchanged under v3; the policy moves so the new column and the short-side β default sit
  under their own key.

**Known limits, stated so absence is not read as coverage:** every scenario is a market fall, so the tail of any
book holding a short (a rally, a squeeze, an idiosyncratic jump), net long or net short, is not stress-tested
here. Each position row persists its side, and the panel states the limit whenever a short is held. A short strategy is decided by its tails and delisting attribution
(`.claude/CLAUDE.md`, risk posture), which this snapshot does not measure.

## Slices

- **Slice 1:** sql/460, the pure core, loader/writer, job + manual trigger.
- **Slice 2:** read endpoint + Portfolio-tab panel; FX-converted marks; own-history stress where an instrument's
  series covers both scenario endpoints.

## Tests (slice 1)

Pure: consecutive-session rule (a gap drops both returns), empty book, < 60 obs, zero-variance SPY, stress sign
and the −100 % floor, β-default counting, cost-mark fallback incl. partial close, stale classification, every check
status (`no_limit` on unconfigured and on NULL-limit configured mandates), HHI scale, top-5 with fewer than five.
DB: one end-to-end run over a seeded book writes one row; a second run is a no-op; UPDATE/DELETE refused; a
leveraged or non-USD position refuses with no row (v1/v2 also refused a short; v3 writes its row with a negative MV).
Pure (v3): a pure-short book has the mirrored long's vol and the negated book β; mixed books are checked against
explicit expected values (gross, net, signed weights, concentration on Gᵢ, book vol from the signed series, e.g.
+w A and −w A on identical returns give vol 0); an all-offset book; opposing lots of one name; the short β default of 0 and the gross defaulted weight;
signed cost fallback with and without `initial_units`; a short's stress gain bounded at |wᵢ| and a negative-β
short's loss unbounded; per-position stale counts. DB: a v3 row beside a v2 row for the same session.

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
Accepted as stated limits: missed sessions stay gaps (#20, #56, #58); intersection on all held instruments with non-zero net exposure (#27);
book β ≠ Σwᵢβᵢ by design — book β is the OLS, stress uses per-position βᵢ (#36); revised prices (#59).
#15 (corporate actions): the mark is the latest close, normally the session's own; price_daily is
back-adjusted by the provider, and units × close ASSUMES the broker's units sit on the same post-action basis
for a current mark; this snapshot does not verify that. A stale mark that spans a
corporate action is not corrected; the stale flag records its age, not its basis.
