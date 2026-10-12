# #3612 slice 2: daily loss on marked P&L, not realised only

Source: #3612 risk lens. *"Daily loss counts realised P&L only. Use realised + Δunrealised from marks."*
The desk skill records the same requirement: `.claude/skills/quant/portfolio-construction-and-risk.md`, "Risk limits that act", *"Daily loss on marked P&L, not realised only."*

## Problem

The paper executor refuses an entry with `portfolio_daily_loss_limit` when today's realised P&L is at or below −(`pool_base` × `max_daily_loss_pct` / 100). The check is at `app/services/strategy_paper_executor.py`, `_observe_local_mandate_risk`. Today's realised P&L is the `realized_pnl_usd` sum of close events in the New York market day (`_MANDATE_OBSERVATION_SQL`).

An open book can fall by any amount without a single close, and that check never sees it. One example: a fully invested pot on a −5% SPY day, with no stop hit. Its realised P&L is 0, so new entries keep opening into the fall. The live pool event (`growth`, `max_daily_loss_pct` = 2.5, capital 40,000; `SELECT risk_profile, max_daily_loss_pct, capital_limit FROM strategy_paper_pool_events ORDER BY strategy_paper_pool_event_id DESC LIMIT 1`, 2026-10-12) would take about a 5% loss of `pool_base`, twice the limit, with the gate silent.

## Source rule

- **Daily P&L is the change in the book's mark-to-market value over the day.** This is the definition behind the pot NAV that #3541 already measures: GIPS 2020 values the portfolio at fair value and links returns over sub-periods. The day's P&L is the NAV change over one sub-period with principal flows excluded. Excluding the flows is exact here, because `PotNav` separates `principal` from `pnl` (`app/services/engine_pot_risk.py`, `PotNav`). So:

  `daily_pnl = PotNav(now).pnl − PotNav(anchor).pnl`

  - Both terms are measured over the same exact-owned population (`engine_book_rows` at the pot epoch, membership point-in-time at each instant per `book_membership`).
  - Realised P&L is cumulative to each instant (`trade_events.executed_at <= at`).
  - Unrealised P&L is the broker's per-position `unrealized_pnl` at each instant.
  - A partial or full close between the two instants lands in the realised term and leaves the unrealised term. A close at the anchor's mark therefore contributes 0, and the day's loss is neither double-counted nor missed.
- **The anchor's unrealised P&L is the broker's own official per-position mark**, as stored by #3068 in `broker_account_position_marks`. It is the same `TradingRealAdminApi_Position.unrealizedPnL` field the executor's live snapshot carries (sql/383 header). No candle close stands in for it, so both instants are marked by one source.
- **Limit and denominator are unchanged:** `pool_base × mandate_max_daily_loss_pct / 100`, the same as the realised check (sql/311 denominates every mandate percentage on effective pot capital).

**Fixed by construction** (no published rule):

- **Anchor instant: the stored account snapshot for the UTC calendar date before the New York market day's start.** For market date D, `market_day_start` is 00:00 America/New_York on D (the executor's existing day boundary). The anchor is the `broker_account_equity_snapshots` row with `snapshot_date = (market_day_start at UTC).date() − 1 day`, together with its `broker_account_position_marks` child set. That row is the last observation of that UTC day: the parent upsert keeps only a later same-day observation (`account_equity_evidence.record_account_equity_snapshot`). Its `observed_at` is therefore before `market_day_start`.
  - Why not "the latest row observed before `market_day_start`": a later write the same UTC day replaces the row. Take an observation at 00:43Z on 2026-10-12, which precedes Monday's 04:00Z market start. It was overwritten at 00:51Z the same night, and any later write that day replaces it again.
- **Anchor freshness: the anchor must be observed at or after the close of the last NYSE session before D.** That is 16:00 America/New_York on the session date, or 13:00 on a `us_market_status` half day. Otherwise the "day" would include part of the previous session. Measured on the dev DB (query below): all 27 completed demo rows from 2026-09-15 to 2026-10-11 meet it. Their earliest observation time of day is 20:14:37Z on 2026-09-16, and the regular close is 20:00Z in EDT. A stale or absent anchor refuses (below); it is never coerced.
- **The gap between the anchor and `market_day_start`** (the previous evening's extended hours after the last write) counts toward day D. That makes the day at most about 4–5 hours longer than the NY calendar day, all of it outside the regular session.

## Design

1. **`engine_pot_risk.observe_pot_pnl_at_anchor(conn, environment, market_day_start, prior_close) -> Decimal`** (new; no existing function changes).
   - It reads the anchor parent row and its marks.
   - It builds the `{position_id: BrokerDirectPositionInvestment}` map from the marks, filling only the fields `_price_book` reads: `position_id`, `instrument_id`, `unrealized_pnl`.
   - It prices `engine_book_rows(conn, epoch)` at the anchor's `observed_at` with the existing `_price_book` and returns `realised + unrealised`.
   - It raises `EngineCapitalObservationError` with a new reason code:
     - `engine_pot_daily_anchor_missing`: there is no parent row for the anchor date.
     - `engine_pot_daily_anchor_stale`: the anchor's `observed_at` is earlier than `prior_close`.
     - Anything `_price_book` already raises, unchanged. For example, a position held at the anchor that is absent from its marks raises `engine_capital_ownership_unwitnessed`.
   - The epoch is read the way `observe_pot_nav` reads it (first pool event), so both instants share one population.
2. **Executor.** In `_observe_local_mandate_risk`, after the existing realised check and inside the same transaction:
   - Compute `pot = observe_pot_nav(conn, risk)` once. It is already computed for drawdown, so it is hoisted, not duplicated.
   - Compute `anchor_pnl` from step 1, with `prior_close` = the regular-close instant of the latest NYSE session before market date D (`market_calendar`).
   - If `pot.pnl − anchor_pnl <= −daily_loss_limit`, return `portfolio_daily_marked_loss_limit`.
   - An `EngineCapitalObservationError` returns its reason code, as the drawdown path already does.
   - The realised check stays: it remains a binding refusal when an anchor-to-now mark is unavailable but closes alone breach.
3. **Out of scope, deliberately: the frozen preview gates.**
   - `ai_trial_start_gate` and `ranking_pot_activation` keep realised-only. They are files in `ai_trial_policy.POLICY_MODULES` and `ranking_pot_policy.POLICY_MODULES`, hashed into live declarations 16 and 23, and editing them is `policy_drift`.
   - Both import `_MANDATE_OBSERVATION_SQL` from the executor, so **that constant must stay byte-identical**. This slice adds a term and edits no existing SQL.
   - A later declaration picks up the marked check by calling the new function. Both programmes are halted or paused (#2437 10-09), so no live path diverges meanwhile.
4. **Operator label:** `portfolio_daily_marked_loss_limit`, "The mandate's daily loss limit is reached on marked P&L", in `StrategiesPage.tsx`'s refusal map, together with the two anchor codes.
5. **Exits are never blocked.** This is an entry refusal on the same path as the existing daily-loss check.

## Tests

- Pure or near-pure, with `_price_book` fed synthetic rows:
  - a held position falling from +100 at the anchor to −400 now gives daily −500;
  - a half close at the anchor mark gives daily 0;
  - a position opened after the anchor contributes its full unrealised P&L;
  - a position fully closed today contributes realised minus the anchor's unrealised.
- DB:
  - a missing anchor row returns `engine_pot_daily_anchor_missing`;
  - a row observed before `prior_close` returns `engine_pot_daily_anchor_stale`;
  - a held-at-anchor position absent from the marks raises the existing ownership code;
  - the executor refuses `portfolio_daily_marked_loss_limit` when marked P&L breaches and realised P&L does not.
- Drift guard: `scripts.ai_trial_policy_guard` reports MATCH for declaration 16. The ranking-pot hash is unchanged (`RANKING_POT_POLICY_HASH` recomputed, compared before and after).

## Full-population verification

The anchor must exist and be post-close on every market day the executor runs. Over all demo rows for completed UTC dates since the marks table began (2026-09-15). Today's row is still being overwritten and anchors no market day yet.

```sql
SELECT snapshot_date, observed_at,
       (SELECT count(*) FROM broker_account_position_marks m
         WHERE m.environment = s.environment AND m.snapshot_date = s.snapshot_date) AS marks
FROM broker_account_equity_snapshots s
WHERE environment = 'demo' AND snapshot_date >= '2026-09-15'
  AND snapshot_date < (now() AT TIME ZONE 'UTC')::date
ORDER BY snapshot_date;
```

Measured 2026-10-12:
- 27 consecutive completed dates, 2026-09-15 to 2026-10-11, with no gap;
- every row carries its marks (7–11 per row);
- the minimum time-of-day of `observed_at` is 20:14:37Z (2026-09-16), so every row is after that day's 20:00Z regular close.

Rows before 2026-09-15 have no marks; the anchor rule refuses them, which affects no current market day.

## Security

No authorisation surface changes. This is an additional fail-closed entry refusal on the demo paper executor, which reads only stored broker evidence.
