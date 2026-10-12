# #3612 slice 2: daily loss on marked P&L, not realised only

Source: #3612 risk lens. *"Daily loss counts realised P&L only. Use realised + Δunrealised from marks."*
The desk skill records the same requirement: `.claude/skills/quant/portfolio-construction-and-risk.md`, "Risk limits that act", *"Daily loss on marked P&L, not realised only."*

## Problem

The paper executor refuses an entry with `portfolio_daily_loss_limit` when today's realised P&L is at or below −(`pool_base` × `max_daily_loss_pct` / 100). The check is in `app/services/strategy_paper_executor.py`, `_observe_local_mandate_risk`. Today's realised P&L is the `realized_pnl_usd` sum of close events in the New York calendar day (`_MANDATE_OBSERVATION_SQL`).

An open book can fall by any amount without a single close, and that check never sees it. Take a fully invested pot on a −5% SPY day with no stop hit: its realised P&L is 0, so new entries keep opening into the fall. The live pool event allows 2.5% (`growth`, `max_daily_loss_pct` = 2.5, capital 40,000; `SELECT risk_profile, max_daily_loss_pct, capital_limit FROM strategy_paper_pool_events ORDER BY strategy_paper_pool_event_id DESC LIMIT 1`, 2026-10-12). The pot would lose about 5% of `pool_base`, twice the limit, with the gate silent.

## Source rule

- **Daily P&L convention (published).** Interactive Brokers' Daily P&L splits two cases (IBKR knowledge base; TWS 958 release notes, "P&L time zone"):
  - A position held from before today: (current price − prior day's closing price) × quantity.
  - A position opened today: measured against its purchase price.

  The day resets once per day, near midnight in the instrument's own time zone. This slice adopts that convention. **The reference for a held position is its mark at the prior session's close, not at New York midnight.** Moves after the prior close, including extended hours, belong to the new day. The realised check keeps its own NY-calendar-day window. It is unchanged and stays a separate, independently binding limit (see Design 4).
- **Mark source.** Both instants use the broker's own per-position `unrealizedPnL` (`TradingRealAdminApi_Position`, sql/383 header):
  - **now** comes from the executor's live `BrokerAccountRiskSnapshot`;
  - **the reference** comes from the #3068 stored marks (`broker_account_position_marks`).

  No candle close stands in for either.
- **Principal flows are excluded exactly.** `PotNav` separates principal from P&L (`engine_pot_risk.py`, module docstring, GIPS 2020 2.A.24(e)/(f) as already cited there), and this term uses P&L only.
- **Parent/child completeness is a documented precondition:** the sql/383 header requires a snapshot's mark count to equal `official_direct_long_positions + official_direct_short_positions`. **Currency** must be documented as USD, the same refusal `observe_pot_nav` applies (`_snapshot_positions`).
- **Limit and denominator are unchanged:** `pool_base × mandate_max_daily_loss_pct / 100` (sql/311 denominates every mandate percentage on effective pot capital).

**Local policy choices** (no published rule found for these; each is frozen by this spec):

- **Reference instant R.** The prior close is not observed exactly, so the stored snapshot is the proxy. For market date D, take the `broker_account_equity_snapshots` row (demo) with `snapshot_date = (D 00:00 America/New_York, in UTC).date() − 1`. Its `observed_at` is before D's NY midnight under both DST offsets, because the previous UTC date ends at 19:00 or 20:00 NY.
  - **What that row is.** It is the last observation of its UTC date that the upsert accepted. The `ON CONFLICT … WHERE` only accepts a later observation while the stored row's date is still the current UTC date. An observation of date X that arrives after X ends is therefore dropped (`account_equity_evidence.record_account_equity_snapshot`).
  - **Why not "the latest row before NY midnight".** A row of the same UTC date as D's start would be overwritten during D. For example, the 00:43Z 2026-10-12 row was replaced at 00:51Z.
- **Freshness.** R must be observed at or after the close of the session P, defined as `latest_completed_us_session(D 00:00 NY)`. The close is 16:00 NY, or 13:00 on a `us_market_status` half day. An older R refuses `engine_pot_daily_anchor_stale`. Both helpers exist in `market_calendar.py`, which is ranking-policy-hashed, so they are called and never edited.
- **Drift between P's close and R** counts in R's value, not the new day's. That is the residual difference from IBKR's exact-close reference.
  - On a weekday it is at most from 16:00 NY to the last write before 00:00Z. That bound is 4 hours (EDT, close 20:00Z) or 3 hours (EST, close 21:00Z), or up to 7 hours after a half day. The drift is extended-hours only.
  - After a weekend or holiday it can span non-trading days, where most US equities do not trade. The executor then measures from R to now, so its window can be up to about 28 hours long. Every hour of it is either in R (before) or counted (after); nothing is dropped.

## Design

1. **Per-position daily P&L by broker identity.** Membership comes from presence in each snapshot, not from local `claimed_at` / `released_at`, which can lag the broker (ckpt-1 r1 F3).
   - The engine population E is every `broker_position_id` with an ownership row on an eligible trade under the pot epoch (`engine_book_rows`).
   - For each p in E:

     `daily_p = U_now(p) − U_R(p) + Σ realised(p, closes with R.observed_at < executed_at ≤ now)`

     - `U_now(p)` is the live snapshot's `unrealized_pnl` if p is present, else 0.
     - `U_R(p)` is R's stored mark if p is present, else 0.
   - The book's daily P&L is Σ over E. A close at R's mark contributes 0; for example, half closed: −50 + 50 = 0.
   - A position opened after R contributes its unrealised P&L against purchase price, which is IBKR's "new position" case.
2. **Completeness witnesses.** Each returns its reason code and refuses the entry, never coerced.
   - **Anchor.** No parent row gives `engine_pot_daily_anchor_missing`. A currency other than USD, or a NULL count, gives `engine_capital_snapshot_unusable`. A child count different from `official_direct_long_positions + official_direct_short_positions` gives `engine_pot_daily_anchor_incomplete`. A stale R gives `engine_pot_daily_anchor_stale` (above).
   - **Closes (r1 F2).** For every p in E:

     `units_R(p) + Σ open units − Σ close units (events with R.observed_at < executed_at ≤ now) = units_now(p)`

     The units are 0 where p is absent from that side. A mismatch gives `engine_pot_daily_close_unwitnessed`, and so does any close in the window with NULL `realized_pnl_usd`. This catches an unrecorded partial or final close, which would otherwise read as a gain. Full population: 260 of 260 consecutive-snapshot position pairs conserve units (query below).
   - **Live side.** `observe_pot_nav(conn, risk)` already runs on this path and keeps its refusals: an owned position absent from the broker, a mismatched instrument, an unusable snapshot.
3. **One consistent read (r1 F9).**
   - A single SQL statement reads, under one snapshot even in READ COMMITTED:
     - R's parent row;
     - R's marks for E;
     - E itself;
     - the window's `trade_events`.
   - The function `engine_pot_risk.observe_daily_marked_pnl(conn, snapshot, *, market_day_start, prior_close) -> Decimal` is new, and every existing function is unchanged. It combines that statement with the live snapshot in Python.
   - The epoch is the first pool event's `changed_at`, as `observe_pot_nav` reads it. No code path updates or deletes `strategy_paper_pool_events` (grep of `app/` for `UPDATE`/`DELETE` on it, 2026-10-12), so the epoch cannot move inside a window.
4. **Executor placement (r1 F11, F12).**
   - The new check goes in `_observe_local_mandate_risk` **after** `advance_pot_drawdown` has linked the current observation. A daily refusal therefore never withholds a valid observation from the drawdown index.
   - The breach test is `daily <= −daily_loss_limit`, returning `portfolio_daily_marked_loss_limit`. A refusal returns from inside the transaction block, as the drawdown refusal does: the reads and the drawdown advance commit, nothing else.
   - The realised check stays where it is. The two are **independent limits with different windows**:
     - the realised one refuses a −2,000 close in the NY day even when the close was at R's mark;
     - the marked one refuses a −2,000 mark move with no close.

     Both are tested.
5. **Frozen preview gates are out of scope.**
   - `ai_trial_start_gate.py` and `ranking_pot_activation.py` are hashed into declarations 16 and 23 (`ai_trial_policy.POLICY_MODULES`, `ranking_pot_policy.POLICY_MODULES`) and keep realised-only.
   - Both import `_MANDATE_OBSERVATION_SQL` from the executor, and the new code changes none of the functions they call. So **`_MANDATE_OBSERVATION_SQL` stays byte-identical**, pinned by a test on its SHA-256.
   - Both programmes are halted or paused (#2437, 10-09). A later declaration adopts the marked check by calling the new function.
6. **Operator labels** in `StrategiesPage.tsx`'s refusal map:
   - `portfolio_daily_marked_loss_limit`, "The mandate's daily loss limit is reached on marked P&L";
   - one line each for `engine_pot_daily_anchor_missing`, `_stale`, `_incomplete` and `engine_pot_daily_close_unwitnessed`.
7. **Exits.** The check sits only on the entry sizing path (`_risk_and_amount` → `_observe_local_mandate_risk`). Exit and protective paths do not call it. A test asserts that the exit path never calls `observe_daily_marked_pnl`.

## Operational dependency

R exists only if `daily_portfolio_sync` (scheduler, demo) writes at least one accepted snapshot on **every UTC date, trading or not**, after the prior session's close. A missed date refuses entries for the next market day, which is fail-closed. It surfaces as the job's own failure and as the refusal code on the next entry.

## Tests

- **Pure, over the per-position formula:**
  - held, +100 at R → −400 now gives −500;
  - half close at R's mark gives 0;
  - full close at R's mark gives 0;
  - opened after R, now −400, gives −400;
  - opened and closed after R gives its realised P&L;
  - claimed locally after R but present in R's marks counts its R mark;
  - closed before R but released locally after R contributes 0 and does not refuse;
  - principal change with P&L unchanged gives 0.
- **Witnesses:**
  - an unrecorded partial close refuses `close_unwitnessed`, and so does an unrecorded final close after an earlier recorded partial;
  - a close with NULL P&L refuses;
  - a child-count mismatch refuses `anchor_incomplete`, and so does a NULL count, while a genuine zero-position snapshot passes;
  - a non-USD or undocumented currency refuses;
  - events exactly at `R.observed_at` are excluded and events exactly at now are included.
- **Calendar:**
  - R and P for a Monday, for the Tuesday after a Monday holiday, for the day after a half day (13:00 close), and across both 2026 DST transitions (03-08, 11-01) and a year boundary;
  - freshness accepts exactly-at-close and refuses one microsecond earlier.
- **Threshold:** at, just below and just above −limit; a non-breach proceeds to sizing.
- **Executor (DB):**
  - the marked check refuses when realised P&L does not breach;
  - the realised check still refuses when the marked one does not;
  - after a marked refusal, the drawdown state has advanced to the current observation;
  - each anchor and witness refusal reaches the entry's recorded reason;
  - an exit with a breached daily loss still executes.
- **Drift:**
  - `_MANDATE_OBSERVATION_SQL` SHA-256 is pinned;
  - `scripts.ai_trial_policy_guard` reports MATCH for declaration 16;
  - `RANKING_POT_POLICY_HASH` is unchanged before and after.

## Full-population verification

**Anchor availability over every market day since marks began.** For each NYSE session D from 2026-09-16 to 2026-10-12, R and P are derived as above, and the parent row and child count are left-joined:

```sql
SELECT snapshot_date, observed_at, official_direct_long_positions + official_direct_short_positions AS declared,
       (SELECT count(*) FROM broker_account_position_marks m
         WHERE m.environment = s.environment AND m.snapshot_date = s.snapshot_date) AS marks
FROM broker_account_equity_snapshots s
WHERE environment = 'demo' AND snapshot_date >= '2026-09-15'
  AND snapshot_date < (now() AT TIME ZONE 'UTC')::date
ORDER BY snapshot_date;
```

- 27 consecutive completed UTC dates, 2026-09-15 to 2026-10-11, with no gap.
- The earliest `observed_at` time of day is 20:14:37Z (2026-09-16), so every row is after a 20:00Z close. All are EDT; the EST case is covered by the calendar tests.
- This is retained-row coverage, measured retrospectively. It does not prove the row existed at each past decision instant: the upsert can insert a row later. The live gate reads at decision time, and an absent row refuses.
- `declared` against `marks` is checked per row by the implementation's acceptance query (PR evidence), not asserted here.

**Close witness.** Every demo position in consecutive stored snapshot pairs from 2026-09-15 on, which covers the operator's positions too, not only the engine book:

`units(d1) + Σ open units − Σ close units in (observed_at(d1), observed_at(d2)] = units(d2)`

- Holds for **260 of 260** pairs (measured 2026-10-12; the script is in the PR).
- `trade_events` held 23 opens and 13 closes, every close with `realized_pnl_usd`.

## Security

No authorisation surface changes. This is an additional fail-closed entry refusal on the demo paper executor, reading only stored broker evidence and the live snapshot it already holds.
