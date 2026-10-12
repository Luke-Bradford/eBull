# #3612 slice 2: daily loss on marked P&L, not realised only

Source: #3612 risk lens. *"Daily loss counts realised P&L only. Use realised + Δunrealised from marks."*
The desk skill records the same requirement: `.claude/skills/quant/portfolio-construction-and-risk.md`, "Risk limits that act", *"Daily loss on marked P&L, not realised only."*

## Problem

The paper executor refuses an entry with `portfolio_daily_loss_limit` when today's realised P&L is at or below −(`pool_base` × `max_daily_loss_pct` / 100). The check is in `app/services/strategy_paper_executor.py`, `_observe_local_mandate_risk`. Today's realised P&L is the `realized_pnl_usd` sum of close events in the New York calendar day (`_MANDATE_OBSERVATION_SQL`).

An open book can fall by any amount without a single close, and that check never sees it. Take a fully invested pot on a −5% SPY day with no stop hit: its realised P&L is 0, so new entries keep opening into the fall. The live pool event allows 2.5% (`growth`, `max_daily_loss_pct` = 2.5, capital 40,000; `SELECT risk_profile, max_daily_loss_pct, capital_limit FROM strategy_paper_pool_events ORDER BY strategy_paper_pool_event_id DESC LIMIT 1`, 2026-10-12). The pot would lose about 5% of `pool_base`, twice the limit, with the gate silent.

## Source rule

- **Daily P&L convention (published).** Interactive Brokers, TWS Reference, "Profit and Loss" (<https://www.interactivebrokers.com.hk/php/webhelp/TWS_Reference_Information/pnl_.htm>):
  - Daily P&L: *"(current price - prior day's closing price) x (total number of outstanding shares) plus New Position calculation for all new positions."*
  - New Position: *"(current price - purchase price) x (number of outstanding shares purchased today)."*

  This slice adopts that convention. **The reference for a held position is the prior session's closing price.**
- **Which close: eToro's own daily bar for session P.** The marks are eToro's per-position `close_rate`, so the reference must be on the same price basis. Measured on the dev DB and the live candle endpoint, 2026-10-12:
  - Marks and eToro's daily candle close share a basis. Every mark in a Friday, Saturday or Sunday snapshot (09-18 to 10-11, 114 position-days) equals `price_daily.close` for that Friday exactly.
  - eToro's daily cut is per instrument. On 2026-10-09, SPY.RTH, VOO and NXH have no five-minute bar after the one starting 15:55 New York, and that bar's close equals the daily close. QQQ trades 24/5: on 2026-10-08 its daily close (748.61) equals the close of the bar starting 16:55, and differs from the bars either side (748.35, 748.42). So the cut is 16:00 New York for regular-session names and 17:00 for 24/5 names.
  - Only 24/5 names drift after R. Every weekday mismatch between R's mark and P's daily close (29 position-days) is QQQ (3006) or GME (1699). The reprice R → P close, summed over R's marks, reaches −961.55 USD (D = 2026-09-22). `scripts/measure_3612_anchor_coverage.py` prints it per session.
  - **Not the official-closing-price endpoint** (`etoro_closing_price_captures`, sql/480). That is a last-trade print on another basis: VOO 715.59 against a weekend mark and daily close of 715.60. It is undated and captured once a day since 2026-10-10.
  - **Not `price_daily`.** It records no fetch time, so finality cannot be shown at decision time. A bar written before the instrument's cut would latch, because `_candles_are_fresh` then skips it.
  - **Not `etoro_session_close_captures`** (sql/479). It holds 11 instruments for 2026-10-09, none of them held.
- **Mark source.**
  - **now** is the executor's live `BrokerAccountRiskSnapshot`, at `risk.observed_at`.
  - **R** is the #3068 stored marks (`broker_account_position_marks`), repriced to P's close (Design 2).
- **Reprice identity (measured, 262 of 262 stored marks).** `unrealized_pnl = market_value − amount` exactly. `market_value = units × close_rate × close_conversion_rate` within 0.0144, which is cent rounding. All 262 marks are long, USD (`asset_currency_id` 1), with `total_fees` 0. So for a long USD position with no fees: `U at price x = U_R + units_R × close_conversion_rate_R × (x − close_rate_R)`. A short, a non-USD asset currency or non-zero fees has no measured identity and refuses (Design 3).
- **Principal flows are excluded exactly.** `PotNav` separates principal from P&L (`engine_pot_risk.py`, module docstring, GIPS 2020 2.A.24(e)/(f) as already cited there), and this term uses P&L only.
- **Parent/child completeness** is a documented precondition: the sql/383 header requires a snapshot's mark count to equal `official_direct_long_positions + official_direct_short_positions`. **Currency** must be documented as USD, the same refusal `observe_pot_nav` applies (`_snapshot_positions`).
- **Limit and denominator are unchanged:** `pool_base × mandate_max_daily_loss_pct / 100` (sql/311 denominates every mandate percentage on effective pot capital).

**Local policy choices** (no published rule found; each is frozen by this spec):

- **Market day D** is `risk.observed_at`'s New York date, and `D_start` is D 00:00 New York. **P** is `latest_completed_us_session(D_start)`. `P_close` is P's regular close: 16:00 New York, or 13:00 on a `us_market_status` half day. Both helpers are in `market_calendar.py`, which is ranking-policy-hashed, so they are called and never edited.
- **Positions source R.** R is the `broker_account_equity_snapshots` row (demo) with `snapshot_date = D_start.astimezone(UTC).date() − 1`.
  - R is always before `D_start`, because that UTC date ends at 19:00 or 20:00 New York on D − 1.
  - **What that row is.** It is the last observation of its UTC date that the upsert accepted. `record_account_equity_snapshot`'s `ON CONFLICT … DO UPDATE … WHERE` restricts which conflicting inserts update the stored row. An update is accepted only while the stored row's date is still the current UTC date; its `now()` is the transaction start time. A later observation of a date that has already ended is not applied.
  - **Why not "the latest row before New York midnight".** A row with the same UTC date as `D_start` is overwritten during D. For example, the 00:43Z 2026-10-12 row was replaced at 00:51Z.
- **R is used only for its position set and its marks, repriced to P's close.** That needs the engine book to be unchanged between P's cut and R. **Quiet interval:** no `trade_events` row for any position in E falls in `(min(R.observed_at, P_close), D_start]`. Otherwise the entry refuses with `engine_pot_daily_anchor_moved`.
  - The interval covers every per-instrument cut without knowing it, because every cut is at or before `D_start`.
  - It replaces r2's freshness rule. A stale R is still correct when the book was quiet, and refused when it was not.
  - Measured, 18 NYSE sessions from 2026-09-16 to 2026-10-09: 0 engine-owned events in the interval. One session (D = 09-23) has 4 events for two non-engine positions, each opened 20:05 and closed 20:07 New York on 09-22.

## Design

1. **Population E.** E is every `broker_position_id` with an ownership row on an eligible trade under the pot epoch (`engine_book_rows`) whose `claimed_at ≤ risk.observed_at`. This is the same point-in-time cut `book_membership` applies to the snapshot (ckpt-1 r2 B2).
   - Presence in each snapshot decides the U terms, not `released_at`, which can lag the broker (r1 F3).
2. **Per-position daily P&L.** For each p in E, with the window W = `(R.observed_at, risk.observed_at]`:

   `daily_p = U_now(p) − U_ref(p) + Σ realised(p, closes in W)`

   - `U_now(p)` is the live snapshot's `unrealized_pnl` if p is present, else 0.
   - `U_ref(p)` is `U_R(p) + units_R(p) × close_conversion_rate_R(p) × (close_P(instrument) − close_rate_R(p))` if p is in R's marks, else 0.
   - The book's daily P&L is Σ over E. A close at P's close contributes 0.
   - A position opened in W contributes against its purchase price, which is IBKR's "New Position" case.
   - Example, QQQ held: R's mark is 758.56 at 19:48, and P's daily close is 757.92. The reference is 757.92, so the 19:48 after-hours move is counted in D, where r2 dropped it.
3. **Witnesses.** Each refuses the entry with its own code; none is coerced.
   - **Anchor.**
     - No R row gives `engine_pot_daily_anchor_missing`.
     - A currency other than USD gives `engine_capital_snapshot_unusable`, as `_snapshot_positions`.
     - A NULL declared count, or a mark count different from `official_direct_long_positions + official_direct_short_positions`, gives `engine_pot_daily_anchor_incomplete`. A NULL is unproven completeness, so it takes the completeness code. A declared 0 with 0 marks passes.
     - A non-quiet interval gives `engine_pot_daily_anchor_moved`.
   - **Reference.** For each p in E present in R:
     - no fetched close for its instrument dated exactly P gives `engine_pot_daily_reference_missing`;
     - `is_buy` false, `asset_currency_id` other than USD, or `total_fees` ≠ 0 gives `engine_pot_daily_reprice_unverified`.
   - **Events (r1 F2, r2 B2).** For every p in E:
     - `units_R(p) + Σ open units − Σ close units (events in W) = units_now(p)`, with units 0 where p is absent from a side;
     - every close in W has a non-NULL `realized_pnl_usd`;
     - if p's `claimed_at` is in W, at least one open event for p is in W.

     A failure gives `engine_pot_daily_event_unwitnessed`. The third check closes r2 B2: a position opened and closed inside W with no events would otherwise pass as 0.
   - **Splits.** Daily candles are back-adjusted at fetch (`market_data.py`, #2066). A split after R re-bases P's bar, while R's units and rate stay pre-split. The broker changes the units without a trade event, so the unit check above refuses. A split applied before R leaves R and the bar on the same basis.
   - **Live side.** `observe_pot_nav(conn, risk)` keeps its refusals: an owned position absent from the broker, a mismatched instrument, an unusable snapshot.
4. **P's closes are fetched once per cycle, outside the transaction.**
   - `strategy_paper_cycle` (`app/workers/scheduler.py`) already refreshes the halt feed before the cycle. It also fetches the daily close for P there: `EtoroMarketDataProvider.get_daily_candles(instrument_id, 10)` for each distinct instrument in R's marks, keeping the bar with `price_date = P`.
   - Ten bars cover the longest NYSE closure run.
   - The bar is final by construction, because the cycle runs after `D_start`, which is after every cut on P.
   - The result is a `Mapping[int, Decimal]` passed through `run_strategy_paper_cycle` and `execute_fired_paper_signal`. Any fetch failure passes an empty mapping, which refuses entries with `engine_pot_daily_reference_missing` and leaves exits untouched (#3546's halt-feed rule).
   - About 10 GETs per cycle, against the documented 120 market-data GET/min shared budget (etoro-api skill).
5. **One consistent read (r1 F9).** One SQL statement reads, under one snapshot even in READ COMMITTED:
   - R's parent row and its marks;
   - E, with `claimed_at`;
   - the `trade_events` of E in the quiet interval and in W.

   The new function `engine_pot_risk.observe_daily_marked_pnl(conn, snapshot, prior_closes) -> Decimal` combines that statement with the live snapshot in Python. It derives D, `D_start`, P and `P_close` from `snapshot.observed_at`. Every existing function is unchanged.
   - The epoch is the first pool event's `changed_at`, as `observe_pot_nav` reads it. No code path updates or deletes `strategy_paper_pool_events` (grep of `app/` for `UPDATE`/`DELETE` on it, 2026-10-12), so the epoch cannot move inside a window.
6. **Exception contract.** The function raises `EngineCapitalObservationError` with one of the codes above, added to `EngineCapitalRefusal`. `_observe_local_mandate_risk` catches it inside its transaction block and returns the code, as it already does for `advance_pot_drawdown`. The code reaches `_persist_rejection`; the message, which names the position, is logged.
7. **Executor placement (r1 F11, F12, r2 B3).** Inside `_observe_local_mandate_risk`'s transaction:
   1. the realised check, unchanged;
   2. `observe_daily_marked_pnl`, so every witness runs **before** the drawdown index links the observation;
   3. `advance_pot_drawdown(conn, observe_pot_nav(conn, risk))`, unchanged;
   4. the threshold `daily <= −daily_loss_limit` returns `portfolio_daily_marked_loss_limit`.

   - A threshold refusal comes after the drawdown advance, so it never withholds a valid observation.
   - A witness refusal does withhold the executor's advance for that call. The drawdown index is still advanced every tick by `refresh_strategy_health` → `_pot_drawdown_block` (`strategy_paper_runtime.py`), which does not call the new function.
   - A refusal returns from inside the block, as the drawdown refusal does: the reads and any drawdown advance commit, nothing else.
8. **The two limits are independent, with different windows and populations.**
   - The realised check sums every demo close in the New York calendar day. `_MANDATE_OBSERVATION_SQL` has no epoch, ownership or verdict filter, so it includes the operator's closes.
   - The marked check covers E only, from P's close.
   - So the realised one refuses a −2,000 close in the New York day even when the close was at P's close. The marked one refuses a −2,000 mark move with no close. Both are tested.
9. **Frozen preview gates are out of scope.**
   - `ai_trial_start_gate.py` and `ranking_pot_activation.py` are hashed into declarations 16 and 23 (`ai_trial_policy.POLICY_MODULES`, `ranking_pot_policy.POLICY_MODULES`) and keep realised-only.
   - Both import `_MANDATE_OBSERVATION_SQL` from the executor, and the new code changes none of the functions they call. So **`_MANDATE_OBSERVATION_SQL` stays byte-identical**, pinned by a test on its SHA-256.
   - Both programmes are halted or paused (#2437, 10-09). A later declaration adopts the marked check by calling the new function.
10. **Operator labels** in `StrategiesPage.tsx`'s refusal map:
    - `portfolio_daily_marked_loss_limit`, "The mandate's daily loss limit is reached on marked P&L";
    - one line each for `engine_pot_daily_anchor_missing`, `_anchor_incomplete`, `_anchor_moved`, `_reference_missing`, `_reprice_unverified` and `engine_pot_daily_event_unwitnessed`.
11. **Exits.** The check sits only on the entry sizing path (`_risk_and_amount` → `_observe_local_mandate_risk`). Exit and protective paths neither call it nor need the fetched closes. A test asserts that the exit path never calls `observe_daily_marked_pnl`.

## Operational dependency

R exists only if `daily_portfolio_sync` (scheduler, demo) writes at least one accepted snapshot on **every UTC date, trading or not**. A missed date refuses entries for the next market day, which is fail-closed. It surfaces as the job's own failure and as the refusal code on the next entry. Since 2026-09-15 there is no missing date (`scripts/measure_3612_anchor_coverage.py`).

## Tests

- **Pure, over the per-position formula:**
  - held, R mark equal to P close, +100 at R → −400 now gives −500;
  - held, 24/5, R mark above P close by 10 USD of U, unchanged now gives +10;
  - half close at P's close gives 0, and so does a full close at P's close;
  - opened in W, now −400, gives −400;
  - opened and closed in W gives its realised P&L;
  - claimed before R and present in R's marks counts its repriced mark;
  - closed before R and released locally after R contributes 0 and does not refuse;
  - principal change with P&L unchanged gives 0.
- **Reprice:** the identity on a stored-mark fixture reproduces `unrealized_pnl` at `close_rate` to the cent. A short, a non-USD asset currency and non-zero fees each refuse `reprice_unverified`.
- **Witnesses:**
  - an unrecorded partial close refuses `event_unwitnessed`, and so does an unrecorded final close after an earlier recorded partial;
  - a close with NULL P&L refuses;
  - an ownership claimed in W with no open event refuses, as does r2 B2's open-and-close with no events;
  - a child-count mismatch refuses `anchor_incomplete`, and so does a NULL declared count, while a genuine zero-position snapshot passes;
  - a non-USD or undocumented account currency refuses;
  - an E event inside the quiet interval refuses `anchor_moved`, while a non-E event there does not;
  - a missing instrument close, and a bar dated other than P, refuse `reference_missing`;
  - events exactly at `R.observed_at` are excluded and events exactly at `risk.observed_at` are included.
- **Calendar:**
  - D, `D_start`, R date, P and `P_close` for a Monday, for the Tuesday after a Monday holiday, for the day after a half day (13:00 close), across both 2026 DST transitions (03-08, 11-01), and across a year boundary;
  - the quiet-interval bounds: an event exactly at its lower bound is outside it, and one exactly at `D_start` is inside it.
- **Threshold:** at, just below and just above −limit; a non-breach proceeds to sizing.
- **Fetch (pure, provider stubbed):** one call per distinct instrument; a raising provider yields an empty mapping, with entries refused and exits still run.
- **Executor (DB):**
  - the marked check refuses when realised P&L does not breach;
  - the realised check still refuses when the marked one does not;
  - after a marked threshold refusal, the drawdown state has advanced to the current observation;
  - after a witness refusal, it has not;
  - each anchor, reference and witness refusal reaches the entry's recorded reason;
  - an exit with a breached daily loss still executes.
- **Drift:**
  - the `_MANDATE_OBSERVATION_SQL` SHA-256 is pinned;
  - `scripts.ai_trial_policy_guard` reports MATCH for declaration 16;
  - `RANKING_POT_POLICY_HASH` is unchanged before and after.

## Full-population verification

`scripts/measure_3612_anchor_coverage.py` runs over every NYSE session D from 2026-09-16 on. For each it derives R, P and the quiet interval with the same calendar helpers, and prints:
- R's time;
- the declared and stored mark counts;
- the count of demo events in the quiet interval, which covers all demo positions, a superset of E;
- how many R marks equal P's `price_daily` close;
- the USD reprice R → P close.

`price_daily` stands in for the live fetch only because the run is retrospective. Run on 2026-10-12, 18 sessions:
- an R row exists for every session;
- declared equals stored marks for every session;
- the quiet interval is empty for 17 sessions, and holds the 4 non-engine events above on D = 09-23;
- R's time ranges from 16:14 to 19:57 New York;
- the reprice is 0.00 on every Monday, and between −961.55 and +194.48 USD on other days.

These are retained rows, measured retrospectively. They do not prove a row existed at each past decision instant: the upsert can insert a row later. The live gate reads at decision time, and an absent row refuses.

**Unit conservation.** `scripts/measure_3612_unit_conservation.py` covers every demo position in consecutive stored snapshot pairs from 2026-09-15 on, the operator's positions included as well as the engine book:

`units(d1) + Σ open units − Σ close units in (observed_at(d1), observed_at(d2)] = units(d2)`

- It holds for **260 of 260** pairs (measured 2026-10-12).
- `trade_events` held 23 opens and 13 closes, every close with `realized_pnl_usd`.

## Security

No authorisation surface changes. This is an additional fail-closed entry refusal on the demo paper executor. It reads stored broker evidence, the live snapshot it already holds, and eToro's public daily candles through the existing market-data provider.
