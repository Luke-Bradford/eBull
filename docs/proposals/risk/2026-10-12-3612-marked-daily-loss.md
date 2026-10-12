# #3612 slice 2: daily loss on marked P&L, not realised only

Source: #3612 risk lens. *"Daily loss counts realised P&L only. Use realised + Δunrealised from marks."*
The desk skill records the same requirement: `.claude/skills/quant/portfolio-construction-and-risk.md`, "Risk limits that act", *"Daily loss on marked P&L, not realised only."*

## Problem

The paper executor refuses an entry with `portfolio_daily_loss_limit` when today's realised P&L is at or below −(`pool_base` × `max_daily_loss_pct` / 100). The check is in `app/services/strategy_paper_executor.py`, `_observe_local_mandate_risk`. Today's realised P&L is the `realized_pnl_usd` sum of close events in the New York calendar day (`_MANDATE_OBSERVATION_SQL`).

An open book can fall by any amount without a single close, and that check never sees it. Take a fully invested pot on a −5% SPY day with no stop hit: its realised P&L is 0, so new entries keep opening into the fall. The live pool event allows 2.5% (`growth`, `max_daily_loss_pct` = 2.5, capital 40,000; `SELECT risk_profile, max_daily_loss_pct, capital_limit FROM strategy_paper_pool_events ORDER BY strategy_paper_pool_event_id DESC LIMIT 1`, 2026-10-12). The pot would lose about 5% of `pool_base`, twice the limit, with the gate silent.

## Round 4: what changed and why

Rounds 1 to 3 anchored the day on `broker_account_equity_snapshots`: the last write of each UTC date, taken anywhere from 16:14 to 19:57 New York. Round 2 showed that a mark taken after the close drops the move between the close and that mark. Round 3 repriced the mark to a live-fetched candle close. That fix added four BLOCKING findings of its own:
- a split after a full close re-bases the fetched price;
- the fetched price is not dated to the observer's session;
- the price basis was inferred from samples;
- a pre-cycle fetch delays exits.

Each of these comes from pricing a stored position with a price from somewhere else. This round removes that: **the anchor is the broker's own marks, captured once at P's close**, so the day is a difference of two broker marks and no external price enters.

## Source rule

- **Daily P&L convention (published).** Interactive Brokers, TWS Reference, "Profit and Loss" (<https://www.interactivebrokers.com.hk/php/webhelp/TWS_Reference_Information/pnl_.htm>):
  - Daily P&L: *"(current price - prior day's closing price) x (total number of outstanding shares) plus New Position calculation for all new positions."*
  - New Position: *"(current price - purchase price) x (number of outstanding shares purchased today)."*

  **The reference for a held position is the prior session's close.** For a US listing that is the regular-session close: 16:00 New York, or 13:00 on a `us_market_status` half day.
- **Mark source.** Both sides are eToro's per-position `unrealizedPnL` from `TradingRealAdminApi_Position` (sql/383 header), as stored for #3068:
  - **now** is the executor's live `BrokerAccountRiskSnapshot`, at `risk.observed_at`;
  - **the reference** is the close capture S (Design 1).

  A difference of two broker marks needs no price basis, adjustment or reprice assumption.
- **Principal flows are excluded exactly.** `PotNav` separates principal from P&L (`engine_pot_risk.py`, module docstring, GIPS 2020 2.A.24(e)/(f) as already cited there), and this term uses P&L only.
- **Parent/child completeness** is a documented precondition: the sql/383 header requires the mark count to equal `official_direct_long_positions + official_direct_short_positions`. **Currency** must be documented as USD, the same refusal `observe_pot_nav` applies (`_snapshot_positions`).
- **Limit and denominator are unchanged:** `pool_base × mandate_max_daily_loss_pct / 100` (sql/311 denominates every mandate percentage on effective pot capital).

**Local policy choices** (no published rule found; each is frozen by this spec):

- **D, P and the capture instant.**
  - D is `risk.observed_at`'s New York date, and `D_start` is D 00:00 New York.
  - P is `latest_completed_us_session(D_start)`, and `P_close` is P's regular close. Both helpers are in `market_calendar.py`, which is ranking-policy-hashed, so they are called and never edited.
  - S, P's close capture, is the first accepted broker snapshot observed at or after `P_close` and on P's New York date.
- **The residual is the gap from the close to the capture.** A position's move between `P_close` and S falls in neither day. That move is zero for a name with no trading after the close. Measured census over every stored mark taken after a session's close, 2026-09-15 to 2026-10-11 (evidence script, section 3):
  - IEP, NXH, SPY.RTH, SVOL, SVXY and VOO: `close_rate` equals the session's daily close on all 139 such marks;
  - QQQ: differs on 15 of 19, and GME on 14 of 18. Both are 24/5 names.

  The gap is the sync's lag after the close: median 0.0 minutes, maximum 76.8 (2026-09-25), over 15 minutes on 3 of 19 sessions (section 1). This is bounded by the capture's own `observed_at`, which is stored, so each day's residual window is auditable.
- **Windows overlap after the close, as IBKR's do.** IBKR resets its daily P&L near midnight, while the reference is the prior close. An observation on D after D's close still measures from S_P, and D + 1 measures from S_D, so the interval from S_D to D's midnight is in both days. This is the published convention, chosen on purpose. It counts a loss twice rather than never.

## Design

1. **Close capture (schema and writer).**
   - Migration `sql/482_broker_account_session_close_marks.sql` creates `broker_account_session_close_snapshots (environment, session_date, observed_at, currency, account_currency_id, official_direct_long_positions, official_direct_short_positions, PRIMARY KEY (environment, session_date))` and `broker_account_session_close_marks`. The marks table has the same per-position columns as `broker_account_position_marks`, keyed `(environment, session_date, position_id)`, with an FK to the parent.
   - Both tables are insert-once. UPDATE is refused by sql/424's `etoro_crowd_append_only` trigger, as in sql/479.
   - Writer: `account_equity_evidence.record_session_close_snapshot(conn, *, environment, snapshot)`.
     - It derives `P = latest_completed_us_session(observed_at)`.
     - It writes only when `observed_at ≥ P_close` and `observed_at`'s New York date is P.
     - The parent is `INSERT … ON CONFLICT DO NOTHING RETURNING`, and the children are inserted only when the parent row was inserted, in the same transaction.
     - The first qualifying observation therefore wins and is never replaced. A later run of the same evening changes nothing.
   - It reuses `_validate_snapshot` and `summarise_direct_positions` unchanged.
   - It is called from `daily_portfolio_sync` (scheduler, demo) next to `record_account_equity_snapshot`, in the same transaction. That job runs every 5 to 10 minutes, so no new job or connection lane is added.
   - If no qualifying run happens on P, there is no S, and D's entries refuse.
2. **Population E.** E is every `broker_position_id` with an ownership row on an eligible trade under the pot epoch (`engine_book_rows`) whose `claimed_at ≤ risk.observed_at`. This is the cut `book_membership` applies to a snapshot (r2 B2).
3. **Per-position daily P&L.** With W = `(S.observed_at, risk.observed_at]`, for each p in E:

   `daily_p = U_now(p) − U_S(p) + Σ realised(p, closes in W)`

   - `U_now(p)` is the live snapshot's `unrealized_pnl` if p is present, else 0.
   - `U_S(p)` is S's stored mark if p is present, else 0.
   - The book's daily P&L is Σ over E.
   - A close in W whose execution price equals S's mark contributes 0. For example, closing half: −50 + 50 = 0.
   - A position opened in W contributes its unrealised P&L against purchase price, which is IBKR's "New Position" case.
   - A position closed before S is P's business, whether or not its close was recorded. An unrecorded close is a ledger-completeness defect for `observe_pot_nav`'s contract (see 7).
4. **Witnesses.** Each refuses the entry with its own code; none is coerced.
   - **Anchor.**
     - No S row for P gives `engine_pot_daily_anchor_missing`.
     - A currency other than USD gives `engine_capital_snapshot_unusable`, as `_snapshot_positions`.
     - A NULL declared count, or a mark count different from `official_direct_long_positions + official_direct_short_positions`, gives `engine_pot_daily_anchor_incomplete`. A NULL is unproven completeness, so it takes the completeness code. A declared 0 with 0 marks passes.
   - **Events.** For every p in E:
     - `units_S(p) + Σ open units − Σ close units (events in W) = units_now(p)`, with units 0 where p is absent from a side;
     - every close in W has a non-NULL `realized_pnl_usd`;
     - if `claimed_at` is in W, at least one open event for p is in W.

     A failure gives `engine_pot_daily_event_unwitnessed`.
     - The third check catches an open-and-close inside W with no events (r2 B2).
     - The first catches a missing final close inside W. That case is real: section 4's census finds one, a non-engine position on instrument 3075 that opened 0.646 units on 2026-09-23, recorded a close of 0.323 and left the account with no further event.
   - **Numeric validity.** Any NaN or infinite `unrealized_pnl`, `units` or `realized_pnl_usd` read for E gives `engine_pot_daily_anchor_incomplete` for S's values or `engine_pot_daily_event_unwitnessed` for events. A non-finite live value is already refused by `observe_pot_nav`. Every value is checked before any arithmetic, so no `decimal` exception can escape.
   - **Splits.** A split after S changes units at the broker without a trade event, so the unit check refuses D's entries. U is a value in USD, so it is split-invariant. The refusal is fail-closed and lasts one day: the next S is post-split.
5. **One read.** One SQL statement reads, under one snapshot even in READ COMMITTED:
   - S's parent row and its marks;
   - E, with `claimed_at`;
   - E's `trade_events` in W.

   The new function `engine_pot_risk.observe_daily_marked_pnl(conn, snapshot) -> Decimal` combines that statement with the live snapshot in Python. It derives D, `D_start`, P and `P_close` from `snapshot.observed_at`. Every existing function is unchanged.
   - The daily figure depends only on this statement and the live snapshot, never on `observe_pot_nav`'s separate reads.
   - An event recorded concurrently is either visible to the statement or not. If it is invisible and changed units, the unit check refuses. If it is visible, it is counted.
   - The epoch is the first pool event's `changed_at`, as `observe_pot_nav` reads it. No code path updates or deletes `strategy_paper_pool_events` (grep of `app/` for `UPDATE`/`DELETE` on it, 2026-10-12).
6. **Exception contract.** The function raises `EngineCapitalObservationError` with one of the codes above, which are added to `EngineCapitalRefusal`. `_observe_local_mandate_risk` catches it inside its transaction block and returns the code, as it already does for `advance_pot_drawdown`. The code reaches `_persist_rejection`; the message, which names the position, is logged.
7. **Executor placement.** Inside `_observe_local_mandate_risk`'s transaction, the order is:
   1. the realised check, unchanged;
   2. `advance_pot_drawdown(conn, observe_pot_nav(conn, risk))`, unchanged;
   3. `observe_daily_marked_pnl`, then the threshold `daily <= −daily_loss_limit` returning `portfolio_daily_marked_loss_limit`.

   - A daily refusal of either kind comes after the drawdown advance, so it never withholds an observation the drawdown contract accepts.
   - **On r2 B3 and r3 finding 4.** The concern is that the drawdown index can link a NAV built on an incomplete ledger. That is real, and it belongs to `observe_pot_nav`, not to this check. `refresh_strategy_health` → `_pot_drawdown_block` (`strategy_paper_runtime.py`) advances the index every tick without calling anything this slice adds. So no ordering inside the executor can protect the index, and r3 placed the witnesses before the advance for no protective effect. The gap is that `observe_pot_nav` has no unit-conservation witness. It is recorded on the PR as pre-existing, outside this slice.
   - The realised check's dates come from `risk.observed_at` instead of `now`, so both checks share one clock (r3 finding 8). The SQL text and its parameters' meaning are unchanged; only the instant they are derived from moves. `risk.observed_at` is at most `max_quote_age_seconds` older than `now` (`_age_ok` above it).
8. **The two limits are independent, with different windows and populations.**
   - The realised check sums every demo close in the New York calendar day. `_MANDATE_OBSERVATION_SQL` has no epoch, ownership or verdict filter, so it includes the operator's closes.
   - The marked check covers E only, from S.
   - So the realised one refuses a −2,000 close in the New York day even when the close was at S's mark. The marked one refuses a −2,000 mark move with no close. Both are tested.
9. **Frozen preview gates are out of scope.**
   - `ai_trial_start_gate.py` and `ranking_pot_activation.py` are hashed into declarations 16 and 23 (`ai_trial_policy.POLICY_MODULES`, `ranking_pot_policy.POLICY_MODULES`) and keep realised-only.
   - Both import `_MANDATE_OBSERVATION_SQL` from the executor, so **it stays byte-identical**, pinned by a test on its SHA-256.
   - Both programmes are halted or paused (#2437, 10-09).
10. **Operator labels** in `StrategiesPage.tsx`'s refusal map:
    - `portfolio_daily_marked_loss_limit`, "The mandate's daily loss limit is reached on marked P&L";
    - one line each for `engine_pot_daily_anchor_missing`, `_anchor_incomplete` and `engine_pot_daily_event_unwitnessed`.
11. **Exits.** The check sits only on the entry sizing path (`_risk_and_amount` → `_observe_local_mandate_risk`). Exit and protective paths do not call it. A test asserts that the exit path never calls `observe_daily_marked_pnl`. The capture writer runs inside `daily_portfolio_sync`, and a failure there is that job's failure, as today. The paper cycle does not wait on it.

## Operational dependency

S exists only if `daily_portfolio_sync` succeeds at least once on P's New York date after `P_close`. A miss refuses entries for D, which is fail-closed. It surfaces as the job's own failure and as the refusal code on the next entry. 19 of 19 sessions since 2026-09-15 had such a run (section 1).

## Tests

- **Writer (DB):**
  - an observation before `P_close`, on a closed day, or on the New York date after P writes nothing;
  - the first observation at or after `P_close` writes the parent and children;
  - a later one the same evening changes nothing, children included;
  - UPDATE is refused;
  - a half day uses 13:00;
  - the writer and `record_account_equity_snapshot` commit or roll back together.
- **Pure, over the per-position formula:**
  - held, +100 at S → −400 now gives −500;
  - a half close and a full close in W at S's mark each give 0, the close executed after S;
  - opened in W, now −400, gives −400;
  - opened and closed in W gives its realised P&L;
  - claimed before S and present in S counts its S mark;
  - closed before S and released locally after S contributes 0 and does not refuse;
  - principal change with P&L unchanged gives 0.
- **Witnesses:**
  - an unrecorded partial close in W refuses `event_unwitnessed`, and so does an unrecorded final close after a recorded partial (the 3075 shape);
  - a close with NULL P&L refuses;
  - an ownership claimed in W with no open event refuses, as does an open-and-close in W with no events;
  - a units change with no event (a split) refuses;
  - a child-count mismatch refuses `anchor_incomplete`, and so does a NULL declared count or a NaN mark, while a genuine zero-position S passes;
  - a non-USD or undocumented currency refuses;
  - events exactly at `S.observed_at` are excluded, and events exactly at `risk.observed_at` are included.
- **Calendar:**
  - D, `D_start`, P and `P_close` for a Monday, for the Tuesday after a Monday holiday, for the day after a half day, across both 2026 DST transitions (03-08, 11-01), and across a year boundary;
  - `risk.observed_at` one microsecond either side of New York midnight selects different P.
- **Threshold:** at, just below and just above −limit; a non-breach proceeds to sizing.
- **Executor (DB):**
  - the marked check refuses when realised P&L does not breach;
  - the realised check still refuses when the marked one does not;
  - after a marked refusal of either kind, the drawdown state has advanced to the current observation;
  - each anchor and witness refusal reaches the entry's recorded reason;
  - an exit with a breached daily loss still executes.
- **Drift:**
  - the `_MANDATE_OBSERVATION_SQL` SHA-256 is pinned;
  - `scripts.ai_trial_policy_guard` reports MATCH for declaration 16;
  - `RANKING_POT_POLICY_HASH` is unchanged before and after.

## Full-population verification

The figures come from `PYTHONPATH=. uv run python scripts/measure_3612_marked_daily_loss_evidence.py --since 2026-09-15 --until 2026-10-11`, run 2026-10-12. 2026-09-15 is the first stored position mark (#3068).

1. **Close-capture lag.**
   - 19 NYSE sessions, every one with a successful `daily_portfolio_sync` after the close on its New York date.
   - Lag: median 0.0 minutes, maximum 76.8 (2026-09-25), over 15 minutes on 3 sessions.
2. **Snapshot continuity, every UTC date.**
   - 27 of 27 dates have a snapshot.
   - No row has a NULL declared count or a count different from its stored marks.
3. **Movement after the close, every mark taken after its session's close.** The full census is in the Source rule section. Only QQQ (15 of 19) and GME (14 of 18), both 24/5 names, ever differ. No mark lacked a daily bar.
4. **Unit conservation, 26 consecutive snapshot pairs.** The population is endpoint marks plus event positions plus engine ownership claimed in the window, so intra-window round trips are included.
   - Position-pairs by group: 237 endpoint only, 7 endpoint + event, 6 endpoint + event + ownership, 7 event only.
   - One failure: the 3075 position above, which has no engine ownership row.

The census is retrospective over retained rows. It does not show that a row existed at each past decision instant. The live gate reads at decision time, and an absent S refuses.

## Security

No authorisation surface changes. This adds a fail-closed entry refusal to the demo paper executor, and an append-only evidence capture written by an existing demo job from a snapshot it already fetches.
