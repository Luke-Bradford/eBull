# #3612 slice 2: daily loss on marked P&L, not realised only

Source: #3612 risk lens. *"Daily loss counts realised P&L only. Use realised + Δunrealised from marks."*
The desk skill records the same requirement: `.claude/skills/quant/portfolio-construction-and-risk.md`, "Risk limits that act", *"Daily loss on marked P&L, not realised only."*

## Problem

The paper executor refuses an entry with `portfolio_daily_loss_limit` when today's realised P&L is at or below −(`pool_base` × `max_daily_loss_pct` / 100). The check is in `app/services/strategy_paper_executor.py`, `_observe_local_mandate_risk`. Today's realised P&L is the `realized_pnl_usd` sum of close events in the New York calendar day (`_MANDATE_OBSERVATION_SQL`).

An open book can fall by any amount without a single close, and that check never sees it. Take a fully invested pot on a −5% SPY day with no stop hit: its realised P&L is 0, so new entries keep opening into the fall. The live pool event allows 2.5% (`growth`, `max_daily_loss_pct` 2.5, `capital_limit` 40,000; evidence section 5). The pot would lose about 5% of `pool_base`, twice the limit, with the gate silent.

## Round history

Rounds 1 to 3 anchored the day on `broker_account_equity_snapshots`: the last write of each UTC date, taken anywhere from 16:14 to 19:56 New York on weekdays (evidence section 5).
- Round 2 found that this drops the move between the close and the mark.
- Round 3 repriced the mark to a live-fetched candle close, and that added four BLOCKING findings of its own: re-basing, dating, basis and exit latency.

Round 4 removed external prices. The anchor is the broker's own marks, captured once from the first qualifying sync observation after P's regular close, so the day is a difference of two broker marks. Rounds 5 to 7 keep that design. The closure audit is at the end of this spec.

## Source rule

- **Daily P&L convention (published).** Interactive Brokers, TWS Reference, "Profit and Loss" (<https://www.interactivebrokers.com.hk/php/webhelp/TWS_Reference_Information/pnl_.htm>):
  - Daily P&L: *"(current price - prior day's closing price) x (total number of outstanding shares) plus New Position calculation for all new positions."*
  - New Position: *"(current price - purchase price) x (number of outstanding shares purchased today)."*

  **The reference for a held position is the prior session's close.** For a US listing that is the regular-session close: 16:00 New York, or 13:00 on a `us_market_status` half day.
- **Mark source and time.** Both sides are eToro's per-position `unrealizedPnL` from `TradingRealAdminApi_Position` (sql/383 header), as stored for #3068.
  - `unrealizedPnL.timestamp` is the documented time "when the broker struck the mark" (`BrokerDirectPositionInvestment.pnl_timestamp`, `app/providers/broker.py`).
  - On all 252 stored marks from 2026-09-15 to 2026-10-11, it is present and 0.07 to 4.2 s before its snapshot's `observed_at` (section 5). Each position's mark time, not the snapshot's receipt time, is therefore the cut for that position on both sides (Design 3).
- **Realised and unrealised are on one convention.** eToro's `unrealizedPnL` and the close's `realized_pnl_usd` must agree, or a close at S's mark would show a spurious P&L. Census of every close of a position present in the preceding stored snapshot, event window capped at `--until` (section 5): 5 closes; `realised − U_S × closed/held − (price − close_rate_S) × closed × conversion` is at most 0.0076 USD in absolute value.
  - That is a historical maximum, not a guaranteed bound. One cent per close is 0.000025% of the 40,000 pool (0.01 / 40,000 × 100), against a 2.5% limit.
  - Conditions of the census: all 5 closes have `fees_usd` 0 and a USD asset currency.
  - Coverage: 2 of the 5 also meet this design's full admission predicates, with the anchor's `total_fees` present and 0. The other 3 anchors predate `total_fees` capture. The identity is therefore measured, not proven for every admitted case, and the design adds an independent witness (cost basis, next bullet) rather than relying on it alone.
  - All 252 marks are USD and `is_buy` true.
  - `total_fees` is NULL on the 134 marks dated before 2026-10-01, when its capture began. It is present on all 118 marks since and is 0 on every one.
  - In `trade_events`, all 13 closes carry `fees_usd` 0, and all 23 opens carry NULL: the ingestion records fees on closes only.

  No documented eToro rule says whether `realized_pnl_usd` or `unrealizedPnL` include `totalFees`. The design does not read a zero `totalFees` as "no costs", because a signed sum can net to zero. It requires an unchanged `totalFees` on held positions, a zero `totalFees` on positions closing in the window, and a zero `fees_usd` on every close. The cost basis is witnessed separately.
- **Cost basis conserves (measured).** For each position across consecutive stored snapshots, cut at its own mark times: `amount(d1) + Σ open investment_usd − Σ close investment_usd = amount(d2)`, with 0 where absent (section 4).
  - It holds exactly for every position that was ever engine-owned.
  - It holds for every position-pair except 4, all non-engine round trips on 2026-09-22/23 whose open records an `investment_usd` of 50 against their close's 25.
  - `amount` is cost basis (`BrokerInstrumentInvestment` docstring: `amount / units` reproduces `openRate`) and is unchanged by a split, so this witness sees a missing close that a split would hide from the unit check.
- **Position-level scope.** The figure is the positions' own P&L. A charge the broker books to the account without attributing it to a position is outside it, as it is outside `PotNav`'s realised and unrealised terms. The ledger shows none: all 13 closes carry `fees_usd` 0, and cost basis conserves.
- **`totalFees`** is documented as *"Total overnight fees and dividends charged/paid on the position in USD. Negative amount represents refund"* (`app/providers/broker.py`). That one signed number carries both fees and dividends.
- **Event identity.** `trade_events` allows one open per position (`uq_trade_events_open`) and one close per `(position_id, executed_at)` (`uq_trade_events_close`). A duplicated round trip therefore cannot add a second open, and a duplicated close breaks unit conservation.
- **Principal flows are excluded exactly.** `PotNav` separates principal from P&L (`engine_pot_risk.py`, module docstring, GIPS 2020 2.A.24(e)/(f) as already cited there), and this term uses P&L only.
- **Parent/child completeness** is a documented precondition: the sql/383 header requires the mark count to equal `official_direct_long_positions + official_direct_short_positions`. **Currency** must be documented as USD, the same refusal `observe_pot_nav` applies (`_snapshot_positions`).
- **Limit and denominator are unchanged:** `pool_base × mandate_max_daily_loss_pct / 100` (sql/311 denominates every mandate percentage on effective pot capital).

**Local policy choices** (no published rule found; each is frozen by this spec):

- **D, P and S.**
  - D is `risk.observed_at`'s New York date, and `D_start` is D 00:00 New York. The reset at New York midnight is local policy; the quoted IBKR text gives no reset time.
  - P is `latest_completed_us_session(D_start)`, and `P_close` is P's regular close. Both helpers are in `market_calendar.py`, which is ranking-policy-hashed, so they are called and never edited.
  - S is P's close capture (Design 1). On weekends and holidays P does not move. Saturday through Monday measure from the latest preceding session's S, normally Friday's, so a loss can appear in the figure of every calendar day until the next session's S exists.
- **What is not counted, and when.** Moves between `P_close` and S's mark time are excluded from D's figure. On P itself they are counted by any observation after S, which still measures from the session before P. From S to D's midnight, an observation on P (from the previous S) and on D (from S) both count the move.
  - Measured, every mark taken after its session's close, 2026-09-15 to 2026-10-11 (section 3, a retained-row comparison against `price_daily`, which is back-adjusted, sql/479 header):
    - IEP, NXH, SPY.RTH, SVOL, SVXY and VOO: `close_rate` equals the session's stored close on all 139;
    - QQQ: differs on 15 of 19, and GME on 14 of 18.
  - The excluded interval's length is **unmeasured until S rows exist**. Each S stores its `observed_at` and every child's `pnl_timestamp`, so every day's interval is auditable from then on.
  - Section 1 is scheduling evidence only. A successful sync started after the close on all 19 sessions, at a job-start lag of median 0.0 minutes and maximum 76.8.

## Design

1. **Close capture (schema and writer).**
   - Migration `sql/482_broker_account_session_close_marks.sql` creates `broker_account_session_close_snapshots (environment, session_date, observed_at, currency, account_currency_id, official_direct_long_positions, official_direct_short_positions, PRIMARY KEY (environment, session_date))` and `broker_account_session_close_marks`.
     - The marks table has `broker_account_position_marks`'s per-position columns, keyed `(environment, session_date, position_id)`, with an FK to the parent. `pnl_timestamp` and `total_fees` are NOT NULL.
     - Every numeric column is CHECKed finite: `<> 'NaN'` and not `±Infinity`, both of which PostgreSQL `numeric` admits.
     - Both tables are insert-once: UPDATE is refused by sql/424's `etoro_crowd_append_only` trigger, as in sql/479.
     - `trade_events` gets no new constraint. Its writer is `sync_portfolio`, which runs before any evidence savepoint, so a CHECK failure there would abort the portfolio refresh (r5 finding 5). Its values are validated at read time instead (4).
   - Writer: `account_equity_evidence.record_session_close_snapshot(conn, *, environment, snapshot)`.
     - It derives `P = latest_completed_us_session(snapshot.observed_at)`.
     - It writes only when all of these hold:
       - `snapshot.observed_at`'s New York date is P;
       - `_validate_snapshot` accepts the snapshot;
       - every direct position has a finite `unrealized_pnl` and `units`, a non-NULL `total_fees`, and a `pnl_timestamp` in `[P_close, snapshot.observed_at]`.
     - The parent is `INSERT … ON CONFLICT DO NOTHING RETURNING`, and the children are inserted only when the parent row was inserted. So S is **the first qualifying snapshot successfully inserted**, never replaced, and a later run the same evening changes nothing.
     - A non-qualifying snapshot writes nothing and logs one INFO line naming the failed condition.
   - It is called from `daily_portfolio_sync` (scheduler, demo) after `record_account_equity_snapshot`, in its **own** savepoint, with the same catch-log-and-continue handling the equity snapshot has (`app/workers/scheduler.py`, the `with conn.transaction():` around `record_account_equity_snapshot`).
     - A write that raises is logged at ERROR and leaves the equity snapshot and the sync intact.
     - No run, no fetched snapshot, or no qualifying snapshot simply leaves S absent. That surfaces only as `engine_pot_daily_anchor_missing` on D's entries.
   - `daily_portfolio_sync` ran at least 5 times (median 32) after every session's close in the census (section 1), so a non-qualifying first snapshot is retried, and no new job or connection lane is added.
   - The provider comments on `pnl_timestamp` ("nothing reads it for a verdict") and `total_fees` ("a MEMO … never an operand") are updated to name this consumer. Their optional parsing stays, for consumers that do not need them.
2. **Population E.** E is every eligible trade under the pot epoch, read with `engine_book_rows`' eligibility, and its ownership row's `broker_position_id` where `claimed_at ≤ risk.observed_at`.
   - An eligible trade whose status is open, closing or closed with no ownership row refuses `engine_capital_population_incomplete`, inside the same statement, as `book_membership` does (r5 finding 4).
   - This is the cut `book_membership` applies to a snapshot (r2 B2).
3. **Per-position windows and daily P&L.**
   - Each position is cut at its own mark times. `t_S(p)` is p's `pnl_timestamp` in S if present, else `S.observed_at`. `t_now(p)` is p's live `pnl_timestamp` if present, else `risk.observed_at`.
   - p's window is `W_p = (t_S(p), t_now(p)]`, so units, marks and events agree at both ends (r5 finding 3).

   `daily_p = U_now(p) − U_S(p) + Σ realised(p, closes in W_p)`

   - `U_now(p)` is the live snapshot's `unrealized_pnl` if p is present, else 0.
   - `U_S(p)` is S's stored mark if p is present, else 0.
   - The book's daily P&L is Σ over E.
   - Under the measured identity, a close executed in `W_p` at a price equal to S's `close_rate` contributes 0. For example, closing half: −50 + 50 = 0.
   - A position opened in `W_p` contributes against its purchase price, which is IBKR's "New Position" case.
   - A position closed before S belongs to P's day, whether or not its close was recorded (see 7 on the ledger).
4. **Witnesses.** Each refuses the entry with its own code; none is coerced.
   - **Anchor.**
     - No S row for P gives `engine_pot_daily_anchor_missing`.
     - A currency other than USD gives `engine_capital_snapshot_unusable`, as `_snapshot_positions`.
     - A NULL declared count, or a mark count different from `official_direct_long_positions + official_direct_short_positions`, gives `engine_pot_daily_anchor_incomplete`. A declared 0 with 0 marks passes.
   - **Live mark freshness.** For every p in E present in the live snapshot, `pnl_timestamp` must be non-NULL and in `[risk.observed_at − max_mark_age_seconds, risk.observed_at]`. Otherwise the entry refuses `engine_pot_daily_mark_stale`. The executor passes `intent.max_quote_age_seconds`, its own quote-age bound (`_age_ok`).
   - **Per-case evidence (r5 findings 1 and 2).** Each p in E falls into exactly one case by presence in S and in the live snapshot. Any evidence named below that is missing or not as required gives `engine_pot_daily_accounting_unverified`. A NULL or non-finite event value gives `engine_pot_daily_event_unwitnessed`.

     | case | evidence required |
     |---|---|
     | held: in S and live | both sides `is_buy` and asset currency USD; `total_fees` non-NULL on both sides and unchanged |
     | closed in W: in S, not live | S side `is_buy`, USD, `total_fees` = 0 |
     | opened in W: not in S, live | live side `is_buy`, USD, `total_fees` = 0; its open event is in `W_p` |
     | round trip: in neither, events in `W_p` | its open is in `W_p` with `side` `buy`. It contributes realised only, which is USD by column, so no mark convention enters. |
     | historical: in neither, no events in `W_p` | contributes 0. If its `claimed_at` is in `W_p`, it refuses `engine_pot_daily_event_unwitnessed` (r2 B2's open-and-close with no events). |

     A held position with a partial close, or an opened position with a partial close, also meets the close rules under Events below, which apply in every case.
     - `totalFees` is a running total on the position. A held position with a constant value contributes nothing new to ΔU, so only a change refuses, on the day of the change. The next S carries the new value, and the refusal does not persist (r6 finding 7).
     - A closing position must have `total_fees` = 0, because whether `realized_pnl_usd` nets the running total is undocumented. A position that has ever had a fee or dividend therefore refuses entries on the day it closes. Replacing that with a signed term is #3612 follow-up work, due once one engine-held dividend has been observed.
   - **Events.** For every p in E, over events in `W_p`:
     - every open has non-NULL, finite `units`, `price` and `investment_usd`. Its `fees_usd` and `realized_pnl_usd` are NULL by ledger shape (23 of 23 opens) and are not required;
     - every close has non-NULL, finite `units`, `price`, `investment_usd` and `realized_pnl_usd`, and `fees_usd` = 0 non-NULL;
     - units conserve: `units_S(p) + Σ open units − Σ close units = units_now(p)`;
     - cost basis conserves: `amount_S(p) + Σ open investment_usd − Σ close investment_usd = amount_now(p)`.

     Both conservation checks use 0 where p is absent from a side. A failure gives `engine_pot_daily_event_unwitnessed`.
     - The unit check catches a missing partial or final close in `W_p`. Section 4's census shows that shape is real: position 3602947846 (instrument 3075, never owned, never in a mark) opened 0.646 units on 2026-09-23, recorded a close of 0.323, and has no further event.
     - Event identity is the schema's: one open per position, one close per `(position_id, executed_at)`. A duplicated close breaks conservation.
   - **Numeric.** Every value read is checked `is_finite()` before any arithmetic, with the codes above. The new tables' CHECKs make S finite at rest. The live side is checked by `observe_pot_nav`, which runs first.
   - **Splits.** A split after S changes units at the broker without a trade event, so the unit check refuses D's entries until the next session's S, taken after the split. U is a USD value, so it is not re-based.
     - A split that coincides with a missing close can leave units conserving by accident (r6 finding 4). Cost basis does not move with a split, so the missing close's `investment_usd` breaks the cost-basis check, which refuses.
5. **One read.** One SQL statement reads, under one snapshot even in READ COMMITTED:
   - S's parent and marks;
   - E, eligible trades without ownership included;
   - E's `trade_events` after `P_close`, which every `t_S(p)` is at or after (Design 1's timestamp bound). That is a superset of every `W_p`, which Python then cuts per position.

   The new function `engine_pot_risk.observe_daily_marked_pnl(conn, snapshot, *, max_mark_age_seconds) -> Decimal` combines that statement with the live snapshot in Python. It derives D, `D_start`, P and `P_close` from `snapshot.observed_at`.
   - `observe_pot_nav`, `advance_pot_drawdown`, `engine_book_rows`, `book_membership` and `_MANDATE_OBSERVATION_SQL` keep their implementations.
   - `_price_book` changes in one place (7).
   - The executor function, the scheduler job and the two provider field comments change (1, 7).
   - The daily figure depends only on that statement and the live snapshot, never on `observe_pot_nav`'s separate reads.
   - An event committed after the statement's snapshot is not seen. If it changed units before `t_now(p)`, the unit check refuses.
6. **Exception contract.** The function raises `EngineCapitalObservationError` with one of the codes above, which are added to `EngineCapitalRefusal`. `_observe_local_mandate_risk` catches it inside its transaction block and returns the code, as it already does for `advance_pot_drawdown`. The code reaches `_persist_rejection`; the message, which names the position, is logged.
7. **Executor changes.** In `_observe_local_mandate_risk`:
   1. The realised check's dates come from `risk.observed_at` instead of `now` (r3 finding 8). Its `daily_realised_pnl` must be finite before the comparison, else it refuses `engine_capital_population_incomplete`. `_MANDATE_OBSERVATION_SQL`'s text is unchanged.
      - It still sums the whole New York calendar day as visible when it runs, including a close executed after `risk.observed_at`. That is the frozen preview gates' behaviour, kept because they share the SQL (r4 finding 14).
      - `risk.observed_at` is at most `max_quote_age_seconds` older than `now` (`_age_ok` in `_risk_and_amount`).
   2. `advance_pot_drawdown(conn, observe_pot_nav(conn, risk))`, with one change in `_price_book` (r6 finding 1).
      - Each position's realised aggregate is checked `is_finite()` before it is added, and a non-finite one raises `EngineCapitalObservationError(…, "engine_capital_population_incomplete")`.
      - Today two historical aggregates of opposite infinite sign would raise `decimal.InvalidOperation` during the sum. That would escape every handler and abort the paper cycle, exits included (the comment in `_risk_and_amount`).
      - Neither policy module list (`ai_trial_policy.POLICY_MODULES`, 10 modules; `ranking_pot_policy.POLICY_MODULES`, 28) contains `engine_pot_risk`, so no declaration hash moves.
   3. `observe_daily_marked_pnl`, then the threshold `daily <= −daily_loss_limit`, which returns `portfolio_daily_marked_loss_limit`. A marked-limit breach and a marked-observation refusal both come after the drawdown advance. The realised refusal keeps its earlier return.

   **Out of scope, recorded on #3612 as a remaining slice (r2 B3, r3 findings 4 and 5, r4 finding 6).** The drawdown index can link a NAV built on an incomplete ledger. `_price_book` accepts a released position with any close history, so a recorded partial followed by an unrecorded final close passes. `refresh_strategy_health` → `_pot_drawdown_block` advances the index every tick without anything this slice adds. The fix belongs in `observe_pot_nav`, covering both callers: a unit-conservation witness read in the same statement that prices the NAV. This slice neither relies on that nor changes it.
8. **The two limits are independent, with different windows and populations.**
   - The realised check sums every demo close in the New York calendar day. `_MANDATE_OBSERVATION_SQL` has no epoch, ownership or verdict filter, so it includes the operator's closes.
   - The marked check covers E only, from S.
   - So the realised one refuses a −2,000 close in the New York day even when the close was at S's mark. The marked one refuses a −2,000 mark move with no close. Both are tested.
9. **Frozen preview gates are out of scope.**
   - `ai_trial_start_gate.py` and `ranking_pot_activation.py` are hashed into declarations 16 and 23 (`ai_trial_policy.POLICY_MODULES`, `ranking_pot_policy.POLICY_MODULES`) and keep realised-only.
   - Both import `_MANDATE_OBSERVATION_SQL`, so **it stays byte-identical**, pinned by a test on its SHA-256.
   - Both programmes are halted or paused (#2437, 10-09).
10. **Operator labels** in `StrategiesPage.tsx`'s refusal map:
    - `portfolio_daily_marked_loss_limit`, "The mandate's daily loss limit is reached on marked P&L";
    - one line each for `engine_pot_daily_anchor_missing`, `_anchor_incomplete`, `_accounting_unverified`, `_mark_stale` (a position's broker mark is missing, in the future, or older than the quote-age bound) and `engine_pot_daily_event_unwitnessed`.
11. **Exits.**
    - The check sits only on the entry sizing path (`_risk_and_amount` → `_observe_local_mandate_risk`), and every refusal it adds is a returned code, never a raise.
    - The capture writer runs in `daily_portfolio_sync`'s own savepoint, after `sync_portfolio`, and the paper cycle does not wait on it.
    - Nothing in this slice adds a constraint to the ingestion path that records exits.

## Operational dependency

S exists only if a qualifying snapshot is inserted on P's New York date after `P_close`. A miss refuses entries for D, which is fail-closed, and appears as `engine_pot_daily_anchor_missing` in the entry's recorded reason. A raising write is also an ERROR in the sync log. A successful sync run started after the close existed for 19 of 19 sessions since 2026-09-15 (section 1). That counts runs, not captures; capture coverage is measured from S rows after deployment.

## Tests

- **Writer (DB):**
  - nothing is written for an observation before `P_close`, on a closed day, on the New York date after P, or with any position's `pnl_timestamp` NULL, before `P_close` or after `observed_at`, or `total_fees` NULL;
  - the first qualifying observation writes the parent and children;
  - a later one the same evening changes nothing, children included;
  - UPDATE is refused, and NaN or Infinity is refused in each numeric column;
  - a half day uses 13:00;
  - a capture failure leaves the equity snapshot stored and the sync successful.
- **Pure, over the per-position formula:**
  - held, +100 at S → −400 now gives −500;
  - a half close and a full close, executed in W at S's `close_rate`, each give 0;
  - opened in W, now −400, gives −400;
  - opened and closed in W gives its realised P&L;
  - claimed before S and present in S counts its S mark;
  - present in S with `claimed_at` just after S and its open before S is accepted;
  - closed before S and released locally after S contributes 0 and does not refuse;
  - principal change with P&L unchanged gives 0.
- **Witnesses:**
  - an unrecorded partial close in W refuses `event_unwitnessed`, and so does an unrecorded final close after a recorded partial (the 3075 shape);
  - a close with NULL P&L refuses;
  - absent from S, claimed in W with no open in W refuses;
  - a units change with no event (a split) refuses;
  - a child-count mismatch refuses `anchor_incomplete`, and so does a NULL declared count, while a genuine zero-position S passes;
  - a non-USD or undocumented account currency refuses;
  - for each case in the per-case table, each missing or wrong piece of evidence refuses with its stated code: a short, a non-USD asset, `total_fees` NULL or non-zero, a close with `fees_usd` NULL or non-zero, and a round-trip open with `side` `sell`;
  - a historical position with no events and `claimed_at` before `W_p` contributes 0 without refusing;
  - an eligible trade with no ownership refuses `engine_capital_population_incomplete`;
  - a live `pnl_timestamp` that is NULL, older than `max_quote_age_seconds` or later than `risk.observed_at` refuses `mark_stale`;
  - per-position cut: an event exactly at `t_S(p)` is excluded, and one exactly at `t_now(p)` is included;
  - a trade between a position's `pnl_timestamp` and the snapshot's `observed_at` is placed on the correct side and conserves;
  - NaN or Infinity in an event's units or realised value refuses `event_unwitnessed` rather than raising;
  - an ordinary open with NULL `fees_usd` and `realized_pnl_usd` is accepted;
  - a partial close of a held position, and of a position opened in W, with `fees_usd` NULL or non-zero refuses;
  - a 2:1 split with a missing close of half the post-split units, where units conserve by accident, refuses on cost basis;
  - consecutive S rows carrying the same non-zero `total_fees` on a held position do not refuse, and a change between S and now does;
  - S accepted with a mark 10 minutes after `P_close` and an event between that mark and `S.observed_at` is read (the statement's range starts at `P_close`).
- **Calendar:**
  - D, `D_start`, P and `P_close` for a Monday, for the Tuesday after a Monday holiday, for the day after a half day, across both 2026 DST transitions (03-08, 11-01), and across a year boundary;
  - `risk.observed_at` one microsecond either side of Friday → Saturday midnight selects different P;
  - either side of Saturday → Sunday midnight selects the same P;
  - the Monday after a Friday closure (2026-04-03, Good Friday) uses Thursday's S.
- **Threshold:** at, just below and just above −limit; a non-breach proceeds to sizing.
- **Executor (DB):**
  - the marked check refuses when realised P&L does not breach;
  - the realised check still refuses when the marked one does not;
  - a non-finite realised total refuses rather than raising;
  - after a marked refusal of either kind, the drawdown state has advanced to the current observation;
  - two historical closes with realised `+Infinity` and `−Infinity` make the entry refuse `engine_capital_population_incomplete`, and a later exit in the same cycle executes;
  - each new refusal reaches the entry's recorded reason;
  - in one cycle, an entry refused by each new code is followed by an exit that executes.
- **Drift:**
  - the `_MANDATE_OBSERVATION_SQL` SHA-256 is pinned;
  - `scripts.ai_trial_policy_guard` reports MATCH for declaration 16;
  - `RANKING_POT_POLICY_HASH` is unchanged before and after.

## Full-population verification

Every figure in this spec comes from `PYTHONPATH=. uv run python scripts/measure_3612_marked_daily_loss_evidence.py --since 2026-09-15 --until 2026-10-11`, run 2026-10-12. 2026-09-15 is the first stored position mark (section 5).

1. **Job-start lag after the close.**
   - 19 NYSE sessions, each with a successful `daily_portfolio_sync` started after the close on its New York date.
   - Median 0.0 minutes, maximum 76.8 (2026-09-25), over 15 minutes on 3.
   - Successful runs per session after the close: minimum 5, median 32.
2. **Snapshot continuity, every UTC date.**
   - 27 of 27 dates have a snapshot.
   - None has a NULL declared count or a count different from its stored marks.
3. **Retained-row price comparison.**
   - 176 marks taken after their session's close; all 176 compared, none missing a bar or instrument metadata.
   - The per-instrument figures are in the Source rule section.
4. **Unit and cost-basis conservation, 26 consecutive snapshot pairs, each position cut at its own mark times.** The population is:
   - endpoint marks;
   - positions with an event in the window;
   - engine ownership claimed in the window;
   - every engine ownership row claimed by the later endpoint (`owned_by_o2`). That last group is a superset of E, so the census covers E, older and released claims included.

   Results:
   - Position-pairs by group: 174 endpoint only, 6 endpoint + event, 1 endpoint + event + owned_by_o2, 6 endpoint + event + owned_by_o2 + ownership, 63 endpoint + owned_by_o2, 7 event only, 18 owned_by_o2 only.
   - 0 events fall between a position's mark time and its snapshot's receipt time.
   - Units: one failure, position 3602947846. Its full history in section 5 shows two events and no ownership row or mark at any time.
   - Cost basis: 4 failures, the non-engine round trips of 2026-09-22/23 (open `investment_usd` 50, close 25), 3602947846 among them.
   - Every pair that includes an owned position conserves both.
5. **Point claims:**
   - the pool limits;
   - weekday after-close snapshot times, 16:14:37 to 19:56:03 New York;
   - the first stored mark date;
   - the failing position's history;
   - the 5-close identity, max |residual| 0.0076, each with its event window;
   - mark timestamps on 252 of 252 marks, 0.07 to 4.2 s old;
   - `total_fees`: 118 zero, 0 non-zero and 134 NULL, all NULLs dated before 2026-10-01;
   - asset currency: 252 USD;
   - `is_buy`: 252 true;
   - `trade_events`: 36 rows, 0 non-finite, `fees_usd` NULL on 23 of 23 opens and 0 on 13 of 13 closes, and 0 closes with NULL realised.

The census is retrospective over retained rows. It does not show that a row existed at each past decision instant. The live gate reads at decision time, and an absent S refuses.

## Security

No authorisation surface changes. This adds:
- a fail-closed entry refusal on the demo paper executor;
- an append-only evidence capture, written by an existing demo job from a snapshot that job already fetches.

## Closure audit, rounds 4 to 6

| round.finding | severity | disposition |
|---|---|---|
| r4.1 numeric before exits | BLOCKING | Resolved: finite checks before every comparison or sum (Design 4, 7.1); no new constraint on the exit-recording ingestion path (Design 1) |
| r4.2 accounting identity | BLOCKING | Resolved by measurement plus scope: 5-close identity; only long, USD, all-fees-zero cases admitted (Design 4 table) |
| r4.3 mark time vs receipt | BLOCKING | Resolved: per-position `pnl_timestamp` cuts (Design 3); S window and live freshness (Design 1, 4) |
| r4.4 residual wording | WARNING | Resolved (Source rule, "What is not counted") |
| r4.5 reset and carry-forward | WARNING | Resolved: midnight reset labelled local policy; weekend and holiday carry-forward stated and tested |
| r4.6 drawdown ledger integrity | WARNING | **Open, deferred**: out of scope (Design 7), recorded on #3612 as a remaining slice |
| r4.7 sync success ≠ capture | WARNING | Resolved: counts relabelled; capture failure handling specified (Design 1) |
| r4.8 job-start lag | WARNING | Resolved: scheduling evidence only; interval unmeasured until S exists |
| r4.9 back-adjusted comparison | WARNING | Resolved: relabelled retained-row comparison; not a safety input |
| r4.10 silent mark exclusion | WARNING | Resolved: left joins; counts reconcile (section 3) |
| r4.11 unreproduced claims | WARNING | Resolved: section 5; cadence and 24/5 wording removed |
| r4.12 event identity | WARNING | Resolved: unique indexes cited |
| r4.13 claimed-in-W in S | WARNING | Resolved: per-case table |
| r4.14 realised cutoff | WARNING | Retained, documented (Design 7.1): frozen shared SQL |
| r4.15 first observation vs first insert | NIT | Resolved: "first qualifying snapshot successfully inserted" (Design 1) |
| r4.16 drawdown-advance wording | NIT | Resolved: realised refusal vs marked refusals distinguished (Design 7.3) |
| r4.17 "captured at P's close" | NIT | Resolved: "captured once from the first qualifying sync observation after P's regular close" (Round history) |
| r4.18 "every function unchanged" | NIT | Resolved: unchanged helpers named, changed ones listed (Design 5) |
| r5.1 fee scope | BLOCKING | Resolved: non-NULL zero fees required on every present side and every close |
| r5.2 per-case witnesses | BLOCKING | Resolved: case table (Design 4) |
| r5.3 mark freshness and cut | BLOCKING | Resolved: per-position windows; S timestamp bounds; live age bound |
| r5.4 ownership-less trades | WARNING | Resolved (Design 2) |
| r5.5 CHECK on ingestion path | WARNING | Resolved: `trade_events` CHECKs dropped; read-time validation |
| r5.6 = r4.6 | WARNING | Open, deferred |
| r5.7 NULL fees miscounted | WARNING | Resolved: 134 NULL found and explained (section 5) |
| r5.8 unreproduced claims | WARNING | Resolved: removed or added to section 5 |
| r5.9 unbounded census window | WARNING | Resolved: capped at `--until` |
| r5.10 census vs E | WARNING | Resolved: `owned_by_o2` superset group |
| r5.11 lag as expected length | WARNING | Resolved: wording |
| r5.12 missing-anchor ERROR | WARNING | Resolved: ERROR only for a raising write |
| r5.13 provider comments | NIT | Resolved (Design 1) |
| r5.14 closure audit | NIT | This table |
| r5.15 Friday holiday | NIT | Resolved, with a test |
| r6.1 infinities in `_price_book` | BLOCKING | Resolved: finite check per aggregate (Design 7.2), cycle test |
| r6.2 partial-close fees | BLOCKING | Resolved: close rules apply in every case (Design 4 Events) |
| r6.3 zero `totalFees` ≠ no costs | BLOCKING | Resolved: no longer read as absence of cost. Cost basis witnessed independently; position-level scope stated (Source rule) |
| r6.4 split hides missing close | BLOCKING | Resolved: cost-basis conservation (Design 4), measured (section 4), tested |
| r6.5 event range | WARNING | Resolved: statement reads from `P_close` |
| r6.6 identity coverage | WARNING | Resolved: 2 of 5 under full predicates stated; independent witness added |
| r6.7 dividend lifetime refusal | WARNING | Resolved: held requires unchanged, not zero; closing requires zero, stated as the scope decision |
| r6.8 census windows | WARNING | Resolved: per-position mark-time windows in section 4 |
| r6.9 freshness input | WARNING | Resolved: `max_mark_age_seconds` keyword |
| r6.10 percentage | WARNING | Resolved: 0.000025% with its arithmetic |
| r6.11 cadence | WARNING | Resolved: runs per session measured (section 1) |
| r6.12 NULL rules by event kind | NIT | Resolved (Design 4 Events), with a positive open test |
| r6.13 stale-mark label | NIT | Resolved (Design 10) |
| r6.14 itemised r4.15–18 | NIT | Resolved (rows above) |
