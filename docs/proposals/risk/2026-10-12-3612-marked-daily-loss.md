# #3612 slice 2: daily loss on marked P&L, not realised only

Source: #3612 risk lens. *"Daily loss counts realised P&L only. Use realised + Δunrealised from marks."*
The desk skill records the same requirement: `.claude/skills/quant/portfolio-construction-and-risk.md`, "Risk limits that act", *"Daily loss on marked P&L, not realised only."*

## Problem

The paper executor refuses an entry with `portfolio_daily_loss_limit` when today's realised P&L is at or below −(`pool_base` × `max_daily_loss_pct` / 100). The check is in `app/services/strategy_paper_executor.py`, `_observe_local_mandate_risk`. Today's realised P&L is the `realized_pnl_usd` sum of close events in the New York calendar day (`_MANDATE_OBSERVATION_SQL`).

An open book can fall by any amount without a single close, and that check never sees it. Take a fully invested pot on a −5% SPY day with no stop hit: its realised P&L is 0, so new entries keep opening into the fall. The live pool event allows 2.5% (`growth`, `max_daily_loss_pct` 2.5, `capital_limit` 40,000; evidence section 5). The pot would lose about 5% of `pool_base`, twice the limit, with the gate silent.

## Scope of this slice (v1), and why

This slice closes exactly that gap: **the marked move of positions that have not closed since the prior close.**

- Combining a close's realised P&L with marks needs eToro's realised and unrealised conventions to agree, fees and collateral included. Checkpoint 1 rounds 4 to 7 showed that no eToro document states that rule. Each witness added to stand in for it brought new surface: fees as a signed running total, and `amount` including collateral (`app/providers/broker.py`).
- So v1 does not combine them. **Any engine close since the anchor refuses further entries for the rest of the day** (`engine_pot_daily_close_in_window`). That is fail-closed. The realised check still covers the closes themselves.
- **When entries resume.** The refusal lasts until the first New York date whose anchor S was captured after the close and itself passes the endpoint, reconciliation and straddle checks. An immutable S that straddles a close keeps refusing until a later session's S. A close during D's session resumes on D + 1. A close after S on a Friday, or on the day before a holiday, keeps refusing through the weekend or holiday, until the next session's S.
- **How often it binds.** In the census, 2026-09-15 to 2026-10-11, there was 1 engine-owned close, on 2026-09-23 (section 5 prints its position and time). Under this rule it would have refused entries for the rest of that day. This is a count of closes, not a measure of future blocked windows, which depend on how often strategies exit.
- **What v1 guarantees.** v1 only adds refusals. Every entry it admits, today's realised-only gate also admits, so it is never weaker than today.
  - It does not claim to see every loss. Each residual below is a loss that today's gate does not see either.
  - Each residual is named so that a later slice can close it, not because v1 relies on it being zero.
- **Named residuals (r9):**
  1. The interval between `P_close` and S's mark time. It is measurable once S exists (Source rule).
  2. Account-level charges: a charge booked to the account without being attributed to a position. It is outside every position's `unrealizedPnL` and `totalFees`, and so outside both engine guards and `PotNav`. The only observer of account cash is `account_reconciliation_check` (#2844 clause 3), which compares broker equity with the local book; it is not an entry guard.
  3. Position-tuple coherence. eToro documents a time for the P&L mark only. If the broker served units that lag a close while its P&L already reflected it, and the ledger also lacked that close, the units and ledger would agree, and the missing realised P&L would go unseen. Ledger reconciliation at both endpoints catches every case where units and ledger disagree. On all 252 stored marks they agree (section 5).
  4. A split exactly offsetting an unrecorded close: for example, half closed unrecorded, then a 2-for-1 split restoring the units. Any non-offsetting combination fails the unit and ledger checks.
  5. Fee posting time. `totalFees` must be unchanged between the endpoints, but when a fee or dividend enters `unrealizedPnL` relative to `totalFees` is undocumented. A posting that moves between them inside the window is unseen. All 118 stored values since capture began are 0 (section 5).
- The follow-up slice v2 adds the realised term once the accounting rule is established. Its evidence so far is in sections 4 and 5: the 5-close identity, and cost-basis conservation with its 4 exceptions. v1 does not use either.

## Source rule

- **Daily P&L convention (published).** Interactive Brokers, TWS Reference, "Profit and Loss" (<https://www.interactivebrokers.com.hk/php/webhelp/TWS_Reference_Information/pnl_.htm>):
  - Daily P&L: *"(current price - prior day's closing price) x (total number of outstanding shares) plus New Position calculation for all new positions."*
  - New Position: *"(current price - purchase price) x (number of outstanding shares purchased today)."*

  **The reference for a held position is the prior session's close.** For a US listing that is the regular-session close: 16:00 New York, or 13:00 on a `us_market_status` half day.
- **Mark source.** Both sides are eToro's own per-position `unrealizedPnL.pnL`, in account currency, from `TradingRealAdminApi_Position` (sql/383 header), as stored for #3068.
  - v1 subtracts the broker's figure from itself. It reprices nothing, converts no currency, and does not depend on direction.
  - A position opened since the anchor contributes its own `unrealizedPnL`, which the broker measures from purchase price: IBKR's "New Position" case.
- **Mark time.** `unrealizedPnL.timestamp` is the documented time "when the broker struck the mark" (`BrokerDirectPositionInvestment.pnl_timestamp`, `app/providers/broker.py`). On all 252 stored marks from 2026-09-15 to 2026-10-11, it is present and 0.07 to 4.2 s before its snapshot's `observed_at` (section 5).
  - The other fields of a position (units, fees) carry no documented time of their own.
  - v1 does not assume they share the mark's instant.
  - Instead it reconciles each endpoint's broker units against the ledger: the open's units minus every recorded close executed at or before that snapshot's receipt time must equal the broker's `units` (Design 4).
  - On all 252 stored marks, this reconciles, 69 of them engine-owned; every position has its open event (section 5).
  - A close the ledger has not yet recorded, or recorded late, therefore refuses whenever the broker's units already reflect it, whichever side of the mark time it happened on. When both units and ledger lag, the close is unseen; that is named residual 3.
- **`totalFees`** is documented as *"Total overnight fees and dividends charged/paid on the position in USD. Negative amount represents refund"*. No eToro document states whether, or when, those amounts also enter `unrealizedPnL`. So v1 adopts a **conservative local policy**: any change in `totalFees` between the two endpoints refuses (Design 4), and v1 neither adds nor subtracts it.
  - It is NULL on the 134 stored marks before 2026-10-01, when its capture began, and present and 0 on all 118 since (section 5).
- **Event identity.** `trade_events` allows one open per position (`uq_trade_events_open`) and one close per `(position_id, executed_at)` (`uq_trade_events_close`).
- **Principal flows are excluded exactly.** `PotNav` separates principal from P&L (`engine_pot_risk.py`, module docstring, GIPS 2020 2.A.24(e)/(f) as already cited there), and this term uses P&L only.
- **Parent/child completeness** is a documented precondition: the sql/383 header requires the mark count to equal `official_direct_long_positions + official_direct_short_positions`. **Currency** must be documented as USD, the same refusal `observe_pot_nav` applies (`_snapshot_positions`).
- **Limit and denominator are unchanged:** `pool_base × mandate_max_daily_loss_pct / 100` (sql/311 denominates every mandate percentage on effective pot capital).

**Local policy choices** (no published rule found; each is frozen by this spec):

- **D, P and S.**
  - D is `risk.observed_at`'s New York date, and `D_start` is D 00:00 New York. The reset at New York midnight is local policy; the quoted IBKR text gives no reset time.
  - P is `latest_completed_us_session(D_start)`, and `P_close` is P's regular close. Both helpers are in `market_calendar.py`, which is ranking-policy-hashed, so they are called and never edited.
  - S is P's close capture (Design 1). On weekends and holidays P does not move. Saturday through Monday measure from the latest preceding session's S, normally Friday's, so a loss can appear in the figure of every calendar day until the next session's S exists.
- **What is not counted, and when.** Moves between `P_close` and S's mark time are excluded from D's figure. On P itself they are counted by any observation after S, which still measures from the session before P. From S to D's midnight, an observation on P (from the previous S) and one on D (from S) both count the move.
  - Retained-row price comparison, every mark taken after its session's close, 2026-09-15 to 2026-10-11 (section 3; `price_daily` is back-adjusted, sql/479 header):
    - IEP, NXH, SPY.RTH, SVOL, SVXY and VOO: `close_rate` equals the session's stored close on all 139;
    - QQQ differs on 15 of 19, and GME on 14 of 18.
  - The excluded interval's length is **unmeasured until S rows exist**. Each S stores its `observed_at` and every child's `pnl_timestamp`, so every day's interval is auditable from then on.
  - Section 1 is scheduling evidence only: a successful sync started after the close on all 19 sessions, at a job-start lag of median 0.0 minutes and maximum 76.8.

## Design

1. **Close capture (schema and writer).**
   - Migration `sql/482_broker_account_session_close_marks.sql` creates `broker_account_session_close_snapshots (environment, session_date, observed_at, currency, account_currency_id, official_direct_long_positions, official_direct_short_positions, PRIMARY KEY (environment, session_date))` and `broker_account_session_close_marks`.
     - The marks table has `broker_account_position_marks`'s per-position columns, keyed `(environment, session_date, position_id)`, with an FK to the parent. `pnl_timestamp` and `total_fees` are NOT NULL.
     - Every numeric column is CHECKed finite: `<> 'NaN'` and not `±Infinity`, both of which PostgreSQL `numeric` admits.
     - Both tables are insert-once: UPDATE is refused by sql/424's `etoro_crowd_append_only` trigger, as in sql/479.
     - No constraint is added to `trade_events`, whose writer `sync_portfolio` runs before any evidence savepoint.
   - Writer: `account_equity_evidence.record_session_close_snapshot(conn, *, environment, snapshot)`.
     - It derives `P = latest_completed_us_session(snapshot.observed_at)`.
     - It writes only when all of these hold:
       - `snapshot.observed_at`'s New York date is P;
       - `_validate_snapshot` accepts the snapshot;
       - every direct position has a finite `unrealized_pnl` and `units`, a non-NULL finite `total_fees`, and a `pnl_timestamp` in `[P_close, snapshot.observed_at]`.
     - The parent is `INSERT … ON CONFLICT DO NOTHING RETURNING`, and the children are inserted only when the parent row was inserted. So S is **the first qualifying snapshot successfully inserted**, never replaced. A non-qualifying snapshot writes nothing and logs one INFO line naming the failed condition.
   - It is called from `daily_portfolio_sync` (scheduler, demo) after `record_account_equity_snapshot`, in its **own** savepoint, with the same catch-log-and-continue handling the equity snapshot has.
     - A write that raises is logged at ERROR, and the equity snapshot and the sync are kept.
     - No qualifying snapshot simply leaves S absent, which surfaces as `engine_pot_daily_anchor_missing` on D's entries.
     - The job ran at least 5 times (median 32) after every session's close in the census (section 1), so a non-qualifying first snapshot is retried. No new job or connection lane is added.
   - The provider comments on `pnl_timestamp` ("nothing reads it for a verdict") and `total_fees` ("a MEMO … never an operand") are updated to name this consumer. Their optional parsing stays.
2. **Population E.** E is every eligible trade under the pot epoch, read with `engine_book_rows`' eligibility, and its ownership row's `broker_position_id` where `claimed_at ≤ risk.observed_at`. An eligible trade whose status is open, closing or closed with no ownership row refuses `engine_capital_population_incomplete` inside the same statement, as `book_membership` does.
3. **Daily figure.** Let W = `(S.observed_at, risk.observed_at]`.

   `daily = Σ_{p held} (U_now(p) − U_S(p)) + Σ_{p opened in W} U_now(p)`

   It is admitted only when every p in E falls into one of the admitted cases below.
4. **Cases and witnesses.** Each p in E falls into exactly one case by its presence in S and in the live snapshot, and by its events in W. Every refusal is a returned code.

   | case | admitted when | otherwise |
   |---|---|---|
   | held: in S and live | no event for p in W; `units` equal; `total_fees` non-NULL, finite and unchanged | an event in W: a close gives `engine_pot_daily_close_in_window`, anything else `engine_pot_daily_event_unwitnessed`; changed units (for example a split) give `event_unwitnessed`; changed or NULL `total_fees` gives `engine_pot_daily_accounting_unverified` |
   | opened: not in S, live | exactly one event for p in W, its open, with non-NULL finite `units` equal to the live `units`; live `total_fees` = 0 | a close in W gives `close_in_window`; a missing open or a units mismatch gives `event_unwitnessed`; non-zero or NULL fees give `accounting_unverified` |
   | closed: in S, not live | never | `close_in_window` if a close for p is in W, else `event_unwitnessed` (a close the ledger missed) |
   | round trip: neither, events in W | never | `close_in_window` if a close is in W, else `event_unwitnessed` |
   | historical: neither, no events in W | contributes 0 | `event_unwitnessed` if its `claimed_at` is in W |

   - **Anchor.**
     - No S row for P gives `engine_pot_daily_anchor_missing`.
     - An account currency other than USD gives `engine_capital_snapshot_unusable`.
     - A NULL declared count, or a mark count different from `official_direct_long_positions + official_direct_short_positions`, gives `engine_pot_daily_anchor_incomplete`. A declared 0 with 0 marks passes.
   - **Mark times.**
     - For every live p in E, `pnl_timestamp` is non-NULL, at or before `risk.observed_at`, and at or after `decided_at − max_mark_age_seconds`.
       - `decided_at` is the executor's `now`, so the bound is the age at the decision, not at receipt (r8 finding 4).
       - The executor passes `intent.max_quote_age_seconds`, its own quote-age bound.
     - For p in both S and live, `t_now(p) > t_S(p)`.
     - Otherwise `engine_pot_daily_mark_stale`.
   - **Ledger reconciliation at both endpoints (r8 finding 1).** For each p in E present in S, and each p in E present live, `units(open) − Σ units(close executed ≤ that snapshot's receipt time)` must equal the snapshot's `units`. Otherwise `event_unwitnessed`.
     - This covers a close whose event is recorded late. Example: S's units already show a close that the ledger does not hold.
   - **Close veto through the decision (r11 finding 1).** Any recorded close of an E position executed in `(S.observed_at, decided_at]` refuses `close_in_window`. This includes positions claimed after the snapshot, and closes recorded after the live snapshot but before the decision. The marked figure itself still cuts at `risk.observed_at`. A close executed but not yet recorded by `decided_at` is seen on a later decision; that delay is the ledger's own.
   - **Straddle.** Any engine event executed between a position's mark time and its snapshot's receipt time, on either side, also gives `event_unwitnessed`, because units and mark may then describe different instants. The census found none (section 4).
   - **Numeric.** Every value read is checked `is_finite()` before any arithmetic. S is finite at rest by CHECK. The live side is checked by `observe_pot_nav`, which runs first.
5. **One read.** One SQL statement reads, under one snapshot even in READ COMMITTED:
   - S's parent and marks;
   - E, including eligible trades without ownership;
   - every `trade_events` row of E's positions up to `risk.observed_at`. Reconciliation needs each position's whole ledger; W and both straddle intervals are subsets of it.

   The new function `engine_pot_risk.observe_daily_marked_pnl(conn, snapshot, *, decided_at, max_mark_age_seconds) -> Decimal` combines that statement with the live snapshot in Python. It derives D, `D_start`, P and `P_close` from `snapshot.observed_at`.
   - `observe_pot_nav`, `advance_pot_drawdown`, `engine_book_rows`, `book_membership` and `_MANDATE_OBSERVATION_SQL` keep their implementations.
   - `_price_book` changes in one place (7.2). Also changed: the executor function, the scheduler job, two provider field comments, and `frontend/src/pages/StrategiesPage.tsx`'s refusal map (10).
   - An event committed after the statement's snapshot is not seen. A close it records normally changes the live snapshot's units or presence, and the case table then refuses. The exceptions are named residuals 3 and 4.
6. **Exception contract.** The function raises `EngineCapitalObservationError` with one of the codes above, which are added to `EngineCapitalRefusal`. `_observe_local_mandate_risk` catches it inside its transaction block and returns the code, as it already does for `advance_pot_drawdown`. The code reaches `_persist_rejection`; the message, which names the position, is logged.
7. **Executor changes.** In `_observe_local_mandate_risk`:
   1. **The realised check.**
      - Its dates stay derived from `now`, exactly as today (r10 finding 1). Deriving them from `risk.observed_at` would change the gate across New York midnight: an operator close just after midnight, outside E, counts toward the new day on `now` but not on the snapshot's day, so today's gate refuses where the changed one would admit. The marked check derives its own D from `risk.observed_at`, independently.
      - Its `daily_realised_pnl` must be finite before the comparison, else it refuses `engine_capital_population_incomplete`.
      - `_MANDATE_OBSERVATION_SQL`'s text is unchanged.
      - It still sums the whole New York calendar day as visible when it runs, the frozen preview gates' behaviour, kept because they share the SQL.
   2. **`advance_pot_drawdown(conn, observe_pot_nav(conn, risk))`, with one change in `_price_book`.**
      - Each position's realised aggregate is checked `is_finite()` before it is added, and a non-finite one raises `EngineCapitalObservationError(…, "engine_capital_population_incomplete")`.
      - Today two aggregates of opposite infinite sign would raise `decimal.InvalidOperation` during the sum. That escapes every handler and aborts the paper cycle, exits included (the comment in `_risk_and_amount`).
      - Neither `ai_trial_policy.POLICY_MODULES` (10 modules) nor `ranking_pot_policy.POLICY_MODULES` (28) contains `engine_pot_risk` or the executor (section 5), so no declaration hash moves.
   3. **The marked check.** `observe_daily_marked_pnl`, then the threshold `daily <= −daily_loss_limit`, which returns `portfolio_daily_marked_loss_limit`. A marked-limit breach and a marked-observation refusal both come after the drawdown advance. The realised refusal keeps its earlier return.

   **Out of scope, recorded on #3612 as a remaining slice.** The drawdown index can link a NAV built on an incomplete ledger: `_price_book` accepts a released position with any close history. `refresh_strategy_health` → `_pot_drawdown_block` advances the index every tick without anything this slice adds. The fix belongs in `observe_pot_nav`, covering both callers. **Resolved 2026-10-12** by `engine_pot_risk._check_units_conserved` inside `_price_book`, which both NAV readers share. Each position's ledger balance (open less its closes) must be at least its broker units, zero once released, and a trade's shortfalls must sum to the unowned partial-close slices under its entry order, whose realised P&L is now counted. With no open ingested yet, a held position passes only with no recorded close or slice and `isPartiallyAltered` false.
8. **The two limits are independent.**
   - The realised check sums every demo close in the New York calendar day. `_MANDATE_OBSERVATION_SQL` has no epoch, ownership or verdict filter, so it includes the operator's closes.
   - The marked check covers E only, from S, and refuses outright once E has a close in W.
   - Both are tested: a −2,000 realised close refuses on the realised check, and a −2,000 mark move with no close refuses on the marked one.
9. **Frozen preview gates are out of scope.**
   - `ai_trial_start_gate.py` and `ranking_pot_activation.py` are hashed into declarations 16 and 23 and keep realised-only.
   - Both import `_MANDATE_OBSERVATION_SQL`, so **it stays byte-identical**, pinned by a test on its SHA-256.
10. **Operator labels** in `StrategiesPage.tsx`'s refusal map:
    - `portfolio_daily_marked_loss_limit`, "The mandate's daily loss limit is reached on marked P&L";
    - `engine_pot_daily_close_in_window`, "An engine position closed after the daily anchor; new entries wait until a later session's close capture becomes the anchor";
    - one line each for `engine_pot_daily_anchor_missing`, `_anchor_incomplete`, `_accounting_unverified`, `_mark_stale` (a position's broker mark is missing, in the future, older than the quote-age bound, or older than its anchor) and `engine_pot_daily_event_unwitnessed`.
11. **Exits.**
    - The check sits only on the entry sizing path (`_risk_and_amount` → `_observe_local_mandate_risk`), and every refusal it adds is a returned code.
    - The capture writer runs in `daily_portfolio_sync`'s own savepoint, after `sync_portfolio`, and the paper cycle does not wait on it.
    - Nothing in this slice adds a constraint to the ingestion path that records exits.

## Operational dependency

S exists only if a qualifying snapshot is inserted on P's New York date after `P_close`.
- A miss refuses entries for D, which is fail-closed. It appears as `engine_pot_daily_anchor_missing` in the entry's recorded reason; a raising write is also an ERROR in the sync log.
- A successful sync run started after the close existed for 19 of 19 sessions since 2026-09-15 (section 1). That counts runs, not captures. Capture coverage is measured from S rows after deployment.

## Tests

- **Writer (DB):**
  - nothing is written for an observation before `P_close`, on a closed day, or on the New York date after P;
  - nothing is written with any position's `pnl_timestamp` NULL, before `P_close` or after `observed_at`, or its `total_fees` NULL;
  - the first qualifying observation writes the parent and children;
  - a later one the same evening changes nothing, children included;
  - UPDATE is refused, and NaN or Infinity is refused in each numeric column;
  - a half day uses 13:00;
  - a capture failure leaves the equity snapshot stored and the sync successful.
- **Pure, per case:**
  - held, +100 at S → −400 now gives −500;
  - a held short and a held non-USD position give their broker ΔU;
  - opened in W, now −400, gives −400;
  - historical with no events and `claimed_at` before W gives 0;
  - principal change with P&L unchanged gives 0.
- **Refusals, one test per table cell:**
  - a held position with a partial or full close in W refuses `close_in_window`;
  - an opened position with a close in W refuses `close_in_window`;
  - a round trip in W refuses `close_in_window`;
  - a closed position with no close event refuses `event_unwitnessed`;
  - an opened position with no open, or with open units different from live, refuses `event_unwitnessed`;
  - a held position with changed units and no event (a split) refuses `event_unwitnessed`;
  - a held position with changed or NULL `total_fees` refuses `accounting_unverified`, and the same non-zero value on both sides does not;
  - historical with `claimed_at` in W refuses `event_unwitnessed`;
  - an eligible trade with no ownership refuses `engine_capital_population_incomplete`.
- **Anchor and marks:**
  - a child-count mismatch refuses `anchor_incomplete`, and so does a NULL declared count, while a genuine zero-position S passes;
  - a non-USD or undocumented account currency refuses;
  - a live `pnl_timestamp` that is NULL, older than `max_mark_age_seconds` or later than `risk.observed_at` refuses `mark_stale`;
  - so does S at 23:59:59 on P with a live mark of 23:59:58;
  - an engine event between a position's mark time and its snapshot's receipt time, on either side, refuses `event_unwitnessed`;
  - r8's counterexample refuses `event_unwitnessed`: S holds 5 units after a partial close of 10 whose event is not yet recorded, and live also holds 5;
  - the same reconciliation failure on the live side refuses;
  - an engine close executed and recorded between `risk.observed_at` and `decided_at` refuses `close_in_window`;
  - a close with NULL or NaN `units` in the position's ledger refuses `event_unwitnessed`, and a later exit in the same cycle executes;
  - the `close_in_window` label's resumption rule: a close after Friday's S still refuses on Saturday and Monday, and a Monday-evening capture releases entries from Tuesday;
  - a Monday S with mark 16:00:00, a close at 16:00:01 and receipt 16:00:02 keeps refusing on Tuesday;
  - realised check at midnight: risk observed Friday 23:59:50, decision Saturday 00:00:10, operator close outside E at 00:00:05 with −2,000 realised refuses `portfolio_daily_loss_limit`, as today;
  - a mark 50 s old at receipt, with receipt 20 s before the decision and a 60 s bound, refuses; 30 s plus 20 s passes;
  - events exactly at `S.observed_at` are outside W, and events exactly at `risk.observed_at` are inside it.
- **Calendar:**
  - D, `D_start`, P and `P_close` for a Monday, for the Tuesday after a Monday holiday, for the day after a half day, across both 2026 DST transitions (03-08, 11-01), and across a year boundary;
  - either side of Friday → Saturday midnight selects different P, and either side of Saturday → Sunday midnight selects the same P;
  - the Monday after Good Friday (2026-04-03, closed) uses Thursday's S.
- **Threshold:** at, just below and just above −limit; a non-breach proceeds to sizing.
- **Executor (DB):**
  - the marked check refuses when realised P&L does not breach, and the realised check still refuses when the marked one does not;
  - a non-finite realised total refuses rather than raising;
  - two historical closes with realised `+Infinity` and `−Infinity` make the entry refuse `engine_capital_population_incomplete`, and a later exit in the same cycle executes;
  - after a marked refusal of either kind, the drawdown state has advanced to the current observation;
  - each new refusal reaches the entry's recorded reason;
  - in one cycle, an entry refused by each new code is followed by an exit that executes.
- **Drift:**
  - the `_MANDATE_OBSERVATION_SQL` SHA-256 is pinned;
  - `scripts.ai_trial_policy_guard` reports MATCH for declaration 16;
  - `RANKING_POT_POLICY_HASH` is unchanged before and after.

## Full-population verification

All figures come from `PYTHONPATH=. uv run python scripts/measure_3612_marked_daily_loss_evidence.py --since 2026-09-15 --until 2026-10-11`, run 2026-10-12, except the IBKR quotation.
- Lifetime queries in section 5 are bounded by `executed_at` / `claimed_at` before 2026-10-12 00:00Z and `snapshot_date ≤ --until`.
- The latest pool event is current state and is not bounded.

1. **Scheduling.**
   - 19 NYSE sessions, each with a successful `daily_portfolio_sync` started after the close on its New York date.
   - Job-start lag: median 0.0 minutes, maximum 76.8 (2026-09-25).
   - Successful runs per session after the close: minimum 5, median 32.
2. **Snapshot continuity, every UTC date.**
   - 27 of 27 dates have a snapshot.
   - None has a NULL declared count, or a count different from its stored marks.
3. **Retained-row price comparison.** 176 marks taken after their session's close, all compared, none missing a bar or instrument metadata.
4. **Conservation, 26 consecutive snapshot pairs, each position cut at its own mark times.** The population covers endpoint marks, positions with events, ownership claimed in the window, and every ownership row claimed by the later endpoint, which is a superset of E.
   - 0 events fall between a position's mark time and its snapshot's receipt time.
   - Units: one failure, position 3602947846: no ownership row, never in a mark, an open of 0.646 units and a close of 0.323 (section 5).
   - Cost basis (v2 evidence, unused here): 4 discrepancies, all non-engine positions opened on 2026-09-23 between 00:05 and 07:59Z. Each records an open `investment_usd` of 50 and a close of 25. Three closed their full units; 3602947846 closed half. The histories are in section 5.
5. **Point claims:**
   - the pool limits;
   - weekday after-close snapshot times, 16:14:37 to 19:56:03 New York;
   - the first stored mark date;
   - the history of every position failing either check, with each event's `investment_usd`;
   - ledger units against broker units at every mark: 252 of 252 reconcile, 69 engine-owned;
   - the 5-close identity (v2 evidence);
   - mark timestamps on 252 of 252 marks, 0.07 to 4.2 s old;
   - `total_fees`: 118 zero, 0 non-zero and 134 NULL, all before 2026-10-01;
   - `trade_events`: 36 rows, 0 non-finite or NULL required fields;
   - engine-owned closes: 1, on 2026-09-23;
   - both policy-module lists' counts, `ai_trial_policy`'s names, and that neither list contains `engine_pot_risk` or the executor.

The census is retrospective over retained rows. It does not show that a row existed at each past decision instant. The live gate reads at decision time, and an absent S refuses.

## Security

No authorisation surface changes. This adds:
- a fail-closed entry refusal on the demo paper executor;
- an append-only evidence capture, written by an existing demo job from a snapshot that job already fetches;
- a finiteness check in `_price_book` that turns a would-be crash into a refusal.

## Closure audit

Rounds 4 to 6, each finding with its disposition, are in the closure table at `c2109f14` (`git show c2109f14:docs/proposals/risk/2026-10-12-3612-marked-daily-loss.md`). r8 removed the realised term, which supersedes:
- r4.2, r6.3 and r6.6: the realised/unrealised identity;
- r5.1, r5.2 and r6.2: fees on closes, now refused outright as a close;
- r6.4: a split hiding a missing close. This is partly addressed: unit and ledger reconciliation catch every non-offsetting case, and the exactly offsetting case is accepted as named residual 4;
- r6.7: dividends at close, now a close;
- r6.12: NULL rules for close fields, for the realised and fee fields, which v1 does not read.

v1 does read every close's `units`, for ledger reconciliation. It requires them non-NULL and finite; otherwise `event_unwitnessed`, a returned code. It also checks units and finiteness on opens.

| round 7 finding | severity | disposition |
|---|---|---|
| 1 `amount` includes collateral | BLOCKING | Superseded: cost basis is not a v1 witness |
| 2 accounting identity rests on a sample | BLOCKING | Superseded: no realised term; a close refuses |
| 3 live mark can precede anchor | BLOCKING | Resolved: `t_now(p) > t_S(p)` (Design 4), with a midnight test |
| 4 mark time vs position tuple | WARNING | Partly addressed: straddling events and ledger disagreement refuse; the remaining case is accepted as named residual 3 |
| 5 account-level charges | WARNING | Open, stated as a v1 exclusion (Scope) |
| 6 50/25 unreproduced | WARNING | Resolved: the history prints each `investment_usd` |
| 7 policy counts | WARNING | Resolved: section 5 prints both lists |
| 8 "2 of 5" | WARNING | Superseded: the identity is v2 evidence |
| 9 numeric census | WARNING | Resolved: required NULL and non-finite `investment_usd` counted (section 5) |
| 10 unbounded queries | WARNING | Resolved: lifetime queries bounded and their scope printed |
| 11 fee contract contradictions | WARNING | Resolved: one rule per case in the table |

| round 8 finding | severity | disposition |
|---|---|---|
| 1 delayed close defeats straddle | BLOCKING | Resolved for the stated counterexample (units disagree with the ledger) by reconciliation at both endpoints, measured 252/252 and tested. The units-agree variant is named residual 3 |
| 2 fee rule from a CHECK | WARNING | Resolved: stated as conservative local policy, with no inclusion claim |
| 3 account-level charges | WARNING | Open, stated as a v1 exclusion (Scope) |
| 4 doubled mark-age allowance | WARNING | Resolved: age measured at `decided_at` |
| 5 refusal window | WARNING | Resolved: resumption rule stated; the census figure labelled a close count |
| 6 unbounded predicates | WARNING | Resolved: ownership and first-mark queries bounded; pool event marked current |
| 7 "round trips" | WARNING | Resolved: "discrepancies", with every history printed |
| 8 policy-list contents | NIT | Resolved: counts and names printed |
| 9 earlier dispositions | NIT | Resolved: supersession listed with reasons; full table at `c2109f14` |
| 10 UI file in inventory | NIT | Resolved (Design 5) |

| round 9 finding | severity | disposition |
|---|---|---|
| 1 tuple coherence: stale units with fresh P&L | BLOCKING | Named residual 3. Contested as BLOCKING because v1 is monotone over today's gate (Scope, "What v1 guarantees"). No eToro field establishes tuple coherence, and no local witness can. |
| 2 split exactly offsetting an unrecorded close | BLOCKING | Named residual 4, same rationale. Every non-offsetting case refuses. |
| 3 fee posting time | BLOCKING | Named residual 5, same rationale |
| 4 drawdown advance before evidence refusal | WARNING | Open, deferred with r4.6 to `observe_pot_nav`, the shared fix for both callers |
| 5 account-charge backstop overstated | WARNING | Resolved: the only observer named, and stated as not an entry guard |
| 6 label duration | WARNING | Resolved: label reworded, with a resumption test |
| 7 close units are read | NIT | Resolved: close units required non-NULL and finite, with a test |

Still open and deferred: the drawdown-ledger integrity in `observe_pot_nav` (r4.6, r5.6). It is recorded on #3612. Resolved 2026-10-12 by unit conservation in `_price_book` (Design 7, the out-of-scope note).

| round 10 finding | severity | disposition |
|---|---|---|
| 1 realised date moved to `risk.observed_at` breaks monotonicity | BLOCKING | Resolved: the realised check keeps `now` (Design 7.1); the midnight operator-close regression test is added |
| 2 dispositions stronger than the residuals | WARNING | Resolved: r6.4, r7.4 and r8.1 qualified; Design 5 points to residuals 3 and 4 |
| 3 straddled immutable anchor | WARNING | Resolved: the resumption rule requires a passing S; tested |
| 4 engine close time | NIT | Resolved: section 5 prints each engine-owned close's position and time |

Round 10's judgement: the three round-9 cases are WARNING-level residuals under the monotone guarantee, and the drawdown-ledger deferral is acceptable.

| round 11 finding | severity | disposition |
|---|---|---|
| 1 closes between snapshot and decision | WARNING | Resolved: close veto through `decided_at` (Design 4), tested |
| 2 delayed-close sentence | WARNING | Resolved: qualified with residual 3 |
| 3 close-to-capture gap | WARNING | Accepted residual 1. Follow-up: measure capture delay from S rows and set a maximum |
| 4 account-level charges | WARNING | Accepted residual 2. Follow-up on #3612 |
| 5 offsetting split and close | WARNING | Accepted residual 4 |
| 6 fee posting time | WARNING | Accepted residual 5 |
| 7 drawdown ledger integrity | WARNING | Open, deferred to `observe_pot_nav` (both callers), recorded on #3612 |
| 8 script window description | NIT | Resolved |

Checkpoint 1 closed at round 11, with no BLOCKING finding.
