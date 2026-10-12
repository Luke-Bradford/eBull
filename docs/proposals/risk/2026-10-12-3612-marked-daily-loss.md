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

Round 4 removed external prices. The anchor is the broker's own marks, captured once from the first qualifying sync observation after P's regular close, so the day is a difference of two broker marks. Round 5, this version, keeps that design and closes round 4's findings: the accounting identity, the mark timestamp, numeric safety, and census precision.

## Source rule

- **Daily P&L convention (published).** Interactive Brokers, TWS Reference, "Profit and Loss" (<https://www.interactivebrokers.com.hk/php/webhelp/TWS_Reference_Information/pnl_.htm>):
  - Daily P&L: *"(current price - prior day's closing price) x (total number of outstanding shares) plus New Position calculation for all new positions."*
  - New Position: *"(current price - purchase price) x (number of outstanding shares purchased today)."*

  **The reference for a held position is the prior session's close.** For a US listing that is the regular-session close: 16:00 New York, or 13:00 on a `us_market_status` half day.
- **Mark source and time.** Both sides are eToro's per-position `unrealizedPnL` from `TradingRealAdminApi_Position` (sql/383 header), as stored for #3068.
  - `unrealizedPnL.timestamp` is the documented time "when the broker struck the mark" (`BrokerDirectPositionInvestment.pnl_timestamp`, `app/providers/broker.py`).
  - On all 252 stored marks from 2026-09-15 to 2026-10-11, it is present and 0.07 to 4.2 s before its snapshot's `observed_at` (section 5). So the mark time, not the receipt time, is the instant S stands for, and S requires it (Design 1).
- **Realised and unrealised are on one convention.** eToro's `unrealizedPnL` and the close's `realized_pnl_usd` must agree, or a close at S's mark would show a spurious P&L. Census of every close of a position present in the preceding stored snapshot (section 5): 5 closes, `realised − U_S × closed/held − (price − close_rate_S) × closed × conversion` is within ±0.0076 USD.
  - All 5 have `fees_usd` 0 and a USD asset currency.
  - All 252 marks have `total_fees` 0, a USD asset currency and `is_buy` true.

  The identity is measured only under those conditions, so the design refuses outside them (Design 4).
- **`totalFees`** is documented as *"Total overnight fees and dividends charged/paid on the position in USD. Negative amount represents refund"* (`app/providers/broker.py`). That one signed number carries both fees and dividends. How a dividend is signed in it has not been observed: every stored value is 0.
- **Event identity.** `trade_events` allows one open per position (`uq_trade_events_open`) and one close per `(position_id, executed_at)` (`uq_trade_events_close`). A duplicated round trip therefore cannot add a second open, and a duplicated close breaks unit conservation.
- **Principal flows are excluded exactly.** `PotNav` separates principal from P&L (`engine_pot_risk.py`, module docstring, GIPS 2020 2.A.24(e)/(f) as already cited there), and this term uses P&L only.
- **Parent/child completeness** is a documented precondition: the sql/383 header requires the mark count to equal `official_direct_long_positions + official_direct_short_positions`. **Currency** must be documented as USD, the same refusal `observe_pot_nav` applies (`_snapshot_positions`).
- **Limit and denominator are unchanged:** `pool_base × mandate_max_daily_loss_pct / 100` (sql/311 denominates every mandate percentage on effective pot capital).

**Local policy choices** (no published rule found; each is frozen by this spec):

- **D, P and S.**
  - D is `risk.observed_at`'s New York date, and `D_start` is D 00:00 New York. The reset at New York midnight is local policy; the quoted IBKR text gives no reset time.
  - P is `latest_completed_us_session(D_start)`, and `P_close` is P's regular close. Both helpers are in `market_calendar.py`, which is ranking-policy-hashed, so they are called and never edited.
  - S is P's close capture (Design 1). On weekends and holidays P does not move: Saturday, Sunday and Monday all measure from Friday's S, so a loss can appear in the figure of every calendar day until the next session's S exists.
- **What is not counted, and when.** Moves between `P_close` and S's mark time are excluded from D's figure. On P itself they are counted by any observation after S, which still measures from the session before P. From S to D's midnight, an observation on P (from the previous S) and on D (from S) both count the move.
  - Measured, every mark taken after its session's close, 2026-09-15 to 2026-10-11 (section 3, a retained-row comparison against `price_daily`, which is back-adjusted, sql/479 header):
    - IEP, NXH, SPY.RTH, SVOL, SVXY and VOO: `close_rate` equals the session's stored close on all 139;
    - QQQ: differs on 15 of 19, and GME on 14 of 18. Both trade 24/5.
  - The excluded interval is stored per capture (S's `observed_at` and each child's `pnl_timestamp`), so each day's interval is auditable. Its expected length is the job's start lag after the close: median 0.0 minutes, maximum 76.8 (2026-09-25), over 15 minutes on 3 of 19 sessions (section 1). That is job-start lag, not capture lag; capture lag is measured from S rows once they exist.

## Design

1. **Close capture (schema and writer).**
   - Migration `sql/482_broker_account_session_close_marks.sql` creates `broker_account_session_close_snapshots (environment, session_date, observed_at, currency, account_currency_id, official_direct_long_positions, official_direct_short_positions, PRIMARY KEY (environment, session_date))` and `broker_account_session_close_marks`.
     - The marks table has `broker_account_position_marks`'s per-position columns, `pnl_timestamp NOT NULL` included, keyed `(environment, session_date, position_id)`, with an FK to the parent.
     - Every numeric column is CHECKed finite (`<> 'NaN'` and not `±Infinity`, which PostgreSQL `numeric` admits).
     - Both tables are insert-once: UPDATE is refused by sql/424's `etoro_crowd_append_only` trigger, as in sql/479.
   - The same migration adds finite CHECKs to `trade_events.units`, `price`, `fees_usd` and `realized_pnl_usd`. Today's CHECKs (`units > 0`, `price > 0`) admit NaN, because PostgreSQL sorts NaN above every number. 0 of 36 existing rows violate them (section 5 counts, re-checked by the migration's own validation).
   - Writer: `account_equity_evidence.record_session_close_snapshot(conn, *, environment, snapshot)`.
     - It derives `P = latest_completed_us_session(snapshot.observed_at)`.
     - It writes only when all of these hold: `snapshot.observed_at`'s New York date is P; every direct position has a `pnl_timestamp` at or after `P_close`; and `_validate_snapshot` accepts the snapshot.
     - The parent is `INSERT … ON CONFLICT DO NOTHING RETURNING`, and the children are inserted only when the parent row was inserted. So S is **the first qualifying snapshot successfully inserted**, never replaced. A later run the same evening changes nothing.
   - It is called from `daily_portfolio_sync` (scheduler, demo) after `record_account_equity_snapshot`, in its **own** savepoint with the same catch-log-and-continue handling that the equity snapshot already has (`app/workers/scheduler.py`, the `with conn.transaction():` around `record_account_equity_snapshot`). A capture failure therefore neither loses the equity snapshot nor fails the sync. It is logged as ERROR and surfaces as `engine_pot_daily_anchor_missing` on D's entries.
   - The job runs every 5 to 10 minutes on the live schedule, so no new job or connection lane is added.
2. **Population E.** E is every `broker_position_id` with an ownership row on an eligible trade under the pot epoch (`engine_book_rows`) whose `claimed_at ≤ risk.observed_at`. This is the cut `book_membership` applies to a snapshot (r2 B2).
3. **Per-position daily P&L.** With W = `(S.observed_at, risk.observed_at]`, for each p in E:

   `daily_p = U_now(p) − U_S(p) + Σ realised(p, closes in W)`

   - `U_now(p)` is the live snapshot's `unrealized_pnl` if p is present, else 0.
   - `U_S(p)` is S's stored mark if p is present, else 0.
   - The book's daily P&L is Σ over E.
   - Under the measured identity, a close executed in W at a price equal to S's `close_rate` contributes 0. For example, closing half: −50 + 50 = 0.
   - A position opened in W contributes against its purchase price, which is IBKR's "New Position" case.
   - A position closed before S belongs to P's day, whether or not its close was recorded (see 7 on the ledger).
4. **Witnesses.** Each refuses the entry with its own code; none is coerced.
   - **Anchor.**
     - No S row for P gives `engine_pot_daily_anchor_missing`.
     - A currency other than USD gives `engine_capital_snapshot_unusable`, as `_snapshot_positions`.
     - A NULL declared count, or a mark count different from `official_direct_long_positions + official_direct_short_positions`, gives `engine_pot_daily_anchor_incomplete`. A declared 0 with 0 marks passes.
   - **Accounting scope.** `engine_pot_daily_accounting_unverified` is returned, for any p in E, when:
     - p is short;
     - p's asset currency is not USD;
     - `total_fees` is NULL on either side, or differs between S and now;
     - a close in W has non-zero `fees_usd`.

     These are the cases outside the measured identity. A dividend changes `total_fees` with a sign not yet observed, so an ex-dividend day on a held name refuses entries until the next S. Replacing that refusal with a signed term is #3612 follow-up work, due once one engine-held dividend has been observed.
   - **Events.** For every p in E:
     - `units_S(p) + Σ open units − Σ close units (events in W) = units_now(p)`, with units 0 where p is absent from a side;
     - every close in W has a non-NULL `realized_pnl_usd`;
     - if p is absent from S and its `claimed_at` is in W, its open event is in W.

     A failure gives `engine_pot_daily_event_unwitnessed`. A position present in S is not asked for an open in W, so a claim recorded just after S for a position opened before it is accepted (r4 finding 13).
     - The unit check catches a missing partial or final close in W. Section 4's census shows that shape is real: position 3602947846 (instrument 3075, no ownership row, never in a mark) opened 0.646 units on 2026-09-23, recorded a close of 0.323, and has no further event.
   - **Numeric.** Every value is finite at rest (the CHECKs above, plus `observe_pot_nav`'s finiteness check on the live side). The function also checks `is_finite()` before any arithmetic, as a second barrier, with the codes above.
   - **Splits.** A split after S changes units at the broker without a trade event, so the unit check refuses D's entries until the next session's S, taken after the split. U is a USD value, so it is not re-based.
5. **One read.** One SQL statement reads, under one snapshot even in READ COMMITTED:
   - S's parent and marks;
   - E, with `claimed_at`;
   - E's `trade_events` in W.

   The new function `engine_pot_risk.observe_daily_marked_pnl(conn, snapshot) -> Decimal` combines that statement with the live snapshot in Python. It derives D, `D_start`, P and `P_close` from `snapshot.observed_at`.
   - `observe_pot_nav`, `advance_pot_drawdown`, `engine_book_rows`, `book_membership`, `_price_book` and `_MANDATE_OBSERVATION_SQL` keep their implementations.
   - The daily figure depends only on that statement and the live snapshot, never on `observe_pot_nav`'s separate reads.
   - An event committed after the statement's snapshot is not seen. If it changed units before `risk.observed_at`, the unit check refuses.
6. **Exception contract.** The function raises `EngineCapitalObservationError` with one of the codes above, which are added to `EngineCapitalRefusal`. `_observe_local_mandate_risk` catches it inside its transaction block and returns the code, as it already does for `advance_pot_drawdown`. The code reaches `_persist_rejection`; the message, which names the position, is logged.
7. **Executor changes.** In `_observe_local_mandate_risk`:
   1. The realised check's dates come from `risk.observed_at` instead of `now` (r3 finding 8). Its `daily_realised_pnl` must be finite before the comparison, else it refuses `engine_capital_population_incomplete`. `_MANDATE_OBSERVATION_SQL`'s text is unchanged.
      - It still sums the whole New York calendar day as visible when it runs, including a close executed after `risk.observed_at`. That is the frozen preview gates' behaviour, kept because they share the SQL (r4 finding 14).
      - `risk.observed_at` is at most `max_quote_age_seconds` older than `now` (`_age_ok` in `_risk_and_amount`).
   2. `advance_pot_drawdown(conn, observe_pot_nav(conn, risk))`, unchanged.
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
    - one line each for `engine_pot_daily_anchor_missing`, `_anchor_incomplete`, `_accounting_unverified` and `engine_pot_daily_event_unwitnessed`.
11. **Exits.**
    - The check sits only on the entry sizing path (`_risk_and_amount` → `_observe_local_mandate_risk`), and every refusal it adds is a returned code, never a raise.
    - The capture writer runs in `daily_portfolio_sync`'s savepoint, so the paper cycle does not wait on it.
    - The new `trade_events` CHECKs make a non-finite event fail at insert, in the sync that writes it, which is where a malformed broker value belongs.

## Operational dependency

S exists only if a qualifying snapshot is inserted on P's New York date after `P_close`. A miss refuses entries for D, which is fail-closed, and appears as `engine_pot_daily_anchor_missing` in the entry's recorded reason and as an ERROR in the sync log. A successful sync run started after the close existed for 19 of 19 sessions since 2026-09-15 (section 1). That counts runs, not captures; capture coverage is measured from S rows after deployment.

## Tests

- **Writer (DB):**
  - nothing is written for an observation before `P_close`, on a closed day, on the New York date after P, or with any position's `pnl_timestamp` before `P_close` or NULL;
  - the first qualifying observation writes the parent and children;
  - a later one the same evening changes nothing, children included;
  - UPDATE is refused, and NaN or Infinity is refused in each numeric column;
  - a half day uses 13:00;
  - a capture failure leaves the equity snapshot stored and the sync successful.
- **Migration (DB):** each new `trade_events` CHECK refuses NaN, `Infinity` and `-Infinity`, and asserts `diag.constraint_name`.
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
  - a short, a non-USD asset, `total_fees` NULL or changed, and a close with fees each refuse `accounting_unverified`;
  - events exactly at `S.observed_at` are excluded, and events exactly at `risk.observed_at` are included.
- **Calendar:**
  - D, `D_start`, P and `P_close` for a Monday, for the Tuesday after a Monday holiday, for the day after a half day, across both 2026 DST transitions (03-08, 11-01), and across a year boundary;
  - `risk.observed_at` one microsecond either side of Friday → Saturday midnight selects different P;
  - either side of Saturday → Sunday midnight selects the same P.
- **Threshold:** at, just below and just above −limit; a non-breach proceeds to sizing.
- **Executor (DB):**
  - the marked check refuses when realised P&L does not breach;
  - the realised check still refuses when the marked one does not;
  - a non-finite realised total refuses rather than raising;
  - after a marked refusal of either kind, the drawdown state has advanced to the current observation;
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
2. **Snapshot continuity, every UTC date.**
   - 27 of 27 dates have a snapshot.
   - None has a NULL declared count or a count different from its stored marks.
3. **Retained-row price comparison.**
   - 176 marks taken after their session's close; all 176 compared, none missing a bar or instrument metadata.
   - The per-instrument figures are in the Source rule section.
4. **Unit conservation, 26 consecutive snapshot pairs.** The population is endpoint marks plus event positions plus engine ownership claimed in the window.
   - Position-pairs by group: 237 endpoint only, 7 endpoint + event, 6 endpoint + event + ownership, 7 event only.
   - One failure, position 3602947846. Its full history in section 5 shows two events and no ownership row or mark at any time.
5. **Point claims:**
   - the pool limits;
   - weekday after-close snapshot times, 16:14:37 to 19:56:03 New York;
   - the first stored mark date;
   - the failing position's history;
   - the 5-close identity, max |residual| 0.0076;
   - mark timestamps on 252 of 252 marks, 0.07 to 4.2 s old, with 0 non-zero `total_fees`, 0 non-USD and 0 short.

The census is retrospective over retained rows. It does not show that a row existed at each past decision instant. The live gate reads at decision time, and an absent S refuses.

## Security

No authorisation surface changes. This adds:
- a fail-closed entry refusal on the demo paper executor;
- an append-only evidence capture, written by an existing demo job from a snapshot that job already fetches;
- finiteness CHECKs on `trade_events`.
