# Core sell leg, revision 5 — rebalance by whole close + completion buy (#2603)

Status: **revision 5 (2026-09-23): PARKED at Codex ckpt-1. Revision 6 is required (see the Verdict). Not built.** Queue: #2437 Tier A
item 2.

This revision replaces the partial-close model in
`docs/proposals/ta/2026-09-22-core-sell-leg.md` (revisions 1–4). That document stays as the
record of why.

## Why the partial-close model was replaced

Codex ckpt-1 raised 33, 29, 46 and then 38 findings across four rounds. In round 4, 20 of
the 38 were marked SAFETY. The count did not converge, and the findings came from three
structural sources rather than from patchable cases.

**1. Realised P&L of a slice lives under a position id nobody owns.** This was observed on
2026-09-23 (#2965): a partial close books the slice under a NEW `position_id`, keyed back
only by the opening `order_id`. The id is shared by every execution of that entry order
(sql/282). Binding a slice to its operation needed:

- identity fingerprints;
- late-candidate re-checks;
- event-level (not position-level) identity;
- conservation across the position's whole life.

Each fix exposed the next case.

**2. Capped-pot arithmetic makes a partial sell mis-predict in both directions.**
`effective_realised_delta` caps aggregate realised P&L, not each sale's gain. Round 4
#15: pot and basis $100, mark $50, prior realised gains $50, band 75–80%. Selling $10
releases $20 of basis while the bound stays at $100, so the sleeve lands at 66.7%. That is
below `lower`, and the next cycle buys it back.

**3. A partially altered position is a new state for every reader.** The capital reader
(`strategy_engine_capital.py:428`), the #3312 release, the manager's resume and the
emergency close each needed a new rule for it.

The #2965 precedent applies here: when a key keeps drawing findings, question the model,
not the case.

## Model

A `sell_core` decision is executed as two ordinary, already-witnessed operations.

1. **Rebalance close.** The manager closes the owned core position **whole**, through
   `manage_owned_position(close_reason="core_rebalance")`. That is the existing
   `_submit_close` → `_resume_operation` close branch → `_finish_close` path, which
   releases ownership on landing. The broker writes the close row under the **owned**
   id, so `_load_realised_delta` counts it with today's join. No new capital rule is
   needed.
2. **Completion buy.** On a later attended `POST /core-sleeve/rebalance`, the sleeve has
   0% core, and the existing buy arm buys. That buy is sized to the completing sell
   decision's `upper` edge rather than `lower` (§3). The end state is therefore the one
   the allocator decided: the near edge of the band (Leland 2000, cited at
   `strategy_core_allocator.py:232-236`).

### Premises (measured 2026-09-23)

- **Cost.** The what-if for `SPY.RTH` quotes a close cost of $0.01 on a $1,000 ticket,
  against $0.03 for the open. The quote spread is 0.26 bps. Source: `scripts/probe_2712_close_side_cost_quote --all-held`,
  run from this worktree. A whole close plus a rebuy of the current sleeve (core
  $229.11, cash $274.96, from the 2026-09-22 intent) costs about $0.02 against about
  $0.001 for an ideal trim. That is negligible at this venue.
  - ⚠ The ratio is not general: QQQ's close arm is 1.38 per $1,000, and GME's is 50. So
    the close is quoted and charged (§2), never assumed.
- **Tax.** UK same-day rule (TCGA 1992 s105(1)(b); `tax-ledger` skill). A disposal matched
  to a same-day reacquisition realises only (sell − rebuy price) on the matched units, and
  the unmatched units come off the s104 pool. When the completion buy is on the same UK
  day, the taxable outcome is that of trimming the unmatched units. On a later day, the
  30-day rule (s106A(5)) matches the same way.
- **Whole close reuses the manager's existing close path.** That is `_submit_close`, the
  close branch of `_resume_operation`, and `_finish_close`, which the operator-close
  endpoint already drives. No new resolution code is needed.
- **One caller.** `execute_core_rebalance` is reached only from the attended endpoint. The
  close leg is the operator-close endpoint's own verb (`app/api/strategies.py:3467`,
  `close_reason="operator_close"`) under a different trigger code.

## Design

### 1. Executor: `sell_core` → rebalance close

In `execute_core_rebalance`:

1. **Under `core_submission_lock`:** run the existing DB preflight. Evaluate. If the
   result is `sell_core`, record the intent as today, then **commit and release the core
   lock**.
2. **Select the position.** Exactly one `active` core ownership must exist, else refuse
   `core_sell_spans_positions`. Multi-position sleeves stay refused.
3. **Call the manager.** `manage_owned_position(conn, broker=…,
   strategy_trade_id=…, broker_position_id=…, close_reason="core_rebalance")`. The manager
   takes `_paper_allocator_lock` → `_position_lock` itself, as it does for the operator
   close.

   ⚠ No lock is nested: the core lock is released before the manager's locks are taken.
   So no lock-order proof is needed. The one thing the core lock protected, "no second
   core submission races this one", is carried by the §4 quarantine instead.
4. **Map the manager result into the executor result.**

   | manager result | executor result |
   | --- | --- |
   | `submitted` / `pending` | `submitted` |
   | `applied` | `closed` |
   | `rejected` | `refused` (the manager's reason code) |
   | `reconcile_required` | `reconcile_required` |

   The intent stays as recorded. The intent row is a decision record, not an order
   state, as it already is for buys.

**Which refusals apply** (C1-33, r2-25):

- `sandbox_exceeded` and the drawdown refusal do not block a `sell_core`, because a close
  only reduces exposure. The drawdown observation is still recorded.
- A disabled mandate or disabled pot still refuses, because the rebalance is an allocator
  action.
- Emergency de-risking keeps its own path (`emergency_risk` / `operator_close`).

**Freshness.** The manager does not re-evaluate the band. If the sleeve drifts back inside
the band between the decision and the close, the close still runs, and the completion
buy restores the upper-edge end state. The result is a net round trip at the §2-charged
cost, with no exposure increase. This is stated, not prevented.

### 2. Close-side cost — quoted, charged, no new threshold

After the evaluation and before step 3, the executor's broker preflight runs the close-arm
what-if:

`BrokerWhatIfOrder(action="close", transaction="sell", position_ids=(pid,), amount=<position market value>)`

It decodes the result with `decode_quoted_trade_cost`.

- The close proceeds only when that decode succeeds and yields `gamma < 1`. This is the
  sizer's existing `cost_rate_implausible` rule.
- The decoded cost is logged with the intent id.
- `core_close_side_cost_quote_unavailable` now means that decode failed. It no longer
  means "any `sell_core`".

The cost is **charged, not thresholded**:

- The close's cost is realised inside the close row's `realized_pnl_usd`, which the
  capital reader already counts.
- The completion buy is sized on the post-close sleeve, from a fresh open-arm quote, by
  the existing `resolve_core_trade_size`. Its `at_zero` / `at_bound` bracket
  (`cost_breaches_far_edge`) keeps the completed weight inside the band after costs.

No new constant is introduced. A round-trip-cost ceiling would be an invented threshold
(`CLAUDE.md`, "source-rule before design"), and none is published.

The magnitude is a property of the mandate's chosen instrument. It was measured for
SPY.RTH above. A mandate on an instrument with a dear close arm (GME, 50 per $1,000)
pays that cost, and the cost is visible on the intent.

### 3. Completion buy — to the completing decision's `upper` edge

`evaluate_core_rebalance` gains one keyword, `completing: CoreRebalanceCompletion | None`.
When it is given and the sleeve is below `lower`, the decision is `buy_core` with
`edge = completing.upper_pct` in place of `lower_pct`. `resolve_core_trade_size` already
takes the edge from the decision, so `edge = lower if buying` becomes "the decision's
edge".

The executor builds `completing` from the DB under `core_submission_lock`. It is
non-`None` iff all of these hold:

- the most recent core intent with a non-`hold` action is a `sell_core`;
- that intent's rebalance close operation (`trigger_code='core_rebalance'`, linked by the
  new `core_rebalance_intent_id` on the operation, §5) is `applied`;
- no core buy order has been linked since.

Anything else means `completing = None` and today's behaviour. Two cases follow from
this:

- A completion therefore happens **at most once** per sell. After it, the most recent
  non-hold intent is the buy.
- If the close is not yet applied, or its history row has not been ingested so capital
  refuses `engine_capital_population_incomplete`, the buy is refused by the existing
  gates, and the operator re-POSTs later.

Until the completion lands, the sleeve holds cash. This is stated. For a beta sleeve that
is tracking error, not a capital-safety risk.

### 4. Quarantine — core buys while a core close is unresolved

The core buy DB preflight gains `core_operation_outstanding`. It refuses when any
core-owned ownership has either:

- an operation in (`intent_persisted`, `submitting`, `submitted`); or
- a `close` in `reconcile_required`.

This closes the race the released core lock opened: a buy submitted while the rebalance
close is in flight. `idx_strategy_position_one_unresolved_operation` (sql/377) already
stops a second close on the same ownership.

### 5. Migration `sql/411_core_rebalance_close.sql`

- `strategy_position_operations_trigger_code_check` gains `'core_rebalance'` (sql/292's
  explicit name).
- A new nullable `core_rebalance_intent_id BIGINT` FK to
  `strategy_core_rebalance_intents`, with a CHECK:
  `(trigger_code = 'core_rebalance') = (core_rebalance_intent_id IS NOT NULL)`.
  Existing rows have other triggers and NULL, so they satisfy the CHECK.
- A UNIQUE partial index on `core_rebalance_intent_id` WHERE NOT NULL, so one close per
  sell intent.
- Nothing else changes: the operation type (`close`), statuses, the resolution shape, the
  unresolved index (sql/377) and the material-identity index (sql/406) are all reused
  as they are.

The manager's `close_reason` Literal gains `"core_rebalance"`. `_submit_close` receives the
intent id and writes it.

### 6. What does NOT change

- the capital reader and `_load_realised_delta`;
- the #3312 release;
- `_resume_operation`;
- the SL/TP repair arm;
- `trade_events`;
- the provider (whole close is the existing `close_demo_strategy_position`);
- the snapshot parse.

Partially altered positions stay refused everywhere, as today.

## Tests

- **Pure, table-driven:**
  - `evaluate_core_rebalance` with and without `completing`, covering edge choice and the
    already-inside-band case;
  - the `completing` predicate over intent/operation/order histories.
- **DB, one per new SQL mechanism:**
  - the migration CHECK and unique index;
  - the executor `sell_core` → manager close path with a fake broker, ending in operation
    `applied`, ownership `released` and the intent linked;
  - the quarantine refusal.

## Attended acceptance (loop-ineligible)

One demo session.

1. Tighten the band so SPY.RTH is above `upper`, then POST. Record:
   - the close ack;
   - the close-order detail;
   - operation `applied`;
   - ownership `released`;
   - the history close row under the owned id with `realized_pnl_usd`.
2. After the next sync, POST again. Record:
   - the completion `buy_core` at `upper` edge;
   - the fill;
   - the new ownership;
   - SL/TP set by #3284's entry levels;
   - the resulting core weight.

Outcomes:

- **Pass** = the weight lands within [`lower`, `upper`] and at or below `upper`, and no
  second completion is admitted on a third POST.
- **Safe fail** = any refusal above, or a `reconcile_required` on the close, with no core
  buy admitted while it stands.

## Delivery

One PR: migration, manager trigger, executor branch, close-quote preflight, completion
keyword, quarantine, and tests. The acceptance is recorded as pending on #2603.

## Verdict — Codex ckpt-1 on revision 5 (2026-09-23): PARKED, revision 6 required

Codex raised 46 findings, cited `r5-n`, and classified in the main thread. They fall into
three groups.

**A. Upper-edge completion does not work (r5-1 to 6, 17 to 21).** The sizer solves a buy
to `lower` so that its zero-cost bracket lands inside the band. Solving to `upper` puts
the zero-cost outcome above `upper`, so `cost_breaches_far_edge` refuses every
completion with positive cost (r5-1: $100 cash, 80%, 1% cost). A historical edge can also
disagree with the current mandate, or reverse the action (r5-3, 4). The "most recent
intent" predicate loses completion on a refused retry or on a second sell (r5-17 to 21).

**Revision 6: drop the completion keyword.** After the close, the unchanged allocator buys
to the current `lower` edge, which is its own published rule applied to fresh state. The
consequence is stated rather than engineered away: a breach above `upper` ends at `lower`,
not `upper`. That is extra turnover at the measured SPY.RTH close cost, and nothing else.
Proposed revision-6 changes:

- r5-1 to 6 and 17 to 21: removed with the keyword.
- r5-44: fixed in place. 0.26 bps is the quote spread, not the close cost.
- r5-45: correct the tax premise. The 30-day window is bounded, and trim equivalence
  holds only when the rebuy happens inside it.

**B. The handoff after releasing the core lock (r5-7 to 10, 12, 16, 24, 25).** Between
the committed sell intent and the manager's operation insert, a concurrent POST can buy,
the position can change, or the mandate can be disabled.

**Revision 6: hold `core_submission_lock` across the manager call.** The lock order is
core → allocator → position. It needs the bounded audit that r4-26 asked for:

- the four allocator-lock sites (`app/api/config.py:258`, `app/api/strategies.py:3600`,
  `app/workers/scheduler.py:6763`, `strategy_position_manager._paper_allocator_lock`)
  must never reach `core_submission_lock`;
- `manage_owned_position` gains a re-check, under its locks, that the mandate and pot are
  enabled and the ownership is the one bound to the intent, for trigger `core_rebalance`
  only;
- the close quote must be taken under those locks and bound to the position id and
  units it priced.

Also in revision 6:

- r5-11: a close in `reconcile_required` must block a new `core_rebalance` close (the
  unique index excludes that status);
- r5-12, 13: the executor maps a manager result only when the operation returned is the
  one linked to this intent. Anything else → `reconcile_required`;
- r5-14, 15: `manage_owned_position` drives resolution on the next attended POST, and the
  paper runtime already resumes owned positions;
- r5-39 to 42: the FK becomes a trigger-checked link to a `sell_core` intent on the same
  instrument. `sql/292` is not the current trigger vocabulary (`fixed_exit_repair` is
  written by the manager), so the migration must read the live constraint rather than
  sql/292;
- r5-46: split acceptance, and add handoff/race tests.

**C. Capital-reader properties that apply to EVERY whole close today (r5-29 to 38).**
These cover:

- the completeness of `realized_pnl_usd`;
- a released-without-full-history ownership;
- late adjustments;
- epoch rollover;
- a released id still present in the snapshot;
- the capped-mode fee absorption.

None of them is introduced by this design: the operator close and every broker SL/TP
close (#3284, #3312) already take the same path. They are recorded here and on the PR,
per the loop rule against opening audit tickets. Revision 6 must not claim to fix them.
It must not rely on them being false either.
