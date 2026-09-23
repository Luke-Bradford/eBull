# Core sell leg, revision 11 — rebalance by whole close + allocator rebuy (#2603)

Status: **revision 11 (2026-09-23): Codex ckpt-1 CONVERGED — ready to build.** Revision 5's Verdict and the
revision-6 to 10 classifications are kept at the end as the record of what each revision answers. Queue: #2437 Tier A item 2.

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
2. **Rebuy.** On a later attended `POST /core-sleeve/rebalance`, the sleeve has 0% core,
   and the unchanged allocator buys to the current `lower` edge (§3). Revision 5's
   upper-edge completion is dropped (Verdict A).

### Premises (measured 2026-09-23)

- **Cost.** The what-if for `SPY.RTH` quotes a close cost of $0.01 on a $1,000 ticket,
  against $0.03 for the open. (0.26 bps is the quote SPREAD, not the close cost —
  r5-44.) Source: `scripts/probe_2712_close_side_cost_quote --all-held`,
  run from this worktree. A whole close plus a rebuy of the current sleeve (core
  $229.11, cash $274.96, from the 2026-09-22 intent) costs about $0.02 against about
  $0.001 for an ideal trim. That is negligible at this venue.
  - ⚠ The ratio is not general: QQQ's close arm is 1.38 per $1,000, and GME's is 50. So
    the close is quoted and charged (§2), never assumed.
- **Tax.** UK same-day rule (TCGA 1992 s105(1)(b); `tax-ledger` skill). A disposal matched
  to a same-day reacquisition realises only (sell − rebuy price) on the matched units, and
  the unmatched units come off the s104 pool. When the completion buy is on the same UK
  day, the taxable outcome is that of trimming the unmatched units. On a later day, the
  30-day rule (s106A(5)) matches the disposal to a reacquisition within the **30 days
  after** it, the same way. ⚠ The window is bounded (r5-45): a rebuy on day 31 or later
  is not matched, the whole close is a disposal against the s104 pool, and trim
  equivalence does not hold. Rebuy timing is the operator's POST, so the spec claims
  equivalence only inside the window.
- **Whole close reuses the manager's existing close path.** That is `_submit_close`, the
  close branch of `_resume_operation`, and `_finish_close`, which the operator-close
  endpoint already drives. No new resolution code is needed.
- **One caller.** `execute_core_rebalance` is reached only from the attended endpoint. The
  close leg is the operator-close endpoint's own verb (`app/api/strategies.py:3467`,
  `close_reason="operator_close"`) under a different trigger code.

## Design (revision 11)

### 0. Lock order — the declared one, not a new one

Revision 5 released `core_submission_lock` before calling the manager, and the Verdict
asked for the reverse: hold it across the call "in the order core → allocator →
position". That order is inverted relative to the code. The repo already declares ONE
advisory order for every writer of a strategy order, its executions and its ownership
(`app/services/strategy_order_reconciliation.py:8-17`, #2964 item 3):

```
PAPER_ALLOCATOR -> CORE_MANDATE -> CORE_SUBMISSION -> _position_lock -> reconciliation_order_lock
```

`core_submission_lock` takes the first three in that order
(`strategy_core_submission_gate.py:267`). `manage_owned_position` takes
`_paper_allocator_lock` then `_position_lock` (`strategy_position_manager.py:1328`).
Called inside `core_submission_lock`, the manager's allocator acquire is a **re-entry**:
PostgreSQL advisory locks are reentrant per session and reference-counted. Measured on
the dev cluster 2026-09-23: `pg_advisory_lock(k)` then `pg_try_advisory_lock(k)` →
`true`; three `pg_advisory_unlock(k)` → `true, true, false`. So the manager's release
decrements to one, and `core_submission_lock`'s own unlock-ownership assertion
(`strategy_core_submission_gate.py:279-286`) still sees the key. The sequence is
allocator → mandate → core → (allocator, re-entered) → position, which is the declared
order with no new edge.

The audit r4-26 asked for is therefore an audit for **inversions of the declared order**,
not of four sites:

- Every other `PAPER_ALLOCATOR_ADVISORY_LOCK` site (`app/api/config.py:258`,
  `app/api/strategies.py:3600`, `app/workers/scheduler.py:6763`,
  `strategy_core_mandate.py:370`, `strategy_control_plane.py:338`,
  `strategy_autonomous_promotion.py:213`, `strategy_paper_executor.py:190`) takes the
  allocator FIRST. Waiting behind our hold is a queue, not a cycle.
- The standalone `CORE_SUBMISSION_ADVISORY_LOCK` xact sites (`ops_monitor.py:1132,1204`
  kill switch; `credential_health.py:292`; `app/api/broker_credentials.py:924`) take the
  core key and then a ROW lock (`kill_switch`, `broker_credentials`). None of them then
  waits on the allocator or a position lock. They queue behind our hold — see the
  kill-switch note in §1.
- `_core_submission_try_lock` (`strategy_order_reconciliation.py:775`) never blocks.

- `_core_submission_try_lock` (`strategy_order_reconciliation.py:775`) is used only by
  `terminalise_unsubmitted_core_entry` (`:856`) and the window-B release
  (`strategy_core_window_b_release.py:484`). Both act on an ENTRY order in
  `authority_committed`. Its documented exemption ("an outer holder is not a submitter
  in flight", `:778-783`) is about core ENTRY submissions; this hold submits a CLOSE and
  no entry (r6-37). The implementation PR adds a test that the manager's close path
  reaches neither `terminalise_unsubmitted_core_entry` nor the window-B release
  (r8-22), so the exemption stays true by test,
  not by reading.

The implementation PR adds one test that holds `core_submission_lock`, calls
`manage_owned_position` on a fake broker, and asserts on exit that the session holds no
advisory lock (`pg_locks` for `pg_backend_pid()` is empty). That is the mechanical form
of "re-entry is balanced" on the success path.

⚠ **Not claimed** (r6-34, 35): `core_submission_lock` acquires its three keys before its
`try`, and its release comprehension stops at the first SQL exception. Both are
properties of every core buy today, unchanged here. The manager's own
`_paper_allocator_lock` raises if its unlock returns false, so a lost inner reference is
loud, not silent.

### 1. Executor: `sell_core` → rebalance close, inside ONE hold

In `execute_core_rebalance`, everything below happens inside the existing single
`with core_submission_lock(conn):` block (`strategy_core_executor.py:678`):

0. **Resolve first** (r7-1, 2, 3, 18; r8-1, 2, 4, 5; r9-2, 3, 4, 6). This runs at the
   very start of `execute_core_rebalance`, in its own `core_submission_lock` hold,
   BEFORE the preliminary mandate/eligibility block (`strategy_core_executor.py:656-672`)
   — so a disabled mandate or a changed proof cannot block resolving what was already
   sent. In one committed read transaction it selects the operation with
   `trigger_code = 'core_rebalance'` in (`intent_persisted`, `submitting`,
   `submitted`). **Only that trigger**: repairs and operator/emergency closes are the
   paper runtime's and the operator endpoint's to drive, and the step-3 precondition
   refuses a sell while one is unresolved (r9-2 to 4, 6 — revision 9's widening to
   every trigger is withdrawn). If one exists, the executor calls
   `manage_owned_position(close_reason=None, expected_ownership_id=<its ownership>)` —
   the paper runtime's call shape, which resumes it — and returns the step-6 mapping for
   that operation and its linked intent id. No evaluation, no new intent, no quote.
   At most one exists: one close per intent (unique index) and a sell requires no
   unresolved operation on any core ownership (step 3). `_load_owned` requires an
   `active` ownership (`strategy_position_manager.py:383`); an unresolved operation on
   a released ownership is unreachable by construction (`_finish_close` terminalises
   then releases in one transaction; the #3312 release runs only when nothing resumed),
   and if seen step 0 returns `reconcile_required`, `core_operation_unloadable`
   (r8-3). An operation created between step 0's hold and the main hold (r9-1, r10-1)
   — a repair, an operator close, or another POST's `core_rebalance` close — makes
   step 3 refuse `core_operation_outstanding`. That is a safe refusal: the POST that
   created the operation, the paper runtime, or the next POST's step 0 drives it.
1. **Sandbox order** (r6-17). Today `usage.headroom.within_bound` is tested before
   evaluation and returns `sandbox_exceeded`. It moves to after
   `evaluate_core_rebalance` and refuses only a `buy_core`. A close reduces exposure;
   refusing it while over the bound would pin the sleeve over the bound. Over the bound
   `usage.headroom.remaining` is negative, so the observation's
   `assigned_cash_available` becomes `min(snapshot.available_cash,
   max(0, usage.headroom.remaining))` — only the headroom term is clamped; a negative
   broker `available_cash` still flows through unchanged (r7-24). Zero core and zero
   cash is the allocator's existing zero-denominator `hold`
   (`strategy_core_allocator.py:266-268`), which is a REFUSAL, `core_sleeve_empty`,
   not a hold (r7-25, r8-12). ⚠ Not claimed (r7-26): the rebalancer
   does not cure a sandbox breach. With `upper = 100` an over-bound sleeve can hold;
   a breach is #2844's refusal surface and #2843's alert, not an allocator action. The drawdown
   refusal is treated like the sandbox one; its observation is still recorded.
2. **Evaluate, re-prove, observe** exactly as today: the in-hold re-proof of mandate
   event id and capital authority (`strategy_core_executor.py:741-762`), the drawdown
   observation (`:773-778`) and the eligibility/provenance re-validation
   (`:795-811`, r10-3) run for a `sell_core` BEFORE any step-3/4 refusal can return.
   On the sell path the `with conn.transaction()` block opened at `:743` ENDS here
   (committed); steps 3 to 5 each run their own top-level transaction, never nested
   in it, so none is a savepoint (r10-4). Thus so a refusal is never recorded from stale preliminary inputs and never
   skips an equity-peak observation (r9-11, 12). The intent INSERT is deferred to step
   4 on the sell path.
3. **`sell_core` preconditions, before any broker call.** The reads run in their own
   `with conn.transaction()` and commit before anything else (r8-9). A refusal here
   or in step 4 INSERTs the intent with that refusal's `reason_code` and NULL quote
   fields in its own committed transaction, then returns (r8-8) — so the refusal is
   recorded before the hold's rollback-on-exit can touch it.
   - exactly one `active` core ownership → else `core_sell_spans_positions`;
   - no operation on ANY core ownership is unresolved or a `reconcile_required` close
     (step 0 has already driven the unresolved kind; this catches what it could not)
     → else `core_operation_outstanding` (r8-14);
   - every `entry`-purpose order on ANY core trade (r8-13), not only the selected one,
     has
     `strategy_order_reconciliation_state.state` in `_TERMINAL_RECONCILIATION_STATES`
     (`{"resolved", "rejected"}`, `strategy_order_reconciliation.py:126`) → else
     `core_entry_not_terminal` (r6-9, r7-16). ⚠ This bounds late fills only as far as
     "resolved" means all executions are materialised, which is that module's contract,
     not a new claim;
   - `lower_pct > 0` → else `core_sell_would_strand_at_zero_lower` (r6-19: with
     `lower = 0` the post-close 0% is in-band, and the allocator would never rebuy);
   - `rebalance_band_pct > 0` (so `lower < upper`, `strategy_core_allocator.py:272-273`)
     → else `core_sell_zero_width_band` (r8-11: a zero-width band leaves the cost
     bracket no room for a positive-cost rebuy);
   - (checked in step 4, AFTER the quote — r9-14) the lower-edge rebuy from the
     post-close state is at least the allocator's own effective floor —
     `max(mandate.min_rebalance_amount, broker_minimum)` with `broker_minimum =
     proof.min_position_amount`, computed by the same code
     (`strategy_core_allocator.py:278-285`, r9-13) — and meets the proof's
     `min_position_exposure` → else `core_rebuy_below_minimum` (r6-20, r7-23, r8-10).
     Post-close state = core 0, cash = `min(snapshot.available_cash,
     usage.headroom.remaining) + core market value − quoted close cost` — the
     UNCLAMPED headroom, so an over-bound sleeve's deficit is netted (r10-2: headroom
     −$80, core $100 → $20 assignable, not $100). Worked case: $100 core, band 10–20%, floor
     $50 → the $10 rebuy is below the floor, so the sell refuses instead of stranding
     the sleeve in cash. Cost and rounding
     can still refuse the rebuy later; that refusal holds cash, which is safe.
4. **Close quote** (§2), bound to the position selected in step 3 (r6-12). ⚠ For a
   `sell_core` the quote is taken BEFORE the intent INSERT, and the intent row carries
   it (r7-7) with its `quoted_at`, so the append-only intent is written once,
   complete. The what-if call raising or timing out, a decode failure, or a response
   currency different from `proof.response_currency` (the check the buy preflight
   already makes, `:738`) all refuse `core_close_side_cost_quote_unavailable`
   (r10-8, 9). A decoded `gamma >= 1` refuses `core_close_cost_implausible` (r9-16).
   The rebuy-minimum check (step 3's last bullet) runs here, on the net figure.

   **Order on the sell path (r10-5):** quote → rebuy-minimum → final evidence-age
   check (step 5) → intent INSERT with the quote (committed) → manager call. EVERY
   refusal happens before the INSERT and records the intent with NULL quote fields;
   after the INSERT nothing can refuse except the manager itself. So an intent with
   quote fields is, by construction, one that was handed to the manager. For a
   `sell_core` this REPLACES `assess_core_broker_preflight`'s trim-sized buy/sell
   preflight: the executed ticket is the whole position, so the allocator's trim
   `amount` is recorded on the intent as the decision and the quote's amount is the
   ticket (r6-13). The sell branch still supplies the common verdict fields the buy
   path consumes (`strategy_core_executor.py:821-839`): account equity and snapshot
   timestamp from the in-hold snapshot, and the existing maximum evidence age (r7-8).
5. **Commit; re-check evidence age; call the manager — still inside the hold.** The
   `sell_core` branch sits after the common re-proof and BEFORE the order-shape gate,
   entry-stop derivation and new-trade insert (`strategy_core_executor.py:844-900`),
   none of which apply to a close (r6-18). The branch exits the `with
   conn.transaction()` block (`:743`) before committing, so the connection is idle
   for the manager (r7-6). The evidence-age check the buy path runs
   (`:824-831`) is repeated immediately before the call (r6-16), and applied to the
   close quote's `quoted_at` too, with the same maximum age (r9-15). Then
   `manage_owned_position(conn, broker=…, strategy_trade_id=…, broker_position_id=…,
   close_reason="core_rebalance", core_rebalance_intent_id=<intent>,
   expected_ownership_id=<ownership selected in step 3>)`. The connection is
   idle (the manager requires it); session advisory locks survive the commit.

   If the ownership has an unresolved operation (`intent_persisted`/`submitting`/
   `submitted`), the manager's `_resume_operation` (`strategy_position_manager.py:1335`)
   drives it FIRST and returns its result without closing. That is intended: a POST
   during an in-flight operation drives it (r6-1, r5-14/15).
6. **Map the result through the linked operation** (r5-12/13, r6-4/5/7). The executor
   re-reads the operation whose `core_rebalance_intent_id` is this intent.

   | after the call | executor result |
   | --- | --- |
   | no linked row, manager status `reconcile_required` (e.g. `owned_position_missing`) | `reconcile_required` + manager reason |
   | no linked row, manager `applied` with the #3312 `RELEASE_REASON` (broker had already closed it whole) | `refused`, `core_position_closed_by_broker` (r8-17) |
   | no linked row, any other manager status | `refused`, `core_rebalance_close_not_started` + manager reason |
   | linked row, and `result.operation_id` ≠ its id | `reconcile_required`, `core_rebalance_result_mismatch` |
   | linked `submitted` | `submitted` |
   | linked `applied` | `closed` |
   | linked `rejected` | `refused`, the operation's `last_error_code` |
   | linked `reconcile_required`, `submitting` or `intent_persisted` | `reconcile_required` |

   `submitting`/`intent_persisted` cannot be the state after a normal return (the manager
   moves both to a terminal or `submitted` state); seeing one means the call raised
   midway, which is uncertain and therefore `reconcile_required`.

   **Exceptions** (r6-6, r7-4, 5). The manager call is wrapped: on any exception the
   executor (on a fresh idle connection state) looks up the operation linked to this
   intent. Linked row exists → map it by the step-6 table's linked-row rows (the
   `result.operation_id` comparison is skipped: a raise returns no result, r9-5) (a row already `applied` or `rejected` maps to `closed` / `refused`; only
   an intermediate state becomes `reconcile_required`), with the exception class as
   detail (r8-6); no linked row → re-raise as `StrategyCoreExecutionError` (nothing reached
   the broker for THIS intent; a `_load_owned` failure lands here). A committed
   operation row stands; §4 blocks a buy while it is unresolved; step 0 drives it on
   the next POST, and the paper runtime (`strategy_paper_runtime.py:493`) on its cycle.

The intent row is a decision record, not an order state, as for buys.

**Why no re-check inside the manager** (Verdict B bullet 2). The re-check the Verdict
asked for existed only because revision 5 released the core lock. With the hold kept,
the mandate cannot be revised (`configure_core_mandate` needs the allocator key, held),
the pot cannot be toggled (every enable site needs the allocator key, held), and a
second core submission cannot start (core key, held), and the kill switch cannot be
flipped (activation needs the core key, `ops_monitor.py:1132`), so the preflight's
kill-switch verdict holds for the whole hold. Ownership of the selected position can
still change — a broker SL/TP fill, or the #3312 release — but only inside the
manager's `_position_lock`, and the manager's own `_load_owned` re-reads it under that
lock. Whatever it finds, step 6 maps a result only when the operation linked to
THIS intent exists, so a changed ownership surfaces as
`core_rebalance_close_not_started`, never as a mis-attributed `closed`. The manager is
therefore unchanged apart from the new `close_reason` value, the intent-id
pass-through, and one guard (r6-10, r7-10, 11, 12): `manage_owned_position` raises
unless `close_reason == "core_rebalance"` ⇔ `core_rebalance_intent_id is not None`;
`expected_ownership_id` is REQUIRED for `core_rebalance` and PERMITTED otherwise
(step 0 passes it with `close_reason=None`, r8-4); and for `core_rebalance` it requires, BEFORE
taking its own locks, that the session holds all three `core_submission_lock` keys
(`core_lock_held` on allocator, mandate and core). A caller holding only the core key
would wait on the allocator while a real holder waits on core — a cycle (r7-11); all
three keys means the manager's allocator acquire is always a re-entry. Under its locks
it requires `owned.is_core` (for `core_rebalance`) and, whenever given,
`owned.ownership_id == expected_ownership_id`, else `rejected`,
`core_rebalance_ownership_mismatch`. ⚠ `core_lock_held` reads `pg_locks` and cannot
tell a session lock from a transaction lock (r9-7). Only `core_submission_lock` takes
all three keys; every xact-lock site takes one (§0). A caller holding all three as
transaction locks does not exist, and one that did would lose them at the manager's
first commit — stated as the guard's limit, not closed by it. `_load_owned` loads by `(strategy_trade_id,
broker_position_id)`, and the executor passes the quoted position id as
`broker_position_id`, so the ownership, the quote and the close name one position
(r8-15). This is an accident control against
a confused caller, as `unattended_guard` is; it does not authorise one.

**Credential provenance** (r6-11). The close is linked to the existing trade, like the
operator close. This POST's eligibility proof is loaded and matched to the broker's
credential ids before the hold (`strategy_core_executor.py:655-671`), and a credential
rotation needs the core key (`broker_credentials.py:924`), so it cannot land inside
the hold. The proof is not persisted on the close; that matches the operator close and
is stated, not added. A rotation to a DIFFERENT account (r7-13) is outside the demo
single-account setup; there the old position id is absent from the new account's
snapshot, the manager's `_exact_broker_position` returns none, and the result is
`reconcile_required`, `owned_position_missing`, or the #3312 release row above.
⚠ Not claimed (r8-16): that position ids never collide across eToro accounts. The
setup has one demo account; binding the account identity to the ownership is a
multi-account concern, out of scope.

**Which refusals apply** (C1-33, r2-25):

- `sandbox_exceeded` and the drawdown refusal do not block a `sell_core` (step 1).
- A disabled mandate or pot still refuses: the rebalance is an allocator action.
- **The kill switch still refuses a `sell_core`.** The core preflight's
  `core_kill_switch_active_or_missing` (`strategy_core_preflight.py:533`) is unchanged.
  The manager's own kill-switch exemption is for de-risking; a rebalance is not
  de-risking, and emergency closes keep their own verbs (`emergency_risk`,
  `operator_close`). Fail-closed default.
- ⚠ Kill-switch activation takes `CORE_SUBMISSION_ADVISORY_LOCK`, so it queues behind
  the hold (r6-36, r8-21). The hold now also covers the manager's exact-position read,
  eligibility read and one close submission — each a broker HTTP call bounded by the
  provider's request timeout. Its `_position_lock` acquire cannot wait: the manager is
  the only taker of that key (`_position_lock` is defined and used only in
  `strategy_position_manager.py:113, 1328`), always after the allocator key, which this
  hold owns. No new bound is invented.

**Freshness.** Evaluation, quote and close submission all happen inside one hold, and
the evidence-age check is repeated right before the manager call (step 5).

### 2. Close-side cost — quoted, charged, no new threshold

Inside the hold, after evaluation and before step 5, the broker preflight runs the
close-arm what-if:

`BrokerWhatIfOrder(action="close", transaction="sell", position_ids=(pid,), amount=<position market value>)`

It decodes the result with `decode_quoted_trade_cost`.

- The close proceeds only when that decode succeeds and yields `gamma < 1` (the sizer's
  existing `cost_rate_implausible` rule).
- The decoded cost is recorded against the intent **with the position id and units it
  priced** (r5-44). The close is whole (`close_demo_strategy_position` takes no units).
  A broker-side change to the position between the in-hold snapshot and the close
  (seconds) is not refused by the close: closing whatever remains only reduces
  exposure, and its realised cost lands in the close row either way (r6-14, 15). The
  "partially altered positions stay refused" rule (§6) is the capital reader's, and it
  still applies on the NEXT POST, before any rebuy. A position that disappears between
  the manager's exact read and the submit (an SL/TP fill, r7-14) takes the existing
  `_submit_close` broker-error arms: `rejected` or `reconcile_required`. A whole close
  ends that position's exposure at zero whatever its units were (r7-15); only the
  realised cost differs from the quote.
- `core_close_side_cost_quote_unavailable` now means the decode failed. It no longer
  means "any `sell_core`".

The cost is **charged, not thresholded**. It is realised inside the close row's
`realized_pnl_usd`, which the capital reader already counts. No round-trip ceiling is
introduced: none is published, and inventing one is the defect `CLAUDE.md`
"source-rule before design" names. At SPY.RTH the what-if quotes $0.01 close / $0.03
open per $1,000 ticket (§Premises). The 0.26 bps figure in revision 5 was the quote
spread, not the close cost (r5-44).

### 3. Rebuy — the unchanged allocator, to `lower`

Revision 5's `completing` keyword is **dropped** (r5-1 to 6, 17 to 21). After the close
applies and ownership is released, the next attended POST sees 0% core and the
unchanged allocator buys to the current mandate's `lower` edge — its own published rule
(Leland 2000, `strategy_core_allocator.py:232-236`) applied to fresh state, with the
existing `cost_breaches_far_edge` bracket.

Consequence, stated: a breach above `upper` ends at `lower`, not `upper`. Turnover is
NOT one band-width (r9-20): with core C over `upper`, a trim trades C − upper·S, while
close + rebuy trades C + lower·S (S = sleeve value). Worked case: $100 core, sleeve
$100, band 60–80% — trim $20, close + rebuy $160, so $140 extra. At SPY.RTH's measured
$0.01 close / $0.03 open per $1,000 that is ≈ $0.006 on this example, and it is
charged, not assumed (§2). Until the rebuy the sleeve
holds cash: tracking error for a beta sleeve, not a capital-safety risk.

### 4. Quarantine — no core BUY while a core close is unresolved

The core buy DB preflight gains `core_operation_outstanding`: it refuses when any
operation on ANY core ownership — active or released (r6-8) — is in
(`intent_persisted`, `submitting`, `submitted`) or is a `close` in
`reconcile_required`. Inside one hold a buy cannot race the close's
submission, but the close can remain `submitted` after the hold ends (fill not yet
witnessed), and a `reconcile_required` close can stand indefinitely. Neither may be
followed by a buy until it resolves.

### 5. Migration `sql/411_core_rebalance_close.sql`

Built from the **live** constraint, read from the dev cluster 2026-09-23 with
`pg_get_constraintdef`, not from sql/292:

```
strategy_position_operations_trigger_code_check:
  trigger_code IN ('entry_exit_gap','causal_resistance_break','timeout',
                   'strategy_exit','emergency_risk','operator_close')
```

(Revision 5's Verdict said sql/292 lacks a manager-written `fixed_exit_repair` trigger.
`fixed_exit_repair` is an `operation_type`; the repair trigger is `entry_exit_gap`, and
the live set above already contains it. The live set is the source either way.)

- DROP and re-ADD `strategy_position_operations_trigger_code_check` with that set plus
  `'core_rebalance'`. The repo history agrees with the live set: only sql/289 (create)
  and sql/292 (re-add) touch this constraint, and dev is the only deployment. The
  migration first takes `LOCK TABLE strategy_position_operations IN ACCESS EXCLUSIVE
  MODE` (r7-30), then asserts the live definition (a `DO` block comparing
  `pg_get_constraintdef` to the exact text captured from the dev cluster on
  2026-09-23 — `CHECK ((trigger_code = ANY (ARRAY['entry_exit_gap'::text,
  'causal_resistance_break'::text, 'timeout'::text, 'strategy_exit'::text,
  'emergency_risk'::text, 'operator_close'::text])))` — raising on mismatch, r8-20), then drops
  and re-adds, all in the migration's one transaction. An exact-text comparison can
  refuse a semantically equal definition on another PostgreSQL version (r7-31); that
  refusal is fail-closed and is the intended direction.
- New nullable `core_rebalance_intent_id BIGINT REFERENCES
  strategy_core_rebalance_intents`, with CHECK
  `(trigger_code = 'core_rebalance') = (core_rebalance_intent_id IS NOT NULL)` and CHECK
  `trigger_code <> 'core_rebalance' OR operation_type = 'close'` (r6-29).
- UNIQUE partial index on `core_rebalance_intent_id` WHERE NOT NULL.
- `BEFORE INSERT` trigger (r6-28, r9-17, 18, r10-6): raises unless the linked intent
  has `action = 'sell_core'`, all four quote fields NOT NULL, a `close_quote_position_id` equal to the
  ownership's `broker_position_id` (so a refused sell intent, which has NULL quote
  fields, can never be linked, and the link names the quoted position), its
  `core_instrument_id` equals the ownership's instrument, and the ownership belongs to
  a core trade.
- `BEFORE UPDATE` trigger on `strategy_position_operations` (r6-30, 32, r7-27): raises
  if `core_rebalance_intent_id`, `ownership_id`, `trigger_code`, `operation_type`,
  `order_id` or `request_id` change. `BEFORE DELETE` trigger: raises for a row with
  `core_rebalance_intent_id` NOT NULL (r7-29; no writer deletes operations today —
  `grep "DELETE FROM strategy_position_operations"` over `app/` and `sql/`: 0 hits).
  The link is write-once and permanent, so "one close per sell intent" holds over time.
- Ownership parents (r7-28): `strategy_position_ownership`'s `strategy_trade_id` and
  instrument are written at insert; the three UPDATE sites
  (`strategy_control_plane.py:1336`, `strategy_position_manager.py:450, 500`) write
  status/release fields only. A `BEFORE UPDATE` trigger on
  `strategy_position_ownership` raises if `strategy_trade_id`, `broker_position_id`
  or the instrument change (r8-18), making that a constraint. And a `BEFORE UPDATE`
  trigger on `strategy_trades` raises if `core_rebalance_intent_id` or `instrument_id`
  changes (r10-7: `_load_owned` reads the trade's instrument,
  `strategy_position_manager.py:319`) — that is
  the column `_load_owned` derives `is_core` from (`strategy_position_manager.py:384`),
  so the core classification validated at insert cannot move (r9-19).
- `BEFORE UPDATE OR DELETE` trigger on `strategy_core_rebalance_intents` (r6-31,
  r8-19): raises on any update or delete. The same migration adds the nullable quote
  columns (`close_quote_position_id`, `close_quote_units`, `close_quote_cost`,
  `close_quoted_at`) the sell path writes at INSERT, with a CHECK that the four are
  all NULL or all NOT NULL.
- Lock order inside the migration (r9-10): one `LOCK TABLE
  strategy_core_rebalance_intents, strategy_trades, strategy_position_ownership,
  strategy_position_operations IN ACCESS EXCLUSIVE MODE` at the top. A live buy
  transaction that holds one of them can still produce a deadlock; PostgreSQL detects
  it and aborts one side. If it aborts the migration, the migration fails loudly and is
  re-run. Nothing is left half-applied, because the migration is one transaction. No writer updates an intent today (`grep "UPDATE strategy_core_rebalance_intents"`
  over `app/` and `sql/`: 0 hits), so this makes the existing append-only practice a
  constraint.

The manager's `close_reason` Literal (`strategy_position_manager.py:1316`) and
`_submit_close`'s `trigger_code` Literal gain `"core_rebalance"`; `_submit_close`
writes the intent id. Nothing else in the operation shape changes: type `close`,
statuses, resolution shape, sql/377 and sql/406 indexes are reused.

### 6. What does NOT change

- the capital reader and `_load_realised_delta`;
- the #3312 release;
- `_resume_operation`;
- the SL/TP repair arm;
- `trade_events`;
- the provider (whole close is the existing `close_demo_strategy_position`);
- the snapshot parse;
- the allocator and sizer.

Partially altered positions stay refused everywhere, as today.

**Not claimed** (Verdict group C, r5-29 to 38). The capital-reader properties raised
there — completeness of `realized_pnl_usd`, a release without full history, late
adjustments, epoch rollover, a released id still in the snapshot, capped-mode fee
absorption — apply to every whole close today (operator close, broker SL/TP, #3312).
This design neither fixes them nor depends on them being false: if the reader refuses
after the close, the rebuy is refused by the existing gate and the sleeve holds cash.
Where the reader does NOT refuse (r6-22 to 27: silently incomplete fees, a late
negative adjustment after the rebuy, epoch rollover, capped-mode fee absorption), the
rebuy inherits the same exposure any rebuy after an operator or SL/TP close inherits
today. Those are properties of `_load_realised_delta` and belong to its own ticket;
this spec records them and does not claim them.

**Known limitation** (r6-21). A rebuy that fills as more than one position makes the
next `sell_core` refuse `core_sell_spans_positions` — safe (the sleeve stays over the
band, no action), and visible on the intent.

## Tests

- **Pure, table-driven:** the step-6 mapping (every row of the table); the sandbox
  reorder and the `max(0, …)` observation clamp (`sell_core` admitted over bound,
  `buy_core` refused); the step-3 preconditions.
- **DB, one per new SQL mechanism:**
  - migration: both CHECKs, the unique index, the insert trigger (rejects a `buy_core`
    intent, a mismatched instrument, a non-core ownership), the two update triggers,
    and the pre-drop definition assert;
  - executor `sell_core` → manager with a fake broker: operation `submitted`, then a
    SECOND POST through `execute_core_rebalance` (step 0) → `applied`, ownership
    `released`, result carries the original intent id (r6-3, r7-32); no advisory lock
    held by the session afterwards (§0), including when the manager raises;
  - independent sessions (r7-32, r8-23): session B's `execute_core_rebalance` blocks on
    `core_submission_lock` while session A holds it across the close; after A's close
    lands `submitted`, B's step 0 drives it (no evaluation); after A's close is
    `reconcile_required`, B's buy refuses `core_operation_outstanding`; the manager guard refuses a caller
    holding only the core key (r7-11);
  - races and boundaries (r5-46, r6-2, r6-38): an unresolved repair operation → the
    manager resumes it → `core_rebalance_close_not_started`, no close row; ownership
    released before `_load_owned` → `StrategyCoreExecutionError` (no linked row, r8-7); missing position → `reconcile_required`;
    a `reconcile_required` close (ownership released) refuses both `sell_core` and
    `buy_core`; broker close raising `BrokerPositionMutationUncertain` →
    `reconcile_required` and the buy quarantine holds; an exception raised after
    `mark_close_submitting` → the operation stays `submitting` and the buy quarantine
    holds; the manager guard refuses `core_rebalance` without the core key.

## Attended acceptance (loop-ineligible) — split

**A. Close leg (one POST, then one manager visit).** Tighten the band so SPY.RTH is
above `upper`, then POST: record the close ack and operation `submitted`. After the next
manager visit (paper runtime cycle, or a second POST): record the close-order detail,
operation `applied`, ownership `released`, and the history close row under the owned id
with `realized_pnl_usd`. Pass = all of these, and the quoted close cost is recorded on
the intent.

**B. Rebuy (after the next sync, one POST).** Record the `buy_core` to `lower`, the
fill, the new ownership, SL/TP set by #3284's entry levels, and the resulting weight.
Pass = the weight lands in [`lower`, `upper`].

**Safe fail** for either = any refusal above, or a `reconcile_required` close with no
core buy admitted while it stands.

## Delivery

One PR: migration, manager trigger value + pass-through, executor branch inside the
hold, close-quote preflight, sandbox reorder, quarantine, tests. Acceptance A and B are
recorded as pending on #2603.

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

## Codex ckpt-1 on revision 6 (2026-09-23): 38 findings, classified → revision 7

Revision 6 held `core_submission_lock` across the manager call. §0's lock-order finding
stands: the Verdict's "core → allocator" order was inverted relative to
`strategy_order_reconciliation.py:8-17`, and the manager's allocator acquire is a
measured re-entry. Codex raised no lock-order cycle.

| findings | class | revision 7 |
| --- | --- | --- |
| r6-1, 2, 3, 4, 5, 6, 7 | real: step ordering and result mapping | step 5 lets the manager resume an in-flight operation; the step-6 table covers every case; exceptions and `submitting` are `reconcile_required`; acceptance A and the tests expect `submitted` then `applied` on the next visit |
| r6-8, 9, 12, 13, 16, 17, 18, 19, 20 | real: missing preconditions and data flow | §4 covers released ownerships; step 3 adds entry-terminal, `lower > 0` and rebuy-minimum checks; step 4 orders the quote after selection and replaces the trim preflight; step 5 repeats the evidence-age check; step 1 clamps observation cash |
| r6-10, 11 | real / stated | manager guard (`core_rebalance` needs the intent id, `is_core` and the held core key); provenance stated |
| r6-14, 15 | contradiction | §2 now says what happens to a broker-side change inside the hold |
| r6-28 to 33 | real: link integrity | close-only CHECK, core-ownership insert trigger, write-once operation link, append-only intents, pre-drop definition assert |
| r6-36, 37 | stated | kill-switch wait bounded by the provider timeouts; the try-lock exemption concerns entries, with a test |
| r6-21 | limitation | stated in §6 |
| r6-22 to 27, 34, 35 | pre-existing, not claimed | properties of `_load_realised_delta` and `core_submission_lock` that every whole close and every core buy has today |
| r6-38 | real | test list extended |

## Codex ckpt-1 on revision 7 (2026-09-23): 32 findings, classified → revision 8

| findings | class | revision 8 |
| --- | --- | --- |
| r7-1, 2, 3, 18 | real: POST recovery re-evaluated and minted a new intent | step 0 resolve-first: drives an in-flight `core_rebalance` operation with no evaluation and maps it to ITS intent |
| r7-4, 5 | real | exception wrapper, keyed on the linked operation |
| r7-6, 7, 8 | real: data flow | exit the transaction block; quote before the intent insert; common verdict fields supplied |
| r7-10, 11, 12 | real: guard allowed a lock cycle | manager requires all three `core_submission_lock` keys and an expected ownership id |
| r7-13, 14, 15 | stated | wrong-account and vanished-position outcomes; whole close ends exposure regardless of units |
| r7-16, 23 | real | exact terminal predicate; `min_position_exposure` added |
| r7-24, 25, 26 | real / stated | clamp only headroom; zero-denominator hold cited; sandbox breach is not the rebalancer's to cure |
| r7-27, 29, 30, 31 | real | `order_id`/`request_id` frozen, delete refused, table lock around the assert, exact-text refusal stated fail-closed |
| r7-28 | stated | ownership parents are not rewritten; confirmed by test |
| r7-32 | real | recovery test goes through the executor; two-session and guard tests |
| r7-9 | residual | evidence can age during the manager's own reads; the close only reduces exposure and the manager reads the live position itself |
| r7-17, 19, 20, 21, 22 | pre-existing, not claimed | other close verbs on an uncertain position, the runtime loop's lack of per-position isolation, unbounded advisory waits and the lock helpers' cleanup shape are properties of every manager caller today |

## Codex ckpt-1 on revision 8 (2026-09-23): 23 findings, classified → revision 9

| findings | class | revision 9 |
| --- | --- | --- |
| r8-1, 2, 4, 5, 14 | real: step 0 placement and scope | step 0 runs before the mandate/proof block, in its own committed read, over any trigger, and may pass `expected_ownership_id` |
| r8-3 | stated | unresolved-on-released is unreachable by construction; if seen, `reconcile_required` |
| r8-6, 7 | real / contradiction | exception path maps by the table; test aligned to the re-raise |
| r8-8, 9 | real | refusal inserts the intent in its own committed transaction; step-3 reads commit first |
| r8-10, 11, 12, 13 | real | allocator floor in the rebuy check; zero-width band refused; `core_sleeve_empty` is a refusal; entry-terminal over all core trades |
| r8-15, 17 | stated | one position named by quote, `_load_owned` key and close; #3312 release row mapped |
| r8-16 | not claimed | cross-account id collision is a multi-account concern |
| r8-18, 19, 20, 22, 23 | real | ownership immutability trigger; intents delete-proof; exact constraint text; both try-lock callers tested; two-session test aligned to step 0 |
| r8-21 | stated | `_position_lock` has one taker, always after the held allocator key |

## Codex ckpt-1 on revision 9 (2026-09-23): 23 findings, classified → revision 10

| findings | class | revision 10 |
| --- | --- | --- |
| r9-2, 3, 4, 6 | real, introduced by revision 9's widening of step 0 | step 0 narrowed back to `core_rebalance`; other triggers are refused at step 3 and driven by their own callers |
| r9-1 | stated | an operation created between holds is a repair or operator close → safe refusal |
| r9-5 | real | exception path skips the id comparison |
| r9-7 | stated | `core_lock_held` cannot tell xact from session; the guard's limit is recorded |
| r9-10 | real | one multi-table `LOCK TABLE` at the top; a deadlock aborts the whole migration |
| r9-11, 12 | real | preconditions run after the common re-proof and drawdown observation |
| r9-13, 14, 15, 16 | real | broker minimum = `proof.min_position_amount`; rebuy check on net post-close cash after the quote; quote age checked; `core_close_cost_implausible` |
| r9-17, 18, 19 | real | insert trigger binds the quoted position and requires quote fields; `strategy_trades.core_rebalance_intent_id` frozen |
| r9-20 | real (wrong arithmetic) | turnover corrected with a worked case |
| r9-8, 9, 21, 22, 23 | pre-existing, not claimed | lock-helper cleanup, blocking SQL inside any core hold, the runtime loop's isolation |

## Codex ckpt-1 on revision 10 (2026-09-23): 9 findings → revision 11. Converged.

Finding counts per round: 46 (r5), 38 (r6), 32 (r7), 23 (r8), 23 (r9), 9 (r10). Round
10 raised no new model-level finding, and did not contest any pre-existing
classification. All nine were precision fixes, applied in revision 11:

- r10-1: the between-holds case now includes another POST's close, and the refusal is safe;
- r10-2: rebuy projection uses unclamped headroom;
- r10-3: eligibility re-validation runs before the preconditions;
- r10-4: transaction boundaries stated, no savepoints;
- r10-5: every refusal precedes the intent INSERT;
- r10-6: all four quote fields required, all-or-none CHECK;
- r10-7: the trade's `instrument_id` is frozen;
- r10-8, 9: a what-if raise and a currency mismatch are both `quote_unavailable`.

Revision 11 is the build spec.
