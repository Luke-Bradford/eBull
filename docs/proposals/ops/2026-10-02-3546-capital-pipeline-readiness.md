# #3546 — readiness contract for scheduled capital pipelines (gap register P8)

Status: contract + audit (slice 1). Slice 1 also fixes gap A; gaps B–H are later slices.

## Contract

**Applies to** any scheduled job that claims, submits or alters capital. R5 applies only to jobs that mutate
broker state. Every property needs a named test, and a cell without one is marked ⚠.

| # | Property | Testable form |
|---|---|---|
| R1 | Readiness before consumption | Each input is classified **transient** or **terminal**. A transient input (data not landed, broker or account-risk read unavailable, feed stale) defers: no write consumes the period or signal, and a later fire inside the period retries. A terminal verdict (policy, mandate, declaration, sizing) may consume, as a named refusal. Deliberate exceptions are written down with their reason: forced stepping after N sessions; one-shot experiment sessions. |
| R2 | Idempotent retry | A re-fire for the same unit (period, signal, intent) is a no-op or a resume. A DB constraint enforces this, not only a code check. Aggregate capital conservation across pipelines is #2844's sandbox invariant, not this one. |
| R3 | Partial failure | Named intermediate states are persisted. A failed item does not stop unrelated items. Nothing stops the risk-reducing work: reconcile, protective repair, halts. An entry-only input failing withholds entries only. |
| R4 | Restart recovery | After a kill at any point, the next boot or fire reaches the unit within a stated bound and resolves or contains it. Containment (blocks new authority, waits for attended resolution) is acceptable only when stated as such. |
| R5 | Durable submission identity | The order row and its UUID commit before broker I/O, bound to account, environment and payload. An uncertain result is resolved on that UUID and never under a new key. A re-send creates exposure, so it must re-pass the entry's permission checks first. |

## Source rule

- **R5:** eToro v2 `X-Request-Id` is the documented idempotency key, and `referenceId` equals it (`.claude/skills/data-sources/etoro-api.md` §"Automated paper-entry boundary").
  - ⚠ A lookup miss is NOT proof of non-placement. On the 2026-09-17 demo probe, an order that filled echoed our `referenceId`, yet `orders:lookup?referenceId=` returned 404. Recorded in `strategy_order_reconciliation.py::terminalise_unsubmitted_core_entry`'s docstring (#2961).
  - Non-placement is proved only by our own write ordering: the submission marker commits before the provider call. A re-send on the same UUID relies on the documented idempotency.
- **R1:** the #3529 precedent, where a session was burned by a data race.
- **R2–R4:** no published rule; fixed by construction above.

## Audit (2026-10-02, at `a46d8ed3`)

| Pipeline | R1 | R2 | R3 | R4 | R5 |
|---|---|---|---|---|---|
| `ai_trial_decision_run` / `_fund_` | ⚠ **E**: bars are checked pre-claim (`ai_trial_jobs.py:390-399`); account risk, declaration validity, step 1 and pack are checked after `claim_run` (`ai_trial_run.py:664-715`), so a transient account-risk outage consumes the session | ✅ `ai_trial_runs_one_per_session` (sql/432:201) | ✅ publish in one transaction; `run_failed` / `publish_failed` recorded | ⚠ a crashed `claimed` row is swept only by a later run that reaches the sweep; same-session fires return `duplicate` first (`ai_trial_jobs.py:383`). Session consumed either way | n/a |
| `ai_trial_execute` | ⚠ **F** | ✅ `strategy_funding_decisions.signal_id` UNIQUE (sql/281:113) | ✅ per-leg try/except (`ai_trial_jobs.py:489-496`) | ⚠ **C** | ✅ entry |
| `ranking_pot_execute` | ⚠ **F** (deferrals write nothing; other refusals consume) | ✅ funding-decision UNIQUE plus `ranking_pot_exec_submissions` | ✅ per-entry try/except (`ranking_pot_executor.py:507-516`) | ⚠ **C** | ✅ entry |
| `strategy_paper_cycle` | ⚠ **F** | ✅ funding-decision UNIQUE | ❌ **A (fixed)**, ⚠ **B** | ⚠ **C** (orders reached by `reconcile_backlog` every fire, lookup only) | ✅ entry; ⚠ **G** edits/closes |
| `ranking_pot_rebalance` | ✅ gates in `prepare` (`ranking_pot_rebalance.py:845-885`) are persisted as refused rows by the job (`ranking_pot_job.py:204-206`) | ✅ `..._one_per_month` over decided/skipped (sql/446:89); refusals append by design | ✅ decided plus book in one transaction | ✅ next hourly fire | n/a |
| `ranking_pot_step` | ✅ bar gate; forced after 10 sessions (stated exception) | ✅ PK (sql/447:59) | ✅ one transaction per session | ✅ | n/a |
| `core_rebalance_execution` | ✅ preflight refusals persist on the intent; daily, no catch-up (stated: waits a session) | ✅ `strategy_trades_core_rebalance_intent_id_key` (sql/349:83) | ✅ distinct rejected / uncertain outcomes | contained: a lookup miss blocks new authority until attended release (`strategy_core_executor.py:393+`, `tests/test_2949_core_restart_recovery_db.py`); unbounded | ✅ |
| `core_rebalance_observation` | — submits nothing | none, deliberately (`scheduler.py:2956-2960`) | single unit | lost-fire rearm | n/a |
| `execute_approved_orders` / `_reconcile` | ✅ guard plus `_assert_submission_controls` | ✅ `idx_orders_recommendation_open_attempt` (sql/375:70) | ✅ per recommendation | window A unattended; window B contained, attended (#2961), unbounded | ✅ |
| `strategy_autonomous_promotion` | ✅ `cycle_precondition_refusal` | ⚠ **H**: unique per transition, not per fire; a re-fire may advance a further stage | ⚠ **H**: only `_Refused` / `_AuthorityRevoked` are caught | next daily fire | n/a |

## Gaps

- **A — fixed in this PR.** `strategy_paper_cycle` refreshed the halt feed before the cycle, and a refresh failure raised out of the job. As a result, reconciliation, position management, the AI-trial pair lifecycle and the loss halts all skipped that fire.
  - The halt feed is an input to entries only. `manage_owned_position` does not read it, and is not blocked even by the kill switch (its docstring).
  - Measured with `select left(error_msg,40), count(*) from job_runs where job_name='strategy_paper_cycle' and status='failure' and started_at > now() - interval '30 days' group by 1` (run 2026-10-02): 24 failures, of which 14 were `Nasdaq halt feed request failed` and 7 were `halt feed publication time regressed`. Seven consecutive fires failed on 2026-09-27 between 02:40 and 03:10Z.
  - Fix: a failed refresh now runs the cycle with `entries=False`, which skips ranking and execution and nothing else. The run is degraded with `errors={"halt_feed_refresh": 1}`. Entry behaviour is unchanged.
  - Not addressed: a reconciliation or health failure inside the cycle still stops management and halts. That is gap B.
- **B — `strategy_paper_cycle` isolation.** `run_strategy_paper_cycle` raises out of reconcile, health, management or execution (`strategy_paper_runtime.py:487-535`). Any of these skips the rest of the fire, including the AI-trial halts.
  - The fix has four parts:
    - per-item isolation;
    - halts on a path independent of the cycle's connection, so they run whatever the cycle did;
    - a backoff for a signal that raises before authority, which today is re-selected every fire and can hold the bounded top-5;
    - management verdicts in the run note, since `managed` today counts visits, not protections.
  - Position rotation is clock-slot based (`:494-498`). Its fairness under skipped fires belongs here too.
  - **Slice 2 (B) fixed:** every stage and item is contained and counted on the run's errors axis; a failed reconciliation or health refresh withholds entries only; the AI-trial pair events and halts run on their own connection after the cycle, whatever it did; `managed=` carries per-state verdicts; each item gets a fresh instant unless the caller pinned one.
  - **Not built — backoff for a signal raising pre-authority.** No population: `select count(*) from strategy_opportunity_ranking_members where selected` = 0 and the only enabled paper deployments are the two AI-trial legs (dev, 2026-10-02). With containment, such a signal holds one of the five slots until its forecast's `valid_through`; it no longer skips the others. Wake: the first selected ranking member.
  - **Rotation fairness not live:** `_OWNED_BATCH_SQL` returns 4 positions against `position_limit=5` (dev, 2026-10-02), so every fire visits all of them.
- **C — scheduled resume of uncertain entries.** All three entry executors (paper, AI trial, ranking pot) have a resume branch that re-sends on the stored UUID. None of their schedulers selects funded signals (`strategy_paper_runtime.py:165`, `ai_trial_jobs.py:431`, `ranking_pot_executor.py:417-424`), so a scheduled fire never reaches an uncertain submission. `reconcile_backlog` only looks up, and per the source rule a miss proves nothing.
  - Before a scheduled re-send is added, it must re-pass the entry's permission checks: halt, session, declaration and trial state, authority expiry. It must also be withheld whenever entries are.
  - Latent: `select count(*) from ai_trial_trade_links` = 0, and `strategy_order_reconciliation_state` holds 5 rows, all `resolved` (dev, 2026-10-02). Unexercised is not low-consequence: this is the identity path every engine order takes.
- **D — tests.** No test drives a scheduled fire through an uncertain submission for any of the three executors. This is folded into C.
- **E — AI-trial decision consumes on transient inputs.** `account_risk_unavailable` and the other post-claim refusals burn the session.
  - ⚠ `ai_trial_run.py` is in a frozen declaration's hashed `POLICY_MODULES`. The fix ships only under a new trial version, never by editing v1's hashed files.
- **F — transient refusals are terminal funding decisions.** Executors persist `decide_funding(verdict='rejected')` for transient causes such as unavailable account risk, eligibility or costs. Their selectors then exclude the signal for good. R1's transient/terminal split has to be applied per refusal code.
- **G — R5 for edits and closes.** Position edits use operation records, not `X-Request-Id` lookup (`strategy_position_manager.py`, etoro-api skill §position boundary). This needs its own assessment.
- **H — promotion fire idempotency and isolation**, per the audit row above.

Additional limit: within a cycle, freshness checks use the cycle's start instant (`strategy_paper_runtime.py:486`), so a slow cycle evaluates entries against an older clock. It belongs with B.

## Codex ckpt-1 (32 findings) — dispositions

- **Applied:**
  - #1, #4: A's scope is stated.
  - #5, #6, #26, #28: folded into B.
  - #7, #10: E, and the decision R4 cell.
  - #8, #9: F, and R1 rewritten as transient vs terminal.
  - #11–#13: C covers all three executors.
  - #14: the re-send permission rule in R5 and C.
  - #15: core marked "contained, unbounded".
  - #16: source rule records the lookup-404 probe.
  - #17: G.
  - #18, #19: H.
  - #20: B's clock note.
  - #21: rebalance cell evidence fixed.
  - #22: stated exceptions in R1.
  - #23: R2 points aggregate capital at #2844.
  - #24: R5 binding.
  - #25: R4 split into reach, and resolve-or-contain.
  - #29: applicability line and ⚠ for untested.
  - #30: a test that management runs with `entries=False`.
  - #31: C is no longer called low-priority.
- **Rebutted:**
  - #2, #3 (withhold stop loosening and TP moves on a failed halt refresh): the halt feed is not an input to position management. Its loosening and TP repair run on every normal fire whatever the feed's state. Withholding them only when the feed fetch fails would couple two unrelated controls. The old abort withheld them by accident, not by design.
  - #27: the refresh is one bounded HTTP call ahead of the cycle, and exits do not wait on it beyond its timeout. The allocator-lock contention it names is a property of the executor, not of this change.

## Out of scope

Changing any refusal threshold. Any operator page.
