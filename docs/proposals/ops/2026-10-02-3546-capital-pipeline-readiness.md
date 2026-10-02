# #3546 — readiness contract for scheduled capital pipelines (gap register P8)

Status: contract + audit (slice 1). Slice 1 also fixes gap A.

## Contract

A scheduled job that can claim, submit or alter capital must have all five properties, each with a test:

| # | Property | Testable form |
|---|---|---|
| R1 | Upstream preconditions before any irreversible claim | Every input the decision reads is checked before the first write that consumes the period/signal. Failure → a named refusal and no claim, with a later fire able to retry inside the period. |
| R2 | Idempotent retry | Re-running a fire for the same period/signal is a no-op or a resume. A DB constraint enforces this, not only a code check. |
| R3 | Partial failure | Each multi-step run persists named intermediate states. A failed item does not stop unrelated items, and does not stop the risk-reducing work (reconcile, protect, halt). |
| R4 | Restart recovery | A kill at any point leaves a state that the next boot or fire resolves, within a stated bound. |
| R5 | Durable submission identity | The order row and its UUID commit before broker I/O. An uncertain result resolves through `orders:lookup` on that UUID, or a re-send with the same UUID. A new key is never used. |

## Source rule

- **R5** follows eToro v2 (`.claude/skills/data-sources/etoro-api.md` §"Automated paper-entry boundary" and §"Order-to-position reconciliation exists in v2"):
  - `X-Request-Id` is the idempotency key, and `referenceId` equals it.
  - Commit the UUID before I/O and never rotate it after an uncertain response.
- **R1** comes from the #3529 precedent: the first fire raced `daily_candle_refresh` and burned a session.
- **R2–R4** have no published rule. They are fixed by construction above.

## Audit (2026-10-02, `origin/main` `a46d8ed3`)

Every file:line below is at that commit.

| Pipeline | R1 | R2 | R3 | R4 | R5 |
|---|---|---|---|---|---|
| `ai_trial_decision_run` / `_fund_` | ✅ bars, declaration and duplicate are checked pre-claim (`ai_trial_jobs.py:377-399`); later refusals are recorded on the run row | ✅ `ai_trial_runs_one_per_session` (sql/432:201) | ✅ publish in one transaction; `run_failed` / `publish_failed` recorded (`ai_trial_run.py:642-649,778-785`) | ✅ lease plus `sweep_stale_claims`; a crash consumes the session by design | n/a (no orders) |
| `ai_trial_execute` | ✅ per-leg refusals persisted (`ai_trial_executor.py`, `ai_trial_intent.py:380-475`) | ✅ `strategy_funding_decisions.signal_id` UNIQUE (sql/281:113) | ✅ per-leg try/except (`ai_trial_jobs.py:489-496`) | ⚠ **gap C** | ✅ request id committed before `place_demo_strategy_order` |
| `ranking_pot_rebalance` | ✅ bar / scores / SPY / drift gates write a refused row (`ranking_pot_rebalance.py:845-885`) | ✅ `ranking_pot_rebalance_attempts_one_per_month` (sql/446:89) | ✅ decided plus executed book commit together; refusals append | ✅ next hourly fire re-derives the month | n/a |
| `ranking_pot_step` | ✅ bar gate refusal, forced after 10 sessions | ✅ PK `(declaration_id, session)` (sql/447:59) | ✅ one transaction per session | ✅ steps every due session; `_looks` repairs | n/a |
| `ranking_pot_execute` | ✅ deferrals write nothing; refusals persisted | ✅ funding-decision UNIQUE plus `ranking_pot_exec_submissions` | ✅ per-entry try/except (`ranking_pot_executor.py:507-516`) | ✅ resumes `submission_uncertain` on the stored UUID | ✅ — ⚠ **gap D** (untested) |
| `strategy_paper_cycle` | ✅ per-signal age gates (`strategy_paper_executor.py:553-575`) | ✅ funding-decision UNIQUE; `fd.signal_id IS NULL` | ❌ **gap A (fixed here)**, ⚠ **gap B** | ✅ `reconcile_backlog` every fire | ✅ |
| `core_rebalance_execution` | ✅ preflight refuses (`strategy_core_preflight.py`); the refusal is persisted on the intent | ✅ `strategy_trades_core_rebalance_intent_id_key` (sql/349:83) | ✅ distinct rejected / uncertain outcomes | ✅ resume does a lookup only, never re-submits (`strategy_core_executor.py:393-413`); `tests/test_2949_core_restart_recovery_db.py` | ✅ |
| `core_rebalance_observation` | — (submits nothing) | none, deliberately: a duplicate is "an extra append" (`scheduler.py:2956-2960`) | single unit | lost-fire rearm | n/a |
| `execute_approved_orders` / `recommendation_order_reconcile` | ✅ guard plus `_assert_submission_controls` | ✅ `idx_orders_recommendation_open_attempt` (sql/375:70) | ✅ per-recommendation connection and try/except | ✅ window A unattended. Window B (`broker_verb_entered` / `uncertain`) is attended by design (#2961) | ✅ |
| `strategy_autonomous_promotion` | ✅ `cycle_precondition_refusal` | ✅ `idx_strategy_promotions_one_*` (sql/281:42) | ✅ one transaction per strategy | next daily fire | n/a |

Stated limits (not gaps):

- `core_rebalance_execution` is daily at 15:37 with no catch-up and no rearm. A refused or missed fire waits for the next session's fire.
- `ranking_pot_rebalance` scores on its own connection before the attempt row. A failure in between leaves a scores run with no attempt; the next fire re-scores.

## Gaps

- **A — fixed in this PR.**
  - **Defect:** `strategy_paper_cycle` refreshed the halt feed before the cycle, and a refresh failure raised out of the job. The cycle's reconciliation, position management and the AI-trial pair lifecycle and loss halts did not run. This holds even though the halt feed gates only new entries and `manage_owned_position` is not blocked even by the kill switch (its docstring).
  - **Measured:** `select left(error_msg,40), count(*) from job_runs where job_name='strategy_paper_cycle' and status='failure' and started_at > now() - interval '30 days' group by 1` (run 2026-10-02) returned 24 failures. Of these, 14 were `Nasdaq halt feed request failed` and 7 were `halt feed publication time regressed`. Seven consecutive fires failed on 2026-09-27 between 02:40 and 03:10Z.
  - **Fix:** a failed refresh withholds entries for that cycle (`run_strategy_paper_cycle(entries=False)`). Everything else runs, and the run is degraded with `errors={"halt_feed_refresh": 1}`. Entries keep exactly their previous fail-closed behaviour.
- **B — slice 2.** `run_strategy_paper_cycle` has no per-item isolation: `manage_owned_position` and `execute_fired_paper_signal` raise out (`strategy_paper_runtime.py:502-535`).
  - One faulting position or signal aborts the rest of that fire.
  - Because the cycle raises, it also skips that fire's AI-trial halts.
  - A signal that raises before authority has no funding decision. It is re-selected every fire, so it head-of-line-blocks the ranked set.
  - Not observed in the 30-day window: the other 3 failures were DB connection losses.
  - Fix: per-item isolation as in `ai_trial_jobs.py:489-496`, plus halts that run whatever the cycle did.
- **C — slice 2.** `ai_trial_execute` never resumes an uncertain leg.
  - `_DUE_LEGS_SQL` excludes any signal that has a funding decision (`ai_trial_jobs.py:431`).
  - `reconcile_backlog` only looks up. A lookup miss stays `not_found` with backoff and is never re-sent on its committed UUID.
  - The paper executor and the ranking-pot executor both re-send on the stored UUID.
  - Latent: `select count(*) from ai_trial_trade_links` = 0 on dev (2026-10-02).
- **D — slice 2.** No test drives `ranking_pot_execute` through an uncertain submission and a re-fire. The shared resume code is tested only through the paper executor (`tests/test_strategy_paper_executor.py`).

## Out of scope

Changing any refusal threshold. Any operator page.
