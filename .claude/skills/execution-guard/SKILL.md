---
name: execution-guard
description: eBull deterministic pre-trade hard-rule gate — evaluate_recommendation in app/services/execution_guard.py, its one-row-per-invocation decision_audit trail, and the fail-closed invariants (no silent bypass, EXIT never blocked) it must preserve.
---

# execution-guard

## When to use

Any change to `app/services/execution_guard.py`, the rule set or thresholds it
enforces, the `decision_audit` / `kill_switch` / `runtime_config` /
`trade_recommendations` tables (sql/001_init.sql, sql/010_execution_guard.sql,
sql/015_runtime_config.sql), the scheduler job that drives it
(`execute_approved_orders`, `app/workers/scheduler.py`), the broker-write
consumer (`app/services/order_client.py::execute_order`), the manual-order path
(`app/api/orders.py`), or the operator-facing audit / rejection endpoints
(`app/api/audit.py`, `app/api/alerts.py`). Also read it before touching any
budget / transaction-cost / safety-layer input the guard reads.

## What it is

`evaluate_recommendation(conn, recommendation_id) -> GuardResult` is the single
hard-rule gate every scheduler-driven trade passes through. It loads the
`trade_recommendations` row, re-evaluates it against **live** DB state, writes
**exactly one** `decision_audit` row (stage `"execution_guard"`), and flips
`trade_recommendations.status` to `'approved'` / `'rejected'` atomically in the
same transaction (`_write_audit`). It returns PASS / FAIL only — **nothing is
sent to eToro here**. Non-executable actions (only `EXECUTABLE_ACTIONS =
{BUY, ADD, HOLD, EXIT}`; `CONSIDERED` #1820 is informational) raise `ValueError`
so a stray call can never be flipped to approved.

Rules (each emits a `RuleResult`; per-rule outcomes stored in `evidence_json`):

- **All actions:** `kill_switch` (missing row → `kill_switch_config_corrupt`,
  fail closed), `runtime_config_corrupt` (singleton missing → fail closed, does
  NOT fall through to defaults — prevention-log #46), `auto_trading`
  (`runtime_config.enable_auto_trading` must be True), `live_trading`
  (`enable_live_trading` must be True).
- **BUY / ADD only:** `safety_layers_enabled` (refuse if `fx_rates` or
  `portfolio_sync` layer is operator-disabled — `is_layer_enabled`), coverage
  (`no_coverage_row` / `coverage_not_tier1` — must be `coverage_tier = 1`),
  thesis (`no_thesis` / `thesis_stale` vs `_FRESHNESS_DAYS = {daily:1, weekly:7,
  monthly:30}` keyed on `coverage.review_frequency`; unknown frequency → stale),
  spread (`spread_unavailable` / `spread_wide` from `quotes.spread_flag`),
  `transaction_cost_prohibitive` (`estimate_cost`; config corrupt → FAIL),
  `budget_available` (`compute_budget_state`; empty/exhausted/`BudgetConfigCorrupt`
  → FAIL; `FxRateUnavailable` → FAIL, #502), and concentration
  (`instrument_missing` / `sector_missing` / `concentration_breach`:
  `current_sector_pct + _MAX_INITIAL_POSITION_PCT (0.05) > _MAX_SECTOR_EXPOSURE_PCT
  (0.25)`).
- **EXIT:** kill-switch + config rules only. Thesis, coverage, spread, cost,
  budget, concentration are **intentionally skipped** — never block a protective
  exit.

Downstream: the `execute_approved_orders` job (`app/workers/scheduler.py`) runs
Phase 1 = guard every `status='proposed'` rec, Phase 2 = execute rows whose
latest `decision_audit` for stage `execution_guard` is `PASS` via
`order_client.execute_order` (which re-asserts safety layers as second-line
defence, `_assert_safety_layers_enabled_for_buy_add`). Manual operator orders do NOT run
the full guard, and the two routes differ: `place_order` (`POST /portfolio/orders`,
BUY/ADD) enforces kill-switch + safety-layers (`is_layer_enabled`, `app/api/orders.py:511`)
+ live-trading and writes `decision_audit` with `STAGE='manual_order'`
(`app/api/orders.py:83,465`); `close_position`
(`POST /portfolio/positions/{position_id}/close`) enforces kill-switch +
live-trading ONLY — safety-layers are intentionally skipped and it writes no
audit row (`app/api/orders.py:596-599`), consistent with EXIT-never-blocked. Live
execution is not yet wired (build priority #7, demo-first): `enable_live_trading=True`
currently returns 501 and demo fills are synthetic.

Operator surfaces: `GET /audit` + `GET /audit/{decision_id}` (evidence_json
detail), `GET /alerts/guard-rejections` (stage `execution_guard` FAILs).

## Invariants (do not break)

- **Fail closed, no silent bypass** (CLAUDE.md non-negotiables; settled-decisions
  "Execution guard semantics"). A missing/corrupt kill_switch, runtime_config,
  budget_config, transaction_cost_config, or FX rate must produce a **FAIL rule**,
  never a defaulted PASS.
- **One `decision_audit` row per `evaluate_recommendation` invocation**, per-rule
  results inside `evidence_json`, status update in the same transaction (settled:
  "Guard auditability"). The scheduler-driven path stays auditable; `close_position`
  is the one manual route that currently writes no audit row (demo-only, pre-live) —
  wire an audit row before live EXIT execution lands.
- **Re-check against current state** — never trust the stored recommendation as
  proof execution is still valid (settled: "Guard re-check rule").
- **EXIT is never blocked** on stale thesis / off-tier coverage / wide spread
  (settled: "Action-specific behaviour"). Long-only v1, no leverage — the guard
  gates entries hard but must not trap a de-risking exit.
- **Unknown cash must FAIL executable BUY / ADD** (settled: "Cash enforcement" +
  "Unknown cash rule"): tolerated in recommendation generation, hard-blocked here.
- `kill_switch` ≠ config flags; `enable_auto_trading` ≠ `enable_live_trading` —
  all three are independent gates (settled: "Kill switch" / "Config controls").
- Deterministic: identical DB state ⇒ identical verdict. No ML, no randomness, no
  external I/O inside any DB transaction.

## Failure conditions

Missing critical source data, timestamps stale beyond the freshness window, or
contradictory / unverifiable state (no coverage row, NULL sector, NULL
`spread_flag`, absent quote, empty `cash_ledger`, unavailable FX rate) must
surface as an **explicit named rule failure** on the audit row — never papered
over with a neutral default or a spurious pass. When a config singleton is
corrupt, emit only the corruption rule; suppress downstream rules that would be
misleading noise (e.g. thesis freshness is skipped when the coverage row is
absent).
