# The capital reader's raise, turned into a refusal verdict (#2979 half b)

Status: proposal, revised after Codex checkpoint 1 (28 findings; three changed the
design). Does NOT close #2979 — it delivers one of that ticket's two acceptance halves.

## The defect

`resolve_engine_capital_usage` refuses the exact-ownership join when an `active`
`strategy_position_ownership` row names a broker position the account snapshot does not
carry (`strategy_engine_capital.py:329`). That refusal is correct and stays — #2602 owns
what may legitimately release ownership, and an ownership row the account cannot witness
must not be marked good.

What is wrong is the SHAPE of the refusal at the callers. `EngineCapitalObservationError`
carries a message and nothing machine-readable, and two callers turn it into an exception
that takes their whole cycle out:

| caller | today | runs |
| --- | --- | --- |
| `strategy_paper_executor._risk_and_amount:907` | `raise StrategyPaperExecutionError` | every `strategy_paper_cycle` tick |
| `strategy_core_executor.execute_core_rebalance:536/563/587` | `raise StrategyCoreExecutionError` → API 409 | operator-attended only |
| `core_rebalance_observation` (scheduler:6335/6373) | raises | every scheduled tick |
| `configure_core_mandate:399` | `raise CoreMandateError` → API 409 | mandate WRITE only |
| `read_core_sleeve:3638` | already a `core_capital_authority_incomplete` blocker | every page load |

### ⚠ The paper arm is the blast radius, not the core arm

`StrategyPaperExecutionError` has **no handler anywhere in `app/`**
(`rg StrategyPaperExecutionError app` returns raise sites only). `execute_fired_paper_signal`
is called in a bare loop (`strategy_paper_runtime.py:518`), so one wedged core ownership
row aborts the entire paper cycle — reconciliation, position management and every alpha
signal evaluation — on that tick and every later one. The wedge is a core-arm fact; the
outage is shared.

### ⚠⚠ The attended core path is not reachable in the wedge state

In the wedge, `load_engine_capital_authority` succeeds and returns non-empty
`core_active_position_ids`. `read_core_sleeve` therefore takes its
`core_live_snapshot_required` branch, leaves `capital_ready = False`, and
`can_rebalance = base_ready and resume_authority is None` is `False`
(`app/api/strategies.py:3662,3835-3838`) — pinned by
`tests/test_2603_core_mandate_api.py:409-449`. `StrategyPortfolioLens.tsx:322` disables
the button on `!sleeve.can_rebalance`.

So a refusal verdict on `execute_core_rebalance` is reachable today only by a direct
`POST /strategies/core-sleeve/rebalance`, not from the UI. It is still worth making —
that endpoint is the engine-facing entry point and the refusal is the contract the
autonomous path (#2843) will read — but this proposal must not claim it fixes the
operator's view. **That the button is disabled for ANY active core position, not just a
wedged one, is a separate defect on the same path and is filed separately.**

## What this proposal does

### 1. A closed refusal vocabulary on the capital reader

`app/services/strategy_engine_capital.py` gains

```python
EngineCapitalRefusal = Literal[
    "engine_capital_ownership_unwitnessed",
    "engine_capital_ownership_mismatched",
    "engine_capital_snapshot_unusable",
    "engine_capital_population_incomplete",
]
```

and `EngineCapitalObservationError` gains a required second constructor argument
`reason_code: EngineCapitalRefusal`, with `super().__init__(message)` so `str(exc)` and
every existing `except` clause are unchanged. A `Literal` so pyright rejects an unlisted
code at every raise site — the device `CoreBrokerPreflightRefusal` uses
(`strategy_core_broker_preflight.py:152`).

⚠ **Declaration order is NOT precedence here, and the spec must not claim it is.** The
preflight's vocabulary can promise that because one function decides in one pass. This
reader raises from 21 sites across three functions in execution order, and
`resolve_engine_capital_usage` iterates positions — so a mismatched position 11 is
reported before an absent position 12. The recorded code is "the first defect reached in
execution order", nothing stronger.

Mapping. Every message stays verbatim; the code is additive.

| code | raise sites | what it asserts |
| --- | --- | --- |
| `engine_capital_ownership_unwitnessed` | `:329` | an active ownership row is not in this snapshot. ⚠ It does NOT assert the close landed — a transient omission or a wrong account view produce the same observation. |
| `engine_capital_ownership_mismatched` | `:331` other instrument, `:333` short, `:335` partially altered, `:338`/`:336` valuation or amount unusable | the snapshot HAS the position and it does not match what we own |
| `engine_capital_snapshot_unusable` | `:318` currency not USD, `:321` duplicate position id, `:325` no configured instrument | the snapshot or mandate config cannot support the join at all |
| `engine_capital_population_incomplete` | every raise in `load_engine_capital_authority` and `_load_realised_delta` | our own DB population is inconsistent |

⚠ `_money` is used for BOTH broker amounts (`:336`) and DB amounts (`:211`, `:281`,
`:285`), so the bucket cannot be inferred from the helper. `_money` takes the code as a
parameter and each call site supplies it.

Four buckets, not one per message: each has a distinct first triage move, and the
message — which carries the trade or position id — is unchanged and still the thing a
reader acts on. Twenty distinct codes with no distinct consequence would be decoration.

### 2. The paper arm returns its refusal instead of aborting the cycle

`_risk_and_amount` already refuses by returning a `str` (`"account_risk_stale"`,
`SANDBOX_EXCEEDED`). The `except EngineCapitalObservationError` arm returns
`exc.reason_code` instead of raising. `execute_fired_paper_signal:1216` routes any `str`
to `_persist_rejection`, which writes `strategy_funding_decisions` (verdict `rejected`,
unconstrained `reason_code` length) and `strategy_entry_preflights` (`reason_code` ≤ 100
chars; the longest new code is 36). **So this refusal is durably stored and queryable,
which the executor's is not.**

⚠ **This is not a weakening.** The refused condition is a property of the shared capital
population, identical for every signal in the cycle, so the remaining signals reach the
same refusal and are rejected too. No signal can be funded while the reader refuses; the
difference is that the cycle's reconciliation and position management now still run, and
the refusal is recorded per signal instead of lost in a job traceback. Pinned by a test
that asserts the returned verdict AND the two durable rows it writes.

### 3. The core executor returns a verdict at its two steady-state sites

- `:535-537` `load_engine_capital_authority` before the broker read → `_result("refused", exc.reason_code)`.
  Inside `core_submission_lock`, no writes have happened.
- `:548-564` the snapshot block → a preceding `except EngineCapitalObservationError`
  returning `_result("refused", exc.reason_code)`; the existing `except Exception` is
  untouched and still raises for a failed broker read and for `CoreSleeveObservationError`.
- `:585-588` the re-proof inside `with conn.transaction()` **keeps raising**. Its
  message ("changed during preflight") carries WHEN the fault arose, after a successful
  first load and a successful broker observation; collapsing it into the same population
  bucket as the first load would discard that. A wedge appearing mid-flight is a
  concurrency fault, not a steady state.

Consequences stated rather than assumed:

- `_result("refused", …)` carries `intent_id=None`, identical to the existing
  `sandbox_exceeded` refusal at `:556`. **No intent row, and no other durable evidence, is
  written** — both new returns precede `record_core_rebalance_intent`. This matches the
  existing refusal and is not claimed as satisfying any durable-evidence rule.
- The response becomes HTTP 200 `CoreRebalanceResponse(state="refused")` instead of a
  409. `StrategyPortfolioLens.tsx:251` already renders
  `Rebalance refused: ${result.reason_code}.`, so no frontend change is needed — but any
  HTTP-status-based monitoring of this endpoint sees one fewer 409.
- `_observe_core_portfolio_drawdown` is still not reached on these paths. That is
  unchanged from the raise behaviour — no new trading is authorised either way — but it
  means the "observe EVERY attended evaluation" comment at `:600` does not cover them.

### 4. Out of scope, stated so nobody assumes otherwise

- **#2979 is not closed.** Its acceptance is two halves — the allocator returns a verdict
  AND the ownership reaches a terminal state. This delivers the first. The ownership row
  stays `active`, the join still refuses, and every later `manage_owned_position` pass
  still returns `owned_position_missing`.
- **`core_rebalance_observation` keeps raising.** With an unwitnessed position the sleeve
  has no market value, so no `CoreSleeveState` exists and `record_core_rebalance_intent`
  has nothing to record; its docstring already refuses to fabricate one. It records a
  `failure` `job_runs` row whose `error_msg` names the position id. ⚠ Its FIRST
  `load_engine_capital_authority` (scheduler:6335) is outside `_tracked_job`, so a
  population refusal there is not recorded as a job failure at all. Noted, not fixed.
- **`assess_core_broker_preflight` keeps mapping this class to
  `core_account_risk_unobservable`** (`strategy_core_broker_preflight.py:363`). It takes a
  SECOND snapshot, so a position that disappears between the two loses the specific code.
  Widening `CoreBrokerPreflightRefusal` is a change to a different closed vocabulary and
  is not bundled.
- **`configure_core_mandate` keeps raising `CoreMandateError`.** It is a mandate WRITE,
  not an allocator cycle. Its 409 does replace the capital message with a generic
  sentence; that is a legibility gap, not this ticket's.

## Source rule

No external rule governs this; it is an in-repo semantics change, and the in-repo
precedents are cited rather than invented:

- the `Literal` closed-vocabulary device (`strategy_core_broker_preflight.py:152-176`) —
  adopted for the type check, explicitly NOT for its precedence property;
- `CoreSleeveObservationError`'s docstring (`strategy_core_sleeve.py:26-35`) for which
  conditions belong on the raising side: INPUT DRIFT raises. An ownership row the account
  cannot witness is not drift in one payload — it is true on this cycle and every later
  one, which is why it belongs on the verdict side;
- the existing `str`-returning refusal channel in `_risk_and_amount` — reuse, not a new
  mechanism.

## Full-population note

Measured on the dev DB at `7905a2eb`: `strategy_position_ownership` active = 0,
`strategy_trades` = 0, `strategy_paper_pool_events` = 0, `strategy_core_mandate_events`
= 0, `orders` = 1. `load_engine_capital_authority` returns `None` today, so **no caller
can reach any of these refusals against the current dev population**. The change is
pre-emptive and is exercised by `tests/test_2525_engine_capital.py` (unit) and
`tests/test_2949_core_close_recovery_db.py` (the lost-close integration scenario). It
must not be described as fixing an observed production state.

## Acceptance

1. `tests/test_2525_engine_capital.py` asserts the reason code at every mapped raise
   site, including both `_money` origins, duplicate snapshot ids, invalid market value,
   the realised-P&L failures and the authority-population failures.
2. Paper arm: with a reader that raises `engine_capital_ownership_unwitnessed`, a fired
   signal returns `verdict="rejected"` carrying that code instead of raising, no broker
   order is placed, and the refusal is readable afterwards from BOTH
   `strategy_funding_decisions` and `strategy_entry_preflights`.
3. Core executor: a first-load refusal and a snapshot-block refusal each return
   `state="refused"` with the code and create no intent, trade, order or broker
   submission; a failing broker read and a `CoreSleeveObservationError` each still raise
   `StrategyCoreExecutionError`; the re-proof site still raises.
4. `tests/test_2949_core_close_recovery_db.py::test_scenario_7b_...` keeps its name and
   its fail-closed assertions (ownership stays `active`, `close_calls == 1`, later passes
   return `owned_position_missing`), replaces the `pytest.raises` block with the refusal
   verdict, and **keeps the position-id evidence** by asserting the reader's own message
   directly rather than losing it to a code-only result.
5. `test_scenario_7a_...` unchanged and still passing.
