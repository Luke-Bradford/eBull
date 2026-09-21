# Core sleeve enablement — from the #2833 verdict to the first engine-placed demo position

Written 2026-09-14, unattended, against `7697c6a5`, and corrected against a Codex
checkpoint-1 pass that returned 40 findings on the first draft. Every path, constant,
field and refusal code below was read from the source, not recalled. Nothing here was
executed: the session it describes is operator-attended by rule (`.autonomy/hard_rules.md`
— *"never execute/approve/simulate a trade, never POST order endpoints, never touch the
kill-switch"*), and this document exists so that session is a checklist rather than a
derivation.

**Covers:** the demo core/cash sleeve (#2603) placing its first engine-originated order,
once #2833's selection verdict is written. The R6 Tier-3 goal.
**Does not cover:** live capital. See §3 — and note there is no core-live promotion route
in the code today, so "and then it goes live" is not a step this document can hand off to.

---

## 0. Preconditions

### 0a. Run it against the MAIN checkout

`app/security/unattended_guard.py::refuse_broker_mutation_if_unattended` refuses every
mutating broker call from a **linked git worktree**, and it deliberately does not try to
tell the autonomy loop from an attended operator in one — its own docstring keeps the
over-refusal on purpose and names the fix: *"run it from the main checkout"*. The dev API
(`:8000`) and vite (`:5173`) already serve `~/Dev/eBull`, so this is satisfied by using
the normal dev stack and violated by pointing at a worktree's API.

⚠ The selection verdict is **three Python constants**, so the serving process must be
running the code that carries them. Editing them in a branch that the API has not loaded
changes nothing the endpoint reports.

### 0b. A PASSING verifier result, not merely an opened window

```python
# app/services/strategy_core_selection.py
SELECTED_CORE_OUTCOME: Final[CoreSelectionOutcome | None] = None   # "pass" | "cash"
SELECTED_CORE_INSTRUMENT_ID: Final[int | None] = None
SELECTED_CORE_EVIDENCE_REF: Final[str | None] = None
CORE_SELECTION_REQUIRED_TRADING_DAYS: Final = 5
```

Transcribe **the outcome the verifier printed**, not an inference from it (#3037):

| verifier `outcome` | `SELECTED_CORE_OUTCOME` | `SELECTED_CORE_INSTRUMENT_ID` | resulting `state` |
| --- | --- | --- | --- |
| `pass` | `"pass"` | the selected id | `ready` (or `unavailable` if its venue is not session-checkable) |
| `cash` | `"cash"` | **leave `None`** | `cash` |

`SELECTED_CORE_EVIDENCE_REF` is non-blank in both cases — a cash verdict keeps its
pointer to the study that produced it.

⚠ Leaving all three `None` after the window closes is **not** neutral: the endpoint then
reports `state: "awaiting_verdict"`, which is the correct "openable, not yet transcribed"
state and blocks enablement. Setting the outcome without the matching instrument id (or
vice versa) reports `unavailable` with a `configuration_error` naming the specific fault.

`state: "ready"` requires that the outcome is `"pass"`, that `SELECTED_CORE_INSTRUMENT_ID`
is one of the declared candidates, that a coverage row exists for it, and that
`SELECTED_CORE_EVIDENCE_REF` is non-blank. **It does not re-check the five dates, the
elapsed boundary, the cost bar or the contents of the evidence ref.** Setting the constants
is the act of *recording* a verdict; it is not proof of one.

The verdict itself comes from `scripts/verify_2833_core_selection.py`, which checks
population completeness, spreads, FX, real/long/x1 eligibility and the 60-bps bar — and
**may select cash**, i.e. "no candidate qualifies" is a legitimate outcome that leaves
this runbook unused. ⚠ Cash does not mean "everything was too expensive": a candidate can
be cheap and still fail on `incomplete_population`, `fx_unmodelled` or
`not_proved_real_long_x1`.

⚠ `earliest_possible_verdict_at` is a **lower bound** computed from observed dates and the
current clock, modelling weekends only. It is not a promise that the window closes then.

⚠ Progress is the **intersection** of observation dates across candidates
(`_COVERAGE_SQL`'s `common_dates` CTE), not the smallest per-candidate count: every
candidate can hold five observations while the common count is below five.

⚠ **A verdict naming an LSE candidate is refused at declaration time.**
`SESSION_SUPPORTED_ASSET_CLASSES` is `frozenset({"us_equity"})` and the check reads the
venue's joined `exchanges.asset_class`, so the refusal is a property of the venue
metadata, not of an instrument id. The endpoint surfaces it as a blocker with
`code="core_selection_invalid"` and the detail naming the symbol. Refusing here moves the
failure off the attended session, which is the most expensive moment available.

### 0c. A current eligibility proof for the selected instrument

`configure_core_mandate` and the executor both require a proof that the instrument is the
**underlying product, not a CFD**, for the same operator / provider / environment and the
**current credential pair**, no older than 24 hours. `core_eligibility_refresh` re-proves
instruments that already have a proof; it cannot seed a new one. Initial provisioning is
`scripts/prove_2603_core_eligibility.py`.

A stale or missing proof is why `PUT /strategies/core-mandate` returns 409 on a payload
that is otherwise perfectly well formed.

---

## 1. Order of operations

The mandate comes first, but the dependency is **conditional**: `PUT /strategies/paper-pool`
consults `_core_pool_activation_ready` only when enabling a currently-disabled pool **and**
the alpha automation readiness is not itself ready. Core readiness additionally requires
the mandate's `policy_version` to equal `CORE_MANDATE_POLICY_VERSION` and the environment
to be `demo`.

### 1a. Configure and enable the mandate

`PUT /strategies/core-mandate`, fields: `enabled`, `core_instrument_id`, `core_target_pct`,
`liquidity_reserve_pct`, `rebalance_band_pct`, `min_rebalance_amount`, `reason`
(required), plus `provider` / `environment` selectors. Percentages are 0-100; amounts are
USD. `strategy_core_mandate` validates: band and minimum strictly positive,
`target − band > 0`, `target + band < 100`, and reserve ≤ `100 − target − band`.

Status codes, which are three different things and are routinely conflated:
**422** request shape, **400** an unrecognised provider/environment selector, **409** a
state of the system — stale eligibility proof, swapped credentials, invalid mandate
values, or nothing material changed.

⚠ Operator session only (`require_session`, not the router's
`require_session_or_service_token`): a mandate revision is an operator authorisation
recorded with a named `changed_by`, and `configure_core_mandate` needs a real
`operator_id` to select the right proof. Saving a mandate submits no order.

### 1b. Enable the shared paper pot

`PUT /strategies/paper-pool`, fields: `enabled`, `capital_limit` (positive when enabling),
`capital_mode` (`fixed` | `compound`), `risk_profile` (`cautious` | `balanced` | `growth`),
`reason`, and optionally `approval_mode`.

⚠ **An omitted `approval_mode` means UNCHANGED, not "manual"** (`resolve_approval_mode`,
#2843). Read the current value back before assuming which mode the pot is in.

⚠⚠ **This call also writes `enable_auto_trading`** via its `automation_changed` branch, so
the pot and the runtime flag move together. A separate toggle nonetheless exists —
`PATCH /config` accepts `enable_auto_trading` — so the flag is *synchronised* here, not
*owned* here; do not assume the pool endpoint is the only writer.

Two refusals fire on enable only: `settings.etoro_env != "demo"`, and
`runtime.enable_live_trading` being true ("paper automation cannot be enabled while
system-wide live trading is enabled" — the inverse rule lives in
`app/api/config.py::patch_config` under the same advisory lock; the two halves are one
invariant). ⚠ Those two are enable-only; *disabling* is not unconditionally accepted —
a corrupt runtime, a no-op request, an invalid payload or a capital-withdrawal constraint
can still refuse one.

### 1c. Clear the kill switch

`POST /config/kill-switch` with `{"active": false, "reason": "...", "activated_by": "..."}`
— both text fields are required on deactivation too. `decide_core_preflight` refuses
`core_kill_switch_active_or_missing` when the switch is active **or when no `kill_switch`
row exists at all**; an absent switch is not an inactive one, and it is read before the
instrument so an emergency stop is never reported as a missing instrument. ⚠ An absent
singleton cannot be repaired by this POST — it returns 503.

### 1d. Run one evaluation

`POST /strategies/core-sleeve/rebalance`. Demo-only (409 otherwise), operator session, and
it needs decryptable demo `api_key` / `user_key` credentials for that operator: missing or
undecryptable credentials are 503, while a credential mismatch against an unresolved
order, reconciliation contention and execution preconditions are 409 — none of which
appear in the four-state response body.

The 200 body carries `state` ∈ `held` / `refused` / `submitted` / `submission_uncertain`,
a `reason_code`, the intent / trade / order ids, the amount, and three policy-version
strings. ⚠ Those three are **defaults on the dataclass**: they are returned on early
refusals and holds too, and are not evidence that all three gates ran.

---

## 2. What the controls actually mean

| control | what it governs |
| --- | --- |
| `etoro_env` | which account. `demo` is what makes this a demo account; not an in-app toggle. |
| `enable_auto_trading` | `core_auto_trading_disabled` in the core preflight, and the alpha executor's own gate. Written by 1b, and separately by `PATCH /config`. |
| `enable_live_trading` | **must stay false**, and 1b refuses while it is true. It is the LEGACY recommendation path's broker-vs-synthetic switch (`app/services/order_client.py`), not a core control: the core executor builds a real `EtoroBrokerProvider` and calls the demo endpoint regardless of it. |
| kill switch | system-wide stop; refused on active OR absent. |
| execution block | **global, not per-instrument** — `EXISTS (SELECT 1 FROM strategy_execution_blocks b WHERE b.active)`. Any active row blocks every core submission. |

⚠ **These controls are shared, and enabling them does not isolate the attended core
evaluation.** `strategy_paper_executor` gates on the same `enable_auto_trading` and the
same kill switch, and `execute_approved_orders` acts on `approved` trade recommendations.
Measured 2026-09-14 on the dev DB: 0 `strategy_deployments`, 0 `strategy_promotions`, and
`trade_recommendations` holds only `considered` (67,501) and `rejected` (171) — so nothing
is eligible to fire today. **Re-measure before the session**; that is the check, not the
figure.

---

## 3. The #2844 reconciliation streak is not an admission gate here

`account_reconciliation_green_days` reaches a refusal in exactly one place:
`strategy_live_gate` appends `account_reconciliation_streak_insufficient` when the streak
is short. That function is the **live promotion** gate, keyed by strategy id and version,
and it also appends `live_strategy_broker_contract_not_validated` **unconditionally** — so
live promotion cannot pass today whatever the streak says.

No admission decision on the demo core path reads it: not `strategy_core_preflight`, not
`strategy_core_broker_preflight`, not `strategy_core_executor`, not the pool endpoint's
refusal logic. ⚠ It is nonetheless **read descriptively** on that path:
`get_strategy_overview` — which `PUT /strategies/paper-pool` calls to compute
`automation_readiness` — loads the streak and reports the countdown. Seeing the number in
that response does not mean it gated anything.

**Consequence: the first engine-placed demo position is gated on the #2833 verdict, not
on the reconciliation countdown's earliest green.** The countdown is evidence for a live
step that does not yet have a route in the code.

---

## 4. Refusal vocabulary — where each code comes from

Five closed vocabularies compose into one verdict. Codes seen at the endpoint may come
from any of them.

- **`strategy_core_allocator.CoreRebalanceReasonCode`** — the verdict itself:
  `core_mandate_absent`, `core_mandate_policy_unsupported`, `core_mandate_invalid`,
  `core_mandate_disabled`, `core_instrument_unset`, `sleeve_currency_mismatch`,
  `sleeve_instrument_mismatch`, `sleeve_valuation_invalid`, `broker_minimum_invalid`,
  `core_sleeve_empty`, `below_min_rebalance_amount`.
- **`strategy_core_preflight.CorePreflightRefusal`** — the DB-and-clock layer:
  `core_runtime_config_corrupt`, `core_auto_trading_disabled`,
  `core_kill_switch_active_or_missing`, `core_execution_block_active`,
  `core_instrument_missing`, `core_instrument_not_tradable`,
  `core_unsupported_market_session`, `core_market_session_closed`,
  `core_halt_feed_missing`, `core_halt_feed_stale`, `core_instrument_halted`,
  `core_quote_missing`, `core_quote_price_invalid`, `core_quote_crossed`,
  `core_quote_spread_flagged`, `core_quote_stale`.
- **`strategy_core_broker_preflight.CoreBrokerPreflightRefusal`** — the broker layer:
  `sandbox_exceeded`, `core_close_side_cost_quote_unavailable`,
  `core_account_risk_unavailable`, `core_account_risk_stale`,
  `core_account_risk_unobservable`, `core_sleeve_moved_since_decision`,
  `core_minimum_currency_unsupported`, `core_broker_open_minimum_unquoted`,
  `core_cost_assessment_unavailable` — **plus the sizing codes it passes through**:
  `cost_quote_unusable`, `cost_quote_stale`, `cost_rate_implausible`,
  `cost_quote_ticket_mismatch`, `cost_breaches_far_edge`.
- **`strategy_core_submission_gate.CoreSubmissionRefusal`** — durable authority:
  `core_intent_missing`, `core_intent_not_actionable`, `core_intent_superseded`,
  `core_mandate_revision_stale`, `core_mandate_not_paper`, `core_mandate_disabled`,
  `core_intent_already_submitted`, `core_trade_in_flight`, `core_eligibility_unproved`,
  `core_partial_close_unproved`. ⚠ Its own docstring warns that this list is not the
  complete submission refusal set — a reader who takes it for the whole concludes the core
  arm has no kill-switch check.
- **The executor's own outcomes**, which belong to no preflight: `portfolio_drawdown_limit`,
  `core_credential_provenance_changed`, `core_submission_action_unbuilt`,
  `core_exit_anchor_unavailable`, `core_exit_levels_underivable`,
  `broker_submission_rejected`, `broker_submission_uncertain`, and the reconciliation
  outcomes.
  - ⚠ The two `core_exit_*` codes are #3284 item 1 and both mean the same operator-facing
    thing: **the entry was refused because its stop and target could not be derived**, so
    no order exists and nothing is naked. `core_exit_anchor_unavailable` is an admitted
    preflight that nonetheless carried no usable ask; `core_exit_levels_underivable` is an
    ask too small to quantize a stop from (under two cents). Neither is reachable for SPY.

⚠ **Do not read a reported code as ruling out the conditions listed above it.** The
declaration order is a stable *reporting* contract inside a module; the executor's actual
call order is not that list (the sell refusal precedes the sandbox check, the account
fetch precedes both, and the broker preflight runs before intent admission and the DB
preflight, so an earlier layer's refusal can mask a later one).

Codes that will be seen and are not faults:

- `core_market_session_closed` — run it inside the regular US session (09:30-16:00 New
  York; half-days close 13:00; holidays closed).
- `below_min_rebalance_amount` — the floor biting. A configured target does not guarantee
  an order: the buy must cross the lower band and clear both the mandate minimum and the
  broker's open minimum after sizing.
- Freshness: quote ≤ 5,400 s, halt feed ≤ 450 s (both `_freshness_bound` = 1.5× nominal
  cadence), future skew ≤ 5 s, and the account snapshot must still be within
  `CORE_MAX_ACCOUNT_RISK_AGE_SECONDS` after the cost call — that second check is the
  binding one, and it fires when the what-if needed retries.

⚠ `sandbox_exceeded` means committed exposure is **past** the bound, not at it
(`committed <= bound` is within-bound and merely full). It is a real breach of the
assigned-capital boundary, not a formality. The read endpoint reports insufficient
headroom under the different code `core_sandbox_exceeded`.

---

## 5. What the autonomy loop does not do

`.autonomy/hard_rules.md`: *never execute, approve or simulate a trade; never POST order
endpoints; never touch the kill-switch; never close a position.* So steps 1c and 1d are
operator actions by rule. Steps 1a and 1b are not named there — the loop's restraint on
those comes from #2843's escalation contract (page once, with the evidence, when
everything else is green) rather than from a mechanical prohibition.

---

## 6. After the order

Take the ids from the response and filter on them; the tables below also carry alpha-arm
rows, so unfiltered "recent" queries prove nothing about this order.

```sql
SELECT order_id, action, order_type, requested_amount, status, execution_origin, strategy_request_id
  FROM orders WHERE order_id = :order_id;
SELECT * FROM strategy_trade_orders WHERE strategy_trade_id = :trade_id;
SELECT * FROM strategy_order_reconciliation_state WHERE order_id = :order_id;
SELECT * FROM strategy_position_ownership WHERE strategy_trade_id = :trade_id;
SELECT core_rebalance_intent_id, core_eligibility_proof_id, instrument_id, status
  FROM strategy_trades WHERE strategy_trade_id = :trade_id;
```

⚠ **`submitted` is not filled.** The row is INSERTed with `status='submitted'` *before*
the broker call, and the state means "accepted, pending reconciliation". The success
criterion is a resolved reconciliation row plus ownership linked to that trade — not the
POST returning 200.

`GET /strategies/core-sleeve` then reports `pending_order_id` and `can_resume`. ⚠
`can_resume` means only "demo environment, and unresolved authority was discoverable"; it
says nothing about credentials or about recovery succeeding. The same rebalance POST takes
the resume path automatically when authority exists, and **resume only reconciles — it
never re-submits**, so a further attended POST is not by itself a duplicate-order attempt.
Repeated lookup misses can strand the authority; there is no terminalisation surface at
HEAD (#2961).

---

## 7. Known-blocked arms — do not discover these live

- **The sell leg** returns `core_close_side_cost_quote_unavailable` from the broker
  preflight, before any *order* call (the account-risk read has already happened). The
  cost quote is no longer the obstacle — the close arm of
  `/api/v2/trading/info/demo/costs` takes a non-empty `positionIds` and was measured
  returning 200 with real cost rows (#2712) — the SUBMISSION half is: a partial close by
  amount needs a new provider mutation, its quota lane, unattended-guard wiring and
  close-side reconciliation, on top of #2979 and #2965. One slice, after those close, with
  its own attended demo CLOSE for acceptance.
- **Non-USD deployment** is #2603 item 4: the committed
  `tests/fixtures/etoro/openapi_v1.375.0.json` documents `orderCurrency` as *"Currently
  always the account's currency (USD), since eToro only supports single-currency accounts
  today."*
- **An LSE sleeve needs three pieces, not one** — the venue allow-list, an LSE session
  clock (`_session_is_open` runs the NYSE calendar), and halt coverage for the venue. The
  halt predicate itself reads stored unresolved `strategy_market_halts` rows from
  `nasdaq_trader_rss`, whose producer only polls US hours, so off-US absence of a halt row
  is not evidence of no halt. Design notes on #2312.
- **Not enforced anywhere on this path, and not inferable from the gates above:**
  `maxUnitsPerOrder` (quoted in units while the sleeve sizes in currency), and whether
  unsettled proceeds or accrued charges are already deducted from the account's available
  cash.

Refs #2603. Refs #2833. Refs #2844. Refs #2843. Refs #2312.
