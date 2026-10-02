# #3542 — mandatory trade ticket on every ENTRY order

Gap register `docs/proposals/2026-10-01-gap-register.md` §P3. Operator standing rule (#2437, 2026-10-01 15:58Z):
*no new strategy, trial or capital action is started without a P3-style ticket stating why this, why now, what
exit, what evidence — when that is "beta" or "experiment", say so.*

## Problem

Four code paths create an engine entry order, each recording a different, partial explanation:

| path | entry site | what is recorded today |
|---|---|---|
| alpha paper | `strategy_paper_executor.py:1575` | `strategy_entry_preflights` (cost, SL/TP); the rule is implicit in the signal |
| AI trial | `ai_trial_executor.py:341` | preflight + the trial's thesis/plan rows |
| ranking pot | `ranking_pot_executor.py:177` | preflight + the §6 lifecycle `ticket` JSON (`sql/450`) |
| core sleeve | `strategy_core_executor.py:1365` | intent + `strategy_core_entry_exit_levels`; **no cost** (the preflight's quoted `cost_rate` is discarded) and no statement that it carries no entry signal |

Nothing makes an explanation mandatory, and no single row answers "why did the engine buy this?".

## Source rule

- **Fields are fixed by the P3 plug, verbatim:** rule id; evidence id (register row or declaration); rationale
  class `signal` / `rebalance` / `experiment`; exit rule, or `not_applicable` with a reason for passive holds;
  expected cost.
- **Exits and protective actions are exempt** — settled EXIT rule: an exit is never blocked
  (`.claude/skills/execution-guard`, "EXIT never blocked"). Enforcement keys on
  `strategy_trade_orders.purpose = 'entry'` only; `exit`/`stop_loss`/`take_profit`/`stop_ratchet`/`reconcile`
  are untouched.
- **Expected cost is a recorded figure, never a new constant**: the stressed cost each alpha/trial/pot path
  already writes to its preflight (`stressed_cost_amount`, with `cost_basis`), and for core `amount × cost_rate`
  from the broker what-if quote its preflight already takes (`strategy_core_broker_preflight.py:401-450`).

## Design

1. **`sql/458` `strategy_entry_tickets`**, one row per entry order:
   `order_id` PK → `orders` RESTRICT; `strategy_trade_id` → `strategy_trades`; `rationale_class`
   CHECK IN (`signal`,`rebalance`,`experiment`); `rule_id` TEXT; `evidence_kind` CHECK IN
   (`strategy_promotion`,`ai_trial_declaration`,`ranking_pot_declaration`,`core_mandate_event`);
   `evidence_id` BIGINT; `why_now` TEXT (1..1000); `exit_rule` TEXT (1..1000) — either a rule or
   `not_applicable: <reason>` (CHECK: the bare word is refused); `expected_cost_usd` NUMERIC ≥ 0;
   `cost_basis` TEXT; `created_at`.
2. **Enforcement:** a `DEFERRABLE INITIALLY DEFERRED` constraint trigger on `strategy_trade_orders` AFTER
   INSERT `WHEN (NEW.purpose = 'entry')` raises unless a ticket exists for `NEW.order_id` with the same
   `strategy_trade_id`. Deferred, so a path may link then ticket in either order inside its authority
   transaction; the transaction cannot commit without one. Fail-closed: an entry without a ticket never
   reaches the broker (every path submits only after its authority commit).
3. **One writer**, `app/services/strategy_entry_ticket.py::write_entry_ticket(conn, ...)`, called in each
   path's authority transaction:

   | path | class | rule_id | evidence | why_now | exit_rule | cost |
   |---|---|---|---|---|---|---|
   | alpha paper | `signal` | `strategy_id@version` | latest `paper_enabled` promotion of that version | signal id + signal date; all entry gates passed | SL/TP rates + max position age from the execution policy | preflight stressed cost |
   | AI trial | `experiment` | `strategy_id@version` (arm or control) | `ai_trial_declarations.declaration_id` | decision run + session | plan SL/TP + trial exit time | preflight stressed cost |
   | ranking pot | `experiment` | `ranking-pot@STRATEGY_VERSION` | the pot declaration id | lifecycle id + rebalance date | SL/TP + the declared exit rule | preflight stressed cost |
   | core | `rebalance` | `core-mandate` | the mandate event id the intent was sized under | band breach that created the intent (beta, no entry signal) | protective SL/TP levels (`strategy_core_entry_exit_levels`); otherwise held to the mandate | `amount × cost_rate` (`cost_basis = 'broker_what_if'`) |

   AI trial and ranking pot are `experiment`: both are declared trials whose readouts decide whether they
   carry an alpha claim. The alpha path is `signal` only because promotion to `paper_enabled` is itself the
   evidence gate.
4. **Frozen code:** no `ai_trial_policy.POLICY_MODULES` file is edited (`ai_trial_executor.py` is not one; the
   fast-tier hash test enforces this). `ranking_pot_executor.py` IS in `ranking_pot_policy.POLICY_MODULES`; v1
   is not yet frozen, so `RANKING_POT_POLICY_HASH` moves and is noted on #2842 — the freeze takes the new hash.
5. **Provider-direct actions** (P2's SVXY/SVOL) never create `strategy_trade_orders`, so they are outside the
   engine by construction; the ticket does not try to cover them.

## Slicing (budget split, after Codex ckpt-1)

- **Slice 1 (this PR):** the table, the writer and all four path writers. Every new entry gets a ticket;
  each path's DB test asserts it. No enforcement yet, so nothing that commits an entry link can break.
- **Slice 2:** the deferred constraint trigger (design 2), ticket immutability (no UPDATE/DELETE;
  `strategy_trade_orders.purpose`/`order_id`/`strategy_trade_id` immutable once linked), and the test-fixture
  tickets the trigger then requires (5 raw `entry` inserts + direct `link_strategy_order` callers).
  Pre-migration entries are grandfathered by construction (the trigger fires on INSERT only); dev holds 5.

## Codex ckpt-1 (37 findings) — dispositions

- **Applied in slice 1:** NOT NULL on every field (#7); no whitespace-only text, `not_applicable: <non-blank>`
  (#8, #9); `expected_cost_usd` refuses `NaN` (#10 — `NaN >= 0` is TRUE in Postgres); exit values are the ones
  the path itself persists — the preflight's `stop_loss_rate`/`take_profit_rate`, core's
  `strategy_core_entry_exit_levels` (with its `policy_version`) — never re-read from plan or policy (#19, #21);
  `cost_basis` copied verbatim from the path's assessment (#22); core cost = the admitted verdict's
  `amount × cost_rate`, the amount the order is placed at, and an admitted verdict with `cost_rate` NULL or
  non-finite raises before the authority commits (#23, #24); core currency USD is asserted (#25; all 9
  mandate events are USD on dev); the writer is called only on the entry branch (#31); text over the CHECK
  limit raises, never truncates (#36); a second ticket for one order is a PK error, never ignored (#29).
- **Evidence (#12-#17):** each id is the one the path's own authority check reads in the same transaction:
  alpha — the latest `strategy_promotions` row of that version, which `current_stage` reads under
  `_lock_strategy` in `decide_funding` (the promotion that authorised it); AI trial — `intent.declaration_id`,
  which `_authority_refusal` re-reads `FOR SHARE`; pot — `intent.declaration_id`; core — the intent's
  `core_mandate_event_id`. Promotion = register row and mandate event = declaration, per P3's "register row or
  declaration" (#14). `rationale_class` is fixed per path by what admits it, not inferred per trade (#15).
- **why_now (#18):** carries the locator of the explanation that already exists (trial plan id, pot lifecycle
  id + ticket sha, signal id + date, core intent id + kind). It does not copy the narrative.
- **Slice 2 (#1-#6, #28, #30, #34):** enforcement, immutability and rollout as above. Orders with no
  `strategy_trade_orders` row (#5, #32) are not engine entries: every engine path links before commit, and
  provider-direct is barred by P3 as a rule, not by this table.
- **#27:** every path commits its authority transaction before the broker verb (unchanged).
- **#33:** the ticket covers entry ORDERS. Starting a strategy or trial is already gated by its declaration
  and promotion records.
- **#35:** `ranking_pot_declarations` holds 0 rows on dev, so no declaration binds the old hash.

## Out of scope

Rendering tickets in the UI (the ranking-pot page already renders its §6 ticket). Back-filling tickets for
entries before this migration — a ticket is a statement made at the point of action, and writing one later
would be a reconstruction.

## Tests

- Slice 1: each path's existing DB acceptance test asserts its ticket row: class, evidence kind and id, cost and
  basis equal to the preflight's, or `amount × cost_rate` for core. Table CHECKs: blank text, bare
  `not_applicable`, NaN cost.
- Slice 2: an entry link without a ticket fails at commit; one with a mismatched trade fails; an exit link
  without one commits; UPDATE/DELETE of a ticket is refused.
