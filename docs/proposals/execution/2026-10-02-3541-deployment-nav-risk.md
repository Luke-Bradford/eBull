# #3541 slice 3 — per-deployment risk state measured on the deployment's own book

Slices 1 (`2026-10-02-3541-engine-pot-drawdown.md`, #3574) and 2 (`2026-10-02-3541-engine-pot-exposure.md`,
#3576) moved the pot drawdown and the exposure caps off the demo account. This slice moves the last
account-basis risk figure: the per-deployment paper-period drawdown the live-promotion gate reads.

## Problem

`strategy_paper_runtime.refresh_strategy_health` writes ONE value — broker ACCOUNT equity — into
`strategy_paper_deployment_risk_state` (sql/290) for every enabled paper deployment
(`strategy_paper_runtime.py:406-441`). `strategy_live_gate.assess_live_gate` reads its
`max_drawdown_pct` as `max_observed_drawdown_pct` and refuses `drawdown_unavailable_or_high` above the
policy's `max_drawdown_pct` (`strategy_live_gate.py:451-460, 726-729`). So a deployment's "paper drawdown"
is the account's, which holds the operator's own positions and copy mirrors: on dev at 14:10Z both AI-trial
deployments (8, 9; $3,000 each, zero trades) carry `last_equity` 105,551.62 and `max_drawdown_pct` 1.566 —
a figure neither deployment produced.

## Source rule

- **Same construction as the pot (slice 1)**, applied to one deployment: `NAV = principal + realised +
  unrealised`, drawdown = peak-to-trough of a time-weighted NAV index linked per GIPS 2020 2.A.24(f), with
  slice 1's fail-closed flow-timing bound (`engine_pot_risk.link`, unchanged and reused).
- **Principal = the deployment's current `capital_limit`** (`strategy_deployments`, sql/281). This is the
  supervisor's definition on #3541 (2026-10-02 14:10Z) and the pot's (its latest pool event is its
  principal). A capital revision is a principal flow and is linked as one.
- **Population = the deployment's exact-owned book**: trades whose funding decision is `allocated` to that
  deployment (`strategy_funding_decisions.deployment_id`), each with its ownership rows; point-in-time
  membership and close accounting exactly as the pot (`claimed_at <= t < released_at`; closes with
  `executed_at <= t`). A trade with status `open`/`closing`/`closed` and no ownership row refuses, as the pot.
- **Max drawdown** over the paper period = the maximum of every linked observation's drawdown — the
  quantity the live gate's policy bounds.

## Design

1. `sql/457`: `strategy_deployment_nav_risk_state` (one row per deployment, FK RESTRICT): `nav_index`,
   `index_high_water`, `last_nav`, `last_pnl`, `last_drawdown_pct`, `max_drawdown_pct`, `last_refusal`,
   `observed_at`. `strategy_paper_deployment_risk_state` stays in place, no longer written or read (its rows
   are account dollars and cannot seed an index — slice 1's precedent with sql/288).
2. `engine_pot_risk`: the snapshot validation and the exact-owned book pricing are extracted from
   `observe_pot_nav` into shared helpers. New `observe_deployment_nav(conn, snapshot, deployment_id)` and
   `advance_deployment_drawdown(conn, deployment_id, nav)` (bootstrap-insert then `FOR UPDATE`, as the pot).
   A snapshot older than the stored row is skipped, not linked. **The base is the deployment when first
   funded** (its first `capital_limit > 0` revision, P&L 0, at `created_at`), so P&L earned before the first
   observation is linked rather than absorbed into the base (Codex ckpt-1 #1), and a capital revision before
   it is linked as a flow (Codex ckpt-2: created at $100, down $50, raised to $1,000 must read 50%, not 5%).
   A non-positive principal refuses in the observer (ckpt-2), so it never reaches the table's CHECKs. The paper period is the deployment's lifetime: a new strategy
   version is a new `(strategy_id, strategy_version, mode)` row (sql/281 UNIQUE), and disable/re-enable
   keeps the same book.
3. **Fail-closed on an unobservable deployment**: `record_deployment_refusal` sets `last_refusal` on an
   existing row; a successful advance clears it. The live gate treats a non-NULL `last_refusal`, like a
   missing row, as `max_observed_drawdown_pct = None` → `drawdown_unavailable_or_high`. One deployment's
   failure never blocks another's advance or the pot's `drawdown` block. **A row the probe stopped advancing
   is not evidence either** — broker probe failing or stale, deployment disabled or without an execution
   policy: the gate ignores a row older than the policy's `max_broker_health_age_seconds`, the declared
   freshness bound of the same broker probe that writes it (Codex ckpt-1 #12-#14).
4. Runtime: the per-deployment write loop calls the above per enabled paper deployment, each in its own
   savepoint, only on a fresh probe (unchanged condition).
5. Carried from slice 2's Codex round 2 (the observer is re-read here), both dispositioned without code:
   - *"the pot takes the latest principal with no `changed_at <= snapshot` filter"* — kept, and the
     deployment reader does the same. A revision written between the snapshot and the read is linked into
     the span ending at the snapshot instead of the next one; `link` already assumes nothing about where in
     a span a flow lands and takes the smaller base, so the mis-timing is fail-closed. A point-in-time
     filter would also break every fixed-clock fixture (`_NOW` = 2026-08-07, events stamp `now()`), which
     is cost with no safety gain.
   - *"a duplicate ownership row silently overwrites `held[position_id]`"* cannot occur:
     `strategy_position_ownership.broker_position_id` is `UNIQUE` (sql/281:189), so one position has at
     most one ownership row and cannot be both held and released. A comment cites it.
6. Display: `strategy_monitoring`'s `max_observed_account_drawdown_pct` is renamed
   `max_observed_pot_drawdown_pct` (API + FE type). Its source column `strategy_entry_preflights.
   account_drawdown_pct` has carried the POT drawdown since slice 1 (every writer passes the shared sizing
   result); a column comment records that. Dev holds 0 preflight rows, so no account-basis row is mixed in.

## Out of scope

- The executor's per-deployment `strategy_execution_policies.max_drawdown_pct` entry check still compares the
  POT drawdown (slice 1). Moving it onto the deployment's own drawdown changes v1-trial entry behaviour and
  is a separate decision; noted on #3541.
- Must not edit any `ai_trial_policy.POLICY_MODULES` file (a fast-tier test fails the push).

## Tests

- DB: deployment population = only that deployment's book (the manual position's mark excluded), creation
  base links pre-observation P&L, max-drawdown persistence, stale skip, refusal set then cleared, a pure
  capital revision moves no return.
- Runtime: the account falls, the deployment owns nothing → deployment drawdown 0, live gate reads 0; a
  refused row and an aged row read as no evidence.

## Codex ckpt-1 (37 findings) — dispositions not applied above

- **#4-#6, #22-#28 (the shared `link` arithmetic and `observe_pot_nav` population rules)**: reused unchanged
  from slice 1, whose spec carried two ckpt-1 rounds over exactly that code. Re-litigating them belongs to a
  change of that design, not to the consumer that reuses it. #5 (offsetting flows between two ticks) is a
  known limit of endpoint-principal linking; capital revisions are rare operator actions and ticks are minutes.
- **#7 currency**: `strategy_deployments.currency` is constrained to the supported set `{USD}` at rest (sql/338).
- **#8 zero capital / #9 non-positive NAV**: refuse, which leaves the gate without evidence — correct for a
  deployment with no capital, and fail-closed for a total loss.
- **#15-#19 ordering/concurrency**: the per-deployment row has ONE writer, `refresh_strategy_health`, which
  runs serially per tick; the executor never writes it. The row lock still serialises a hypothetical second.
- **#20 gaps**: every observation is cumulative (principal + all P&L to date), so a gap misses only the
  intra-gap trough — the same sampling limit every tick has.
- **#21**: the exception is caught outside the savepoint and the refusal written after rollback, as specified.
- **#29**: the "duplicate-held" test line was removed with the code (see 5).
- **#34**: `strategy_entry_preflights` is empty on the only database (dev = the demo-trading environment).
- **#36**: equality semantics are unchanged (`<=` promotion vs `>=` block).
