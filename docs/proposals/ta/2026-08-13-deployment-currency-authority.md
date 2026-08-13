# Deployment currency: one declared authority, one named refusal

**Ticket:** #2603 scope item 4. **Status:** proposal for the refusal branch.

Item 4 reads: *"Non-USD/GBP deployment support across all three hardcode sites, or one
explicit operator-visible refusal if deferred — never a partial lift."*

This takes the refusal branch. Non-USD support is **not** built: FX is unmodelled
(#2363 split `fx_unmodelled` out as its own standing refusal), so there is nothing
honest to charge a non-USD deployment. What ships is the deferral made enforceable.

## Source rule

Two rules govern, and neither is inferred here.

1. **ISO 4217** fixes the canonical form of a currency code as upper case. That is the
   whole of what it supports here, and the whole of what is claimed: `.upper()` on an
   operator-supplied code before comparison. It does **not** license whitespace
   stripping, so `strip()` is applied only where `_require_text` has already rejected
   blank input, and it is **not** applied to broker response values at all (below).
   The enforced invariant is set membership, not the ISO shape — no `^[A-Z]{3}$` CHECK
   is claimed or added.
2. **`docs/review-prevention-log.md:720`** — *"New TEXT columns in migrations need CHECK
   constraints or Literal types"* (#232). Its symptom is verbatim the defect below:
   *"`capital_events.currency` was a free-text TEXT column with no CHECK constraint …
   arbitrary strings persisting in domain columns violate the enum-style semantics."*
   Its prescribed fix is a CHECK constraint plus a constrained type on the API model.
   That is a settled repo invariant, not a judgement call, and the deployment tables
   have never satisfied it.

## The defect, measured

The executor reads the **per-strategy deployment** currency, not the pool's:
`strategy_paper_executor.py:198` selects `d.currency`, and `:374` refuses on it with
`deployment_currency_unsupported`. That column is validated nowhere:

| layer | what it does |
| --- | --- |
| `sql/281_strategy_promotion_ownership.sql:72` | `currency TEXT NOT NULL DEFAULT 'USD'` — no CHECK |
| `sql/281:82` | the only constraint is `currency <> ''` |
| `sql/281:93`, `:101` | `strategy_deployment_events.currency`, same |
| `strategy_control_plane.py:558` | `currency: str = "USD"`; the only guard is `_require_text` (non-empty) |
| `app/api/strategies.py:1883` | passes `row.allocation.currency` — read back out of the DB |
| `app/api/strategies.py:2162` | passes `str(currency)` — read back out of the DB |

`StrategyAllocationView.currency` is a bare `str` (`strategies.py:241`), so nothing
narrows it on the way out either. The `Literal["USD"]` fields at `:224` and `:284` are
on `StrategyPnlView` and `StrategyPaperPoolView` — **response** views, which constrain
what is rendered and never what is written.

So the stored currency is a self-perpetuating round-trip seeded by the column default.
There is no path by which an operator supplies one, and equally no path that would
reject one if a job, script or future allocator did.

The pool side is not affected: `strategy_paper_pool_events.currency` carries
`CHECK (currency = 'USD')` (`sql/290:96`) and `configure_paper_pool` writes a `'USD'`
SQL literal (`strategy_control_plane.py:313`). The lock is real there; it is the
deployment side, which is what the executor actually reads, that is open.

### Full population, dev DB, at branch point `72619dcc`

```sql
select currency, count(*) from strategy_deployments group by 1;            -- 0 rows
select currency, count(*) from strategy_deployment_events group by 1;      -- 0 rows
select count(*) from strategy_paper_pool_events;                           -- 0
select currency, count(*) from strategy_live_gate_policies group by 1;     -- 0 rows
select count(*) from strategy_core_mandate_events;                         -- 0
select currency, count(*) from broker_account_equity_snapshots group by 1; -- [('USD', 3)]
```

Nothing to migrate and nothing to repair. The constraint validates trivially today and
forbids the junk row tomorrow — which is the only window in which adding it is free.

## What ships

**One declared authority.** A new module `app/services/strategy_base_currency.py`
holding `DEPLOYMENT_CURRENCY`, `SUPPORTED_DEPLOYMENT_CURRENCIES`, the refusal code
`DEPLOYMENT_CURRENCY_UNSUPPORTED`, and `normalise_deployment_currency(value) -> str | None`
(canonical code if supported, else `None`).

It is a standalone module rather than a symbol on `strategy_control_plane` because the
executor and the API both already import that module for other reasons, but a constant
this small should not require a 1000-line import to reach — and `strategy_core_mandate`
records a deliberate refusal to couple to it at all. `DEPLOYMENT_CURRENCY` is the single
supported code; `SUPPORTED_DEPLOYMENT_CURRENCIES` is a `frozenset` containing it, so
runtime mutation cannot widen authorisation, and an invariant test pins their agreement.

The helper returns `None` rather than raising so that each caller raises its own error
type; a shared exception would drag one module's error class across four others.

**Enforcement at the capital authority.** `configure_deployment` normalises the
currency once, at the top, and refuses an unsupported one with `StrategyControlError`.
Normalising by rebinding the parameter means every downstream use — the
`is_risk_reducing_deployment_change` comparison at `:589`, the INSERT at `:617`, the
UPDATE at `:636`, the event INSERT at `:646` — reads the canonical value. One
normalisation, not four comparisons kept in step.

**The refusal is unconditional, and needs no risk-reducing exemption.** The neighbouring
`purpose` and `stage` guards each carry `and not risk_reducing`, because a guard that
blocks an operator from *disabling* a deployment fails open — and the retirement path
(`strategies.py:2162`) does pass the stored currency straight back in. This guard does
not need the same escape hatch: `sql/338` forces the stored currency to be supported, so
the value a disable echoes back is always canonical and can never trip the refusal. An
exemption would be provably dead code justified by a state the CHECK forbids.

**The service returns the currency it persisted.** `Deployment` gains a `currency`
field and `update_strategy_allocation` echoes `deployment.currency` instead of the
pre-call `row.allocation.currency`. Without this the response can disagree with the row.

**Two different comparisons, deliberately not the same one.** This is the distinction
Codex checkpoint 1 caught, and getting it wrong is what would make this a partial lift.

- *Is the stored deployment currency one we support?* — a **membership** test against
  `SUPPORTED_DEPLOYMENT_CURRENCIES`. Sites: `strategy_paper_executor.py:374` and
  `app/api/strategies.py:1129`.
- *Did the broker answer in the currency this deployment trades in?* — an **equality**
  test against `intent.currency`. Sites: `strategy_paper_executor.py:555` (eligibility
  response) and `:593` (each cost component).

Membership at the broker sites would be a latent partial lift. With the set at
`{"USD", "GBP"}` a response carrying a USD component and a GBP component passes both
membership tests, and `_costs` then sums them into one total with no FX conversion —
precisely the arithmetic #2363 refused to perform. Equality against a single
`intent.currency` makes a mixed-currency response unrepresentable, and ties eligibility
and costs to each other transitively.

`_Intent` gains `currency` from `row["currency"]` for this. Ordering is already safe:
the `checks` tuple at `:352` short-circuits and returns before `_Intent` is constructed
(`:456`), so `:374` has always passed by the time either broker comparison runs.

`.upper()` is preserved verbatim at both broker sites and `strip()` is **not** added —
the broker contract, not ISO, governs what its response field may contain, and today
`" USD "` is rejected. Widening that is a behaviour change with no source rule behind it.

**`sql/338`** adds `strategy_deployments_currency_supported` and
`strategy_deployment_events_currency_supported`, both `CHECK (currency = 'USD')`,
matching the treatment `sql/290:96` already gives the pool and `sql/027`'s
`chk_capital_events_currency` gives `capital_events`. Distinct names so a current-state
violation and an event violation stay distinguishable in the `diag.constraint_name`
assertion.

**Out of the change: two authorities that already lock themselves.**
`configure_paper_pool`'s `'USD'` SQL literal (`strategy_control_plane.py:313`) and
`CORE_MANDATE_BASE_CURRENCY` (`strategy_core_mandate.py:32`) stay literals. Both sit
behind their own schema CHECK (`sql/290:96`, `sql/336:26`), and pointing them at the
deployment constant would let one authority's widening silently drive two others whose
schemas had not moved — a cross-authority coupling with no upside. Only the stale
site-list comment above `CORE_MANDATE_BASE_CURRENCY` is corrected, because this change
invalidates it.

## Why a pinned-cardinality test, and why it is not a stale literal pin

`SUPPORTED_DEPLOYMENT_CURRENCIES` is asserted equal to `{"USD"}` in a fast-tier test
whose docstring enumerates every site that must move together when it widens. Item 4's
*"never a partial lift"* is otherwise a sentence in a ticket; this makes it a gate.

The prevention log warns against literal version pins (`:2189`) because the invariant
there is "the constant is stamped through", which a live-constant comparison expresses
better. This is the other case the same entry sanctions at `:2193(3)`: a deliberate
bump-visibility pin, whose whole purpose is to force acknowledgement of a change — and
it belongs in the fast tier, where it fires on push. The db tier does not.

A db-tier test asserts each constraint fires **by name** (`diag.constraint_name`), not
merely that *some* `CheckViolation` was raised — a class-only assertion passes on a
bystander constraint. A fast-tier test pins `DEPLOYMENT_CURRENCY ∈
SUPPORTED_DEPLOYMENT_CURRENCIES`; two constants that must agree get an invariant test
rather than a comment (`docs/review-prevention-log.md:2081`).

No test parses the migration text to bind `sql/338`'s literal to the constant. Text
matching passes while the migration is unapplied, superseded or altered by a later
file, and this repo has already been bitten by a convention test satisfied by an
`import` line. The db-tier constraint-name assertion is the real gate.

## Where the refusal is actually visible

Precision matters here, because "operator-visible" was nearly overclaimed.
`update_strategy_allocation` maps **every** `StrategyControlError` to a generic
`"allocation update refused"` (`strategies.py:1887`), so the `StrategyControlError` this
adds is a *service-layer guard*, not an operator surface.

The operator surface is the existing `deployment_currency_unsupported` code, which
already renders in two places and keeps doing so: `allocation_refusals` on
`/strategies/overview` (`strategies.py:1130`) and the executor's stored rejection reason
(`:374`). The change makes both read the declared set instead of a literal; it does not
invent a new surface, and does not claim one.

## Two things noticed, not fixed here

- `strategies.py:1859` computes `risk_reducing` with `current_currency` and `currency`
  both bound to `row.allocation.currency` — a tautological comparison that can never
  detect a currency change. Harmless (the service now normalises and re-checks), but it
  means the API-side `is_risk_reducing_deployment_change` call is weaker than it reads.
- `StrategyAllocationView.currency` is a bare `str` (`strategies.py:241`). The
  prevention entry's second limb ("`Literal` types on the API model") is satisfied on
  the write path by the service guard; narrowing the read view is a separate change
  with a frontend type consequence.

## Explicitly not delivered

- Non-USD or GBP deployment support. Deferred, and now refused by name rather than by
  a column default nobody enforces.
- Any change to `strategy_paper_pool_events`, `strategy_live_gate_policies`,
  `broker_account_equity_snapshots` or `strategy_core_mandate_events` — schema or code.
  Their CHECKs are already correct and their code-side literals match them; see the
  cross-authority coupling argument above.
- The `Literal["USD"]` response fields. They render, they do not authorise.
- Any repair or backfill. There is one database (dev), the six currency columns hold a
  combined 3 rows and all 3 are `USD`, so `ADD CONSTRAINT` validates against the full
  population with nothing to fix.
