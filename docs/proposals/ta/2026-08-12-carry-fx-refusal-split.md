# Splitting `carry_unmodelled` into `carry_unmodelled` + `fx_unmodelled`

Ticket: #2363, item 1 of the 2026-08-12 Tier-1 re-scope. Refs #2437, #2277, #2599.

## The decision this document exists to make

#2363's re-scope comment states the constraint and defers the choice:

> The refusal is COUPLED: `cost_model.py:137` computes `CARRY_UNMODELLED` from
> `CARRY_BPS is None or FX_BPS is None`. Carry cannot be resolved independently of FX
> without splitting the refusal — so the deliverable is either both halves together, or
> a schema/refusal split (`carry_unmodelled` / `fx_unmodelled`) so each can close on its
> own evidence. **Decide in the spec, not mid-implementation.**

**Decision: split.** "Both halves together" is not available to this run and is not
available soon: closing carry needs a per-order eToro product-eligibility response
proving underlying-at-x1, and closing FX needs `GET /api/v1/balances` to resolve the
account's native currency and measure `exchangeRate` against our `fx_rates` mid. Both are
live-portal work that #2598 already owns and that an unattended run cannot reach. Coupled,
the *cheaper* evidence cannot be banked when it arrives — and the two are not even the
same kind of evidence: carry is a per-order product question, FX is an account-state
question.

## Source rule

There is **no published formulation** for this — it is an internal promotion invariant,
not a data-treatment rule with a governing reg. Per `.claude/CLAUDE.md` ("where a
published formulation genuinely does NOT exist, say so explicitly and fix the rule **by
construction**, freezing the constants in a version hash"), the rule is fixed by
construction and frozen in `STRUCTURAL_REFUSAL_POLICY_VERSION`, which exists for exactly
this: `prereg_contract.declaration_refusals` refuses a declaration frozen under a
superseded policy version rather than re-interpreting it.

Two existing repo conventions govern parts of this and are followed rather than reasoned
out afresh: `sql/326`'s store-column-then-`CREATE OR REPLACE VIEW`-then-restore-check-option
sequence, and `cost_model.py`'s own rule that a change to what is charged is "a code
change plus a new `COST_MODEL_ID`".

## Why now, measured

Computed against the dev DB on 2026-08-12, this run. Reproduce with:

```sql
select count(*) from strategy_preregistration_declarations;
select universe_basis, carry_unmodelled, count(*) from strategy_results_store group by 1,2;
select carry_cost_known, fx_cost_known, count(*) from cost_model where valid_to is null group by 1,2;
```

Results as measured 2026-08-12, before the migration. `strategy_backtest_run` is a live
job, so the counts move — re-run the block above rather than reading these as current.

| query | result (2026-08-12) |
| --- | --- |
| declarations | **0** |
| `strategy_results_store` by basis/carry | `survivor_only, t, 276` |
| `strategy_results` (the in-sample VIEW over it) | `survivor_only, t, 24` |
| active `cost_model` completeness flags | `f, f, 43` |

Two consequences, both pointing the same way:

1. **A policy-version bump supersedes nothing today.** The declarations table is empty, so
   the split costs zero re-freezes. #2616 leaves a freeze script for the operator to run
   and C-4's freeze is also pending; the moment either runs, this change starts
   invalidating frozen declarations — the mechanism working correctly, and still a cost
   somebody pays. This is the cheapest hour it will ever be.
2. **No result can change verdict.** All 300 stored results are `survivor_only` *and*
   `carry_unmodelled`, so each already carries `universe_basis_not_survivorship_free`
   independently. ⚠ Stated precisely, because the loose version is false: every row's
   refusal *tuple* GAINS the `fx_unmodelled` code — what is unchanged is every row's
   promotability *verdict*, which was already "refused" and stays "refused". Acceptance
   below checks the verdict over every physical row rather than inferring it.

## What changes

**Vocabulary.** `PromotionRefusal` gains `fx_unmodelled`. Same argument as the three
`synthetic_control_*` codes and as `deflated_sharpe_not_computed` vs
`effective_sample_size_not_computed`: each names a different broken thing and a different
operator action. An operator reading a refusal census needs to know which is blocking.

**Constants.** `cost_model.CARRY_UNMODELLED` becomes `CARRY_BPS is None` alone; a new
`FX_UNMODELLED` is `FX_BPS is None`. Both stay derived, never hand-written. ⚠ They are
derived at IMPORT, so a test that monkeypatches `CARRY_BPS` does not flip them — the
independence tests therefore exercise `structural_promotion_refusals` with explicit
arguments, which is the function the gate and the freeze both call anyway.
`CALIBRATION_LIMITS[3]` is reworded, since "carry and FX are unmodelled" as one limit
stops being true the moment either half can close alone.

**Stamp.** `StrategyResult` gains a required `fx_unmodelled: bool` beside
`carry_unmodelled`, same as-at-compute-time contract — a gate must never re-read today's
module constant to judge an old row. Required and undefaulted so every writer states it.

**Declaration.** `PreregDeclaration` gains `declared_fx_unmodelled`, inside the `sha256`
payload — a declaration that cannot name FX separately cannot pre-declare the refusal set
it will produce, which is the whole function of the freeze. The declared-vs-actual
substitution check gains an FX twin (`declared_fx_unmodelled_substituted`); note those
substitution codes are free strings on `PreregDeclarationRefused`, not members of
`DeclarationRefusal`, so no second vocabulary changes.

**Policy version.** `STRUCTURAL_REFUSAL_POLICY_VERSION` bumps — the rule changed, not the
comments. ⚠ Consequence the first draft of this spec did not state: the bump is read by
`strategy_live_gate.assess_live_gate` as well as by the research path, so any trial
holding a frozen declaration would also start failing the live gate with
`declaration_no_longer_coherent`. Zero declarations exist, so nothing is affected today;
the test fixtures that freeze one must move to the new policy version and the 3-code
expectation.

**Consumers.** All of them, enumerated rather than sampled — `structural_promotion_refusals`
and `check_promotable`; the ledger's `_RESULT_COLUMNS` / `_RESULT_VALUES` / `_row_params` /
`_result_from_row` positional tuple, which must move together or the round trip silently
mis-decodes; the declaration insert/select and their positional row indexes; the promotion
predicates in `strategy_paper_executor` and the independently-duplicated one in
`strategy_monitoring`; `app/api/strategies.py`'s hand-rolled refusal reconstruction; the
refusal label map in `frontend/src/pages/StrategiesPage.tsx`; `backtest_run`; and the
`scripts/` writers and probes that construct results or declarations.

**Migration** (`sql/335`). ⚠ `strategy_results` is a VIEW over `strategy_results_store`,
not a second table — the column is added to the STORE, the view is recreated with
`SELECT *` and its cascaded check option restored (`sql/326`'s sequence), and
`idx_strategy_results_promotable_basis` — which lives on the store — is dropped and
recreated with `AND NOT fx_unmodelled` in its predicate. `fx_unmodelled BOOLEAN NOT NULL DEFAULT
TRUE` backfills the 300 existing rows, and `declared_fx_unmodelled BOOLEAN NOT NULL
DEFAULT TRUE` does the same on the declarations table. All in one transaction.

⚠ **The backfill is measured, not imputed.** A stored `carry_unmodelled = true` only proves
`CARRY_BPS is None OR FX_BPS is None`, so writing `fx_unmodelled = true` would ordinarily be
an assumption. It is not: `git log -S"CARRY_BPS" -- app/services/cost_model.py` returns
exactly one commit (`c3ee15f0`, the phase-5b introduction), so no version of that module has
ever held a non-NULL value for either. Both `cost_model_id`s present on the store —
`static-p75-insession-v1` and `static-p75-insession-v2+split-adjusted-max` — therefore
charged neither component. The argument is over the SET of ids, not their row counts, so
no count is written here; `select cost_model_id, carry_unmodelled, count(*) from
strategy_results_store group by 1,2` is the check.

⚠⚠ **The defaults are KEPT, departing from every other stamp column here.** The first draft
dropped them so a writer had to state the stamp. Measured before committing to that:
`strategy_backtest_run` has 13 rows in `job_runs`, most recent 2026-08-12T18:56Z — a LIVE
job. Between this migration applying and the daemon picking up the new code, an old writer
would insert without the column and hit a NOT NULL violation. Keeping the default is
rolling-safe and costs nothing real: `TRUE` is the fail-closed value, it is *truthful* for an
old writer (which charges no FX), and explicitness is enforced where it belongs —
`StrategyResult.fx_unmodelled` is required and undefaulted, so every writer going through
Python must still state it. The database default only ever catches a raw-SQL writer, and for
that caller fail-closed beats a constraint error. This also makes the migration
population-independent: `ADD COLUMN ... DEFAULT` is catalog-only on PG11+, so it fires no row
trigger and would behave identically against an environment that already holds declarations.

⚠ The index predicate is a coarse candidate filter, not the gate — many refusals are
deliberately absent from it. It is widened here only so it cannot offer a row the
structural rule now refuses.

## The hazard checkpoint 1 found, and what closes it

Codex's first finding on this spec was the important one, and it is verified rather than
taken on trust: **`CARRY_BPS` and `FX_BPS` are charged nowhere.**
`grep -rn "CARRY_BPS\|FX_BPS" app scripts tests` returns only the module itself, the two
derived flags, `__all__`, one verify-script printout and a test asserting they are `None` —
no price, no return and no equity mark adds either. So setting one to a measured number
would clear its promotion refusal *without charging the cost*, making every result under
that model promotable while modelling exactly what it modelled the day before. That is
#2286's shape aimed straight at the gate.

The split does not create this — the coupled flag has the same property — but it doubles
the number of ways to trip it, so this is the right place to close it.
`cost_model._check_unmodelled_components_are_not_charged` runs at IMPORT, beside
`_check_bands_are_total`, for that function's stated reason: it guards an edit somebody
makes to the literal above it, so it belongs beside the literal rather than in a test file
they may not run. Removing the guard is part of the work of charging a component — charge
it, ship a new `COST_MODEL_ID`, then delete the clause — not a prerequisite to be waived.

## What does NOT change

- **`CARRY_BPS` and `FX_BPS` stay `None`.** This splits a refusal; it does not measure a
  cost. Nothing becomes promotable, and both refusals still fire on every row.
- **`COST_MODEL_ID` does not bump**, because no charged amount moves. ⚠ But the converse
  must be written down where it will be read, and is: when either flag is later closed by
  setting its bps, that IS a change to what is charged, so `cost_model.py`'s existing rule
  applies and a new `COST_MODEL_ID` is required. Without that, a recomputation under
  measured costs would collide with the old `(strategy_id, strategy_version,
  result_version)` row while charging materially different costs.
- **`ResultIdentity.version` does not move.** Neither stamp is in the identity payload —
  `universe_basis` is excluded by name as "an OBSERVATION about the corpus, not a knob
  somebody set", and these are the same kind of thing. The claim is checked, not argued:
  the ledger round-trip test re-derives the hash for all stored rows.
- **The live path.** Verified this run, not assumed: `execution_guard._check_transaction_cost`
  refuses on every branch unless `missing_cost_components` is empty, and all 43 active
  `cost_model` rows are `(carry_cost_known=f, fx_cost_known=f)`. #2535 already closed the
  silent-zero defect this ticket was re-scoped around; no live-path zero remains to fix.

## Acceptance

1. `structural_promotion_refusals` returns the two codes independently — all four
   `(carry, fx)` states are asserted, not only the two single-clear transitions.
2. A declaration frozen under the superseded policy version is refused with
   `structural_refusal_policy_superseded`.
3. A result whose `fx_unmodelled` disagrees with its declaration is refused with
   `declared_fx_unmodelled_substituted`, and the carry twin still fires independently —
   each probed with the other removed, so neither masks the other.
4. The promotion predicates in `strategy_paper_executor` and `strategy_monitoring` require
   both flags false.
5. The ledger round trip preserves both stamps (positional decode pinned).
6. Post-migration, over EVERY physical row: `fx_unmodelled` is true, and the promotability
   verdict is unchanged from the pre-migration verdict for that row.
