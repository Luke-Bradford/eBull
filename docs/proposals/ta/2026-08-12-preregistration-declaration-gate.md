# Preregistration declaration gate (#2599)

Research-side counterpart to the runtime funding gate. Sealing outcomes on a
trial that cannot promote must be a **declared** falsification, never incidental.

## Measured premise (dev DB, 2026-08-12, this session)

```
select universe_basis, carry_unmodelled, count(*) from strategy_results_store group by 1,2;
--  survivor_only | t | 244        (single group)
select purpose, count(*) from strategy_results_store group by 1;
--  harness_validation | 244
select count(*) from strategy_live_gate_policies;   --  0
select count(*), count(distinct strategy_id) from strategy_holdout_accesses;  -- 224 | 4
select count(*) from strategy_deployments;          --  0
```

Every stored result is structurally unpromotable. ⚠ These counts describe **this
dev database at this moment** and are therefore not a licence to write a
migration that only works on an empty table — see "Migration safety" below,
where the design is made independent of them.

## Source rule

No external regulator governs this; the governing rules are our own settled
ones, cited rather than re-derived:

- `strategy_result.py:768` `check_promotable` — `universe_basis_absent` /
  `universe_basis_not_survivorship_free` / `carry_unmodelled` are the three
  refusals whose outcome is **fully determined at freeze time** by stamps the
  run will carry. Everything else in the vocabulary depends on the run.
- Runtime funding gate: `strategy_paper_executor.py:283,294` and
  `strategy_monitoring.py:435` require `universe_basis = 'survivorship_free'`.
- `StrategyResult.purpose` is a closed two-value vocabulary
  (`strategy_result.py:557`). It is **not** extended; the declaration is a
  contract-level field.
- `trial_register_superseded` (`strategy_result.py:251`) is the settled
  precedent for "a frozen artefact computed under a superseded policy is
  refused, not re-interpreted". The structural-refusal policy version reuses
  that shape rather than inventing one.
- Prevention-log entry for #2600 (Gate D-0.1): *"if you cannot name the sentence
  or table that evidences arm n, arm n is not in the floor"*. Applied here as:
  the expected-refusal list is **recomputed** from declared stamps and compared,
  never taken on trust; and no floor VALUE is chosen by this ticket.

## The defect the first draft had

Codex checkpoint 1 killed a caller-supplied `PreregDeclaration` dataclass passed
to `require_outcome_access`: *"a caller can construct a favourable declaration
after seeing/reading outcomes"*. A declaration that is not persisted before the
look is not a declaration. Everything below is built on a **frozen, hashed,
immutable row**, and the access path loads it by trial identity — it is never an
argument.

## What ships

### 1. `structural_promotion_refusals` — one source, two callers

`check_promotable`'s three stamp-determined refusals are extracted into a pure
function in `strategy_result.py` and called by both `check_promotable` and the
declaration check. A second hand-written copy would drift; this cannot. A test
asserts `check_promotable` still emits the same codes for the same stamps.

Versioned: `STRUCTURAL_REFUSAL_POLICY_VERSION`.

### 2. `sql/333` — the frozen declaration

`strategy_preregistration_declarations`, one row per
`(strategy_id, strategy_version)`, `UNIQUE`, with a `BEFORE UPDATE OR DELETE`
trigger raising (pattern: `sql/307_strategy_quote_observation_immutability.sql`).

| column | why |
| --- | --- |
| `prereg_purpose` | `capital_candidate` \| `falsification_only`, CHECKed |
| `structural_refusal_policy_version` | which policy the expectation was computed under |
| `declared_universe_basis`, `declared_carry_unmodelled` | the stamps the run will carry |
| `expected_structural_refusals TEXT[]` | the list, not a bool — side- and product-dependence loses the reasons |
| `min_forward_decision_dates`, `min_forward_calendar_weeks` | the forward-shadow floor, both `> 0` |
| `forward_shadow_derivation` | free text naming the power calculation the floor came from; non-empty |
| `declared_by`, `frozen_at`, `declaration_sha256` | who, when, and over what bytes |

`declaration_sha256` is over canonical JSON (`sort_keys=True`, compact
separators) of the declared fields — the same freezing pattern
`scripts/verify_2582_schedule13d_preregistration.py` already uses on a contract
file.

⚠ **The expected-refusal list is stored even though it is recomputable.** That
is the point: it records what was expected *at freeze time*, so a later change
to the refusal policy is detectable as a disagreement rather than silently
absorbed.

### 3. `app/services/prereg_contract.py` — the coherence check, pure

`declaration_refusals(decl) -> tuple[DeclarationRefusal, ...]`:

| code | fires when |
| --- | --- |
| `structural_refusal_policy_superseded` | frozen version ≠ current |
| `expected_structural_refusals_mismatch` | declared list ≠ recomputed from declared stamps |
| `ineligible_trial_not_declared_falsification` | **recomputed** list non-empty AND purpose is `capital_candidate` |
| `forward_shadow_floor_not_positive` | either floor ≤ 0 |
| `forward_shadow_derivation_missing` | derivation blank |

⚠ The purpose check reads the **recomputed** list, not the declared one. Reading
the declared list would let a falsified empty list downgrade a purpose violation
into a bare mismatch.

Malformed/unknown/blank fields raise `ValueError` at construction rather than
returning a refusal — a malformed declaration is a writer bug, which is the
convention `StrategyResult.__post_init__` already sets, and freezing is the
place it fails.

Eligible + `falsification_only` is **allowed**: a candidate voluntarily
restricting itself is strictly tighter than the rule and is never refused.

### 4. Enforcement at the existing chokepoint

Every paved door to the withheld side — `store_holdout_result`,
`read_holdout_results`, and the third caller at `result_ledger.py:929` — already
funnels through `record_holdout_access`. The declaration check goes **there**,
so no door is missed and no new convention has to be remembered.

- **A trial with no frozen declaration behaves exactly as today.** No
  retroactive invalidation: the 224 existing access rows and every current
  evaluator keep working.
- **A trial that HAS frozen one cannot escape through the old door** — the
  check re-runs `declaration_refusals` and refuses.
- `require_outcome_access(conn, access)` is the new named door for new
  evaluators: it additionally requires the declaration to exist
  (`preregistration_not_frozen`) before recording.
- `store_holdout_result` additionally refuses when the result row's actual
  `universe_basis` / `carry_unmodelled` disagree with the frozen declared
  stamps. Declaring eligible and storing survivor-only is the substitution the
  declaration exists to prevent.

⚠ Stated limit, not papered over: a direct `SELECT` against the hold-out tables
remains physically possible. This closes every path that goes through the
ledger — **which is every path that WRITES A RESULT ROW.**

⚠⚠ **CORRECTED BY #2614.** This sentence originally ended *"which is every path
we have written"*, and that clause was false on the day it was written. The
unchecked premise is that opening an outcome always goes through the ledger. A
sealed study that computes its own statistics from raw price windows and emits a
signed artifact stores nothing in `strategy_results_store`, so there is no ledger
call to intercept — and three such scripts existed
(`scripts/evaluate_2582_schedule13d_outcomes.py`,
`scripts/verify_2476_pead_outcomes.py`, `scripts/verify_2480_insider_outcomes.py`).
C-4, the very trial that motivated this gate, was one of them.

#2614 gates C-4 explicitly (`require_outcome_access` at gate construction, then
`verify_outcome_access_provenance` at the price-window chokepoint) and adds
`tests/test_sealed_outcome_scripts_are_gated.py` so a NEW sealed opener fails by
default rather than relying on someone remembering the convention. The two
pre-cutoff `verify_*` scripts stay on that test's explicit allowlist with their
trial-register entries named. The generalisation worth carrying: **a second path
to the outcomes needs its own gate, and "we have written them all" is a claim to
verify by grep, not to assert.**

### 5. Forward-shadow floor reaches the live gate

`strategy_live_gate_policies` gains a **nullable** `declaration_id` FK.
`register_live_gate_policy` resolves the frozen declaration for the trial and
stores its id; it refuses when none is frozen, when the declaration is not
coherent, or when its purpose is `falsification_only` (a falsification trial has
no live gate to register). The floor therefore cannot be registered below the
frozen one — there is no parameter to type a different number into.

`assess_live_gate` gains one fact and three refusal codes:

- `forward_shadow_floor_missing` — the policy carries no declaration. Fail-closed
  by construction, and the reason the column is nullable rather than `NOT NULL`.
- `declaration_digest_mismatch` — the frozen declaration no longer matches its
  own digest. ⚠ Added after Codex checkpoint 2 found this path validating
  coherence but not the digest, while the outcome-access path validated both: a
  declaration edited around the immutability trigger stays perfectly coherent
  while carrying a different floor. A distinct code from "no floor was frozen",
  because "never declared" and "rewritten after declaring" are different
  operator emergencies. `register_live_gate_policy` refuses on the same
  condition, so a tampered declaration cannot bind a new policy either.
- `forward_decision_dates_insufficient` — `forward_decision_dates` below floor.
- `forward_calendar_weeks_insufficient` — `forward_days < 7 * min_forward_calendar_weeks`.

`forward_decision_dates` = `count(distinct s.signal_bar_date)` under **exactly
the predicate that already produces `forward_resolved`** — resolved outcome
present, `created_at` inside the forward window, `signal_bar_date >=` the
forward-observation date, `signal_kind='entry' AND verdict='fired'`. An
unresolved signal is not evidence, so it is not a decision date either.

⚠ **The floor counts decision dates; it does not claim they are statistically
independent.** Same-day fan-out across instruments is correlated, and dates are
not iid either — that dependence is what stage 5e-2's block bootstrap exists to
handle. The claim made here is narrower and true: a distinct-date count cannot
be inflated by firing N signals on one day, which a signal count can.

⚠ `forward_days` uses `timedelta.days`, which truncates. `forward_days >= 7 * M`
therefore requires 7M *fully elapsed* days — truncation makes the bound stricter,
which is the fail-closed direction.

**Dual enforcement, not replacement.** The existing operator-registered
`min_forward_resolved_signals` / `min_forward_days` still bind. The new floors
are contract-frozen and bind as well; both must pass.

The refusal assembly moves out of `assess_live_gate` into a pure
`live_gate_refusals(purpose, policy, declaration, facts, requested_capital)` so
the whole table is testable without Postgres.

### Migration safety

The new columns on `strategy_live_gate_policies` are **nullable**, and NULL is
read as `forward_shadow_floor_missing`. So the migration is correct on any
population, not merely on a dev database that happens to be empty today — the
empty-table measurement above is context, never the safety argument.

## Explicitly NOT in scope

**#2582's C-4 contract is not amended and no declaration is authored for it.**
`schedule13d-public-catalyst-v1.json` stays byte-identical (its verifier pins
the sha256). Its forward-shadow floor must be frozen *from its own power
calculation*, and its contract declares no prospective-shadow size — choosing
one here would be the #2600 padded-floor defect verbatim. #2437's queue step 7
assigns that authoring to #2582: *"C-4 opens as its preregistration declares it:
a falsification trial under #2599's contract."* This ticket supplies the
contract shape; #2582 supplies its values.

Consequence, stated plainly: after this merges the mechanism exists, is tested,
and has **zero** frozen declarations until a candidate freezes one — at which
point that trial's existing access path starts enforcing it.

Also out of scope, with follow-up tickets filed rather than silently dropped:

- **Refused-attempt auditing (#2611).** A refusal raises and writes nothing.
  Recording it needs the autonomous-transaction argument
  `record_holdout_access`'s docstring already sets out.
- **Forward-window splicing on re-promotion (#2612).** `assess_live_gate` reads
  `max(promoted_at)` per stage, so a re-promotion resets the forward window.
  Pre-existing, unchanged here.
- **Trial-register integration.** #2600 owns the register; a falsification
  declaration charges it exactly as any other trial does, and nothing about that
  changes here.

## Acceptance

Pure-logic tests enumerate:

1. eligible (`survivorship_free`, carry modelled) + `capital_candidate` → no refusals.
2. ineligible + `capital_candidate` → `ineligible_trial_not_declared_falsification`.
3. ineligible + `falsification_only` → allowed.
4. eligible + `falsification_only` → allowed.
5. superseded policy version → refused.
6. declared list disagreeing with the recomputed one → refused.
7. falsified empty declared list on ineligible stamps → BOTH the mismatch and
   the purpose refusal, never the mismatch alone.
8. live gate: no declaration → `forward_shadow_floor_missing`, report not `passed`.
9. live gate: dates/weeks below floor → the two respective codes.
10. `check_promotable` still emits the same three stamp refusals after the extraction.

DB-backed (one per mechanism, per the repo's lean-test rule): freeze then
re-freeze refuses; `record_holdout_access` refuses once a declaration is frozen
and the trial is incoherent; `require_outcome_access` refuses when nothing is
frozen and records when it is.
