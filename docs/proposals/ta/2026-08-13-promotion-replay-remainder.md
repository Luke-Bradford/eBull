# Closing the promotion transition's remaining replay gaps (#2639)

Follow-up to #2625, which declared `REPLAY_TEMPORAL_POLICY` and closed the universe,
ambiguity and structural-stamp halves. `unenforced_candidate_fields()` still returns
`{holdout_evaluations, recorded_accesses, quarantine_arms_compared}`, and the `result`
entry carries a `⚠ PARTIAL` — the deflation, effective-sample-size and synthetic-control
clauses are columns on `strategy_results_store` and are not replayed.

Target: `unenforced_candidate_fields() == frozenset()` and the PARTIAL note gone.

⚠ **No migration.** The first draft of this spec proposed one (`strategy_result_quarantine_pair`,
a per-result pointer at the sibling arm). Codex checkpoint 1 killed it — see Half B.

## Measured first (dev, at branch point `36c97f18`, 2026-08-13)

```
strategy_results_store                                        324
  namespace = 'hold_out'                                      300
  namespace = 'in_sample'                                      24
  purpose   = 'harness_validation'                            324
  quarantine_arm = 'masked' / 'admitted'                  162 / 162
  with a stored flipped-arm sibling                           324
strategy_holdout_accesses                                     304
  access_kind = 'evaluate'                                    300
strategy_promotions                                             0
strategy_promotion_results                                      0
strategy_result_universe                                        0
strategy_result_ambiguity                                       0
distinct (strategy_id, strategy_version)                        8
rows with dsr_model_id                                        268
rows with effective_sample_size                               324
rows with synthetic_control_model_id                            0
```

Per `(strategy_id, strategy_version)`, hold-out rows and `evaluate` accesses are equal on
all eight (52/52, 32/32, 32/32, 52/52, 52/52, 32/32, 16/16, 32/32). So criterion 5's count
clause passes today on every strategy version, and every result still refuses on
`universe_record_missing`, `ambiguity_verdict_unrecorded`, `synthetic_control_not_run`,
`harness_validation_only` and the structural stamps. Nothing is promotable and nothing
becomes promotable here — this is enforcement, not admission.

**Full-population reconstruction check.** All 324 rows rebuild through
`result_ledger._result_from_row` without raising (324 ok / 0 failed), which is the read
path Half C adds. Re-derive with the probe in `scripts/verify_2639_promotion_replay.py`.

## Half A — the hold-out counts. The temporal rule, decided.

⚠ **Correcting the inherited premise first.** #2625's policy entry says re-reading is safe
and that is right: `result_ledger.holdout_access_counts` (`:1441`) is two pure `COUNT`
statements, and the function that RECORDS is `quarantine_arms_compared` (`:1547`), only on
a `hold_out` identity. Re-verified at `36c97f18`.

**Decision: `today`.** Both counts are read live at the transition, not frozen at result
time.

The argument is not preference, it is that **frozen defeats criterion 5**. The counts are
scoped to `(strategy_id, strategy_version)`, so a count frozen when result #1 was written
records the hold-out looks that had happened *by then*. A strategy that later evaluates its
hold-out four more times without recording an access would replay result #1 as `(1, 1)` —
consistent, promotable, and blind to precisely the repeated unlogged look criterion 5
exists to catch. The retroactivity that #2621 rejected for the universe is the desired
behaviour here: a later unrecorded evaluation SHOULD block a promotion that has not
happened yet.

The transition is also the counts' natural scope: `promote_strategy` promotes a
`(strategy_id, strategy_version)`, which is exactly what the counts are keyed on.

⚠ **Three consequences, stated rather than discovered later.**

- **The clause is strategy-version-wide, not per-result.** One unrecorded hold-out
  evaluation blocks promotion of *every* result of that version, across namespaces, scopes
  and windows. That is `check_promotable`'s own behaviour — the field is on
  `PromotionCandidate`, not on `StrategyResult` — and the transition inherits it rather than
  inventing it.
- **The rule heals as well as blocks.** A missing access recorded later, or a hold-out
  evaluation that arrives after a version had none, moves the answer from refuse to pass.
  Criterion 5 asks that every evaluation carry a record; it does not ask when the record was
  written.
- **Reading it live changes the answer as accesses are inserted, not only as evaluations
  are.** Both sides move.

### This widens the `today` restriction, deliberately and narrowly

`REPLAY_TEMPORAL_POLICY`'s current rule is that `today` is legitimate only where the stored
record "DECLARES a validity window or is explicitly supersedable", and
`TestTodayIsRestricted` pins `today == {"promotion_evidence"}`. That wording named the two
shapes that existed; its actual content is that **a today-check must name what can change
the answer, and that change must be one the criterion wants to invalidate on.** What it
guards against is an *undeclared* freshness rule.

Three qualifying shapes, not two:

1. the record declares its own validity window (`promotion_evidence`:
   `cost_observed_on` / `cost_valid_through`);
2. the record is explicitly supersedable;
3. **the record is an append-only AUDIT LOG, the clause is a comparison between that log
   and the rows it audits, and the criterion the clause serves requires the comparison to
   be current** (`holdout_evaluations` / `recorded_accesses` against
   `strategy_holdout_accesses`).

⚠ Shape 3 is written this narrowly on purpose. "A ledger of our own conduct" — the first
draft's wording — would admit any mutable operational counter, which is most of the
database. The three clauses together are the restriction: an audit log, a
log-versus-audited-rows comparison, and a criterion that asks for currency.
`TestTodayIsRestricted` still pins the exact member set, so a fourth today-check fails the
test whatever it claims about itself.

### Snapshot: what is and is not guaranteed

Two `COUNT`s as two statements is two READ COMMITTED snapshots, so a concurrent
`store_holdout_result` between them can return a pair that never simultaneously existed —
and the direction that matters is `accesses < evaluations`, a false
`holdout_accesses_unrecorded`. Fixed: one statement, two scalar subqueries, one snapshot.

⚠ **That does not make the counts atomic with the promotion INSERT, and this spec does not
claim it does.** `_lock_strategy` takes an advisory lock keyed on the strategy version;
`store_holdout_result` does not take it, so a hold-out write may commit after the count
statement and before the promotion row lands. The promotion is then decided on counts that
were true when read. Closing that would mean putting the same advisory lock on the hold-out
writers, which is a change to the write path and outside this ticket — recorded on #2639 as
a known bound rather than silently assumed away.

## Half B — criterion 9's quarantine comparison

**Re-derived from the two result rows, not from a new record. No migration.**

Half C already rebuilds the pinned row through `_result_from_row`, which yields its
`ResultIdentity`. The sibling arm's `result_version` is that identity with
`quarantine_arm` flipped — a pure hash, the same derivation
`result_ledger.quarantine_arms_compared` performs — so the presence of both arms is one
`COUNT` over `strategy_results_store` and costs nothing extra.

⚠ **Why not the pointer table the ticket proposed.** A per-result
`sibling_result_id` lets the WRITER choose which sibling to name. The transition would then
verify the sibling it was handed, not that the named sibling is the only one the identity
admits — strictly weaker than today's derivation, which cannot be pointed anywhere else
because the version is a hash of the identity. Codex checkpoint 1 raised this along with
asymmetric pointers, multiple candidate siblings, and the fact that the record would
witness a pointer insertion rather than a pair write. Dropping the table removes all four.

⚠ **The transition does NOT call `result_ledger.quarantine_arms_compared`.** On a
`hold_out` identity that function records a `read` access, and 300 of 324 stored rows are
`hold_out`, so a transition calling it would write one audit row per promotion attempt into
the log it is auditing — the prevention-log rule *"it must not ask the database a question
it is the answer to"* (`docs/review-prevention-log.md`, the `_candidate` entry), one layer
further out. Instead the counting half is extracted into `quarantine_arm_pair_present`,
which records nothing, and `quarantine_arms_compared` records and then calls it. One copy
of the count; two doors, and the door that records stays the one criterion 5 governs.
Promotion is not an evaluation of the withheld side.

⚠ **Rule: `frozen`.** Both arms' rows are records written at result time, and
`strategy_results_store` has no delete path — so the derived answer is monotone, moving
only from "one arm" to "both arms" when a sibling with the identical identity-minus-arm is
stored. Nothing about today's world enters it.

Refusal code: `quarantine_arms_not_compared`, the gate's own. No new code, because there is
no new record whose absence needs its own name.

## Half C — the row's own remaining clauses

`check_promotable`'s purpose / criterion 6 / criterion 3 / §9 blocks read `result.purpose`,
`result.deflated_sharpe`, `result.trial_count`, `result.deflated`,
`result.metrics.effective_sample_size` and `result.synthetic_control`. Every one is a column
on the row the transition already pins.

Three pure functions extracted from `check_promotable` **and called by it**, so there is one
copy — the `structural_promotion_refusals` move:

- `purpose_promotion_refusals(purpose)` → `harness_validation_only`
- `deflation_promotion_refusals(*, deflated_sharpe, trial_count, deflated, effective_sample_size)`
  → `deflated_sharpe_not_computed`, `trial_count_undeclared`, `trial_register_superseded`,
  `effective_sample_size_not_computed`
- `synthetic_control_promotion_refusals(control)` → `synthetic_control_not_run`,
  `synthetic_control_cohort_shows_edge`, `synthetic_control_sharpe_below_cohort`

⚠ Three functions and not one, because the blocks are **not contiguous** in
`check_promotable` — the universe and hold-out clauses sit after purpose, and §3.4's
ambiguity and criterion 9's sit between deflation and §9. Merging them would reorder the
returned refusals, and the order is the spec's.

⚠ Each helper keeps `check_promotable`'s body VERBATIM: three independent `if`s in the
deflation helper (never `elif`, never an early return), `trial_register_superseded` guarded
on `deflated_sharpe is not None` and not on `deflated`, and the two §9 codes independent so
both can fire.

### The row's own `purpose` is a second axis, and it is not currently checked

`promote_strategy` refuses on `registered_strategy_purpose(strategy_id)` — the MANIFEST's
purpose. The row carries its own stamped `purpose`, and nothing compares them. Measured: all
324 stored rows are `harness_validation`, and all four registered strategies are
`harness_validation` today, so they agree. The moment a strategy's manifest entry becomes
`capital_candidate`, its existing `harness_validation`-stamped rows become pinnable and the
transition would not refuse them, while `check_promotable` would. Same shape as the seven
M9 defects: the control exists on a path the decision does not take. Closed by replaying the
row's own purpose.

### The read path

`result_ledger.stored_result_promotion_refusals(conn, result_id)` selects `_RESULT_COLUMNS`,
rebuilds through the existing `_result_from_row`, and returns **only refusal codes** — the
purpose, deflation and §9 blocks, in `check_promotable`'s order, plus the arm-pair clause
from Half B.

⚠ **It returns codes and never a `StrategyResult`, deliberately.** A public
`load_result_by_id` would be a new unaudited door to the withheld side —
`read_holdout_results` is the sanctioned one and it records the access first. Keeping the
withheld numbers inside `result_ledger` and handing the transition a refusal list means the
new function cannot become that door.

⚠ Reading the row's own columns for a result the caller has already pinned is not a new
governance cost: `promote_strategy` already selects six columns off these rows, and
`holdout_access_counts` already counts them, neither recording an access.

⚠ **`_result_from_row` RAISES where `check_promotable` refuses**, on three states: a
`result_version` that does not match the identity it carries, a `synthetic_control_passed`
that disagrees with its own stored inputs, and a partially-written DSR or control block.
That is the `load_result_ambiguity` precedent — corruption is an integrity failure to
surface loudly, not a gate verdict to report politely — and it carries the same named cost:
the raise aborts before the other refusals are gathered, so it masks them. Verified on the
full population: 324/324 rows reconstruct, so no stored row takes that path today.

⚠ **The transition keeps its own read of the structural stamps and does NOT source them
from the rebuilt object.** `_result_from_row` coerces `carry_unmodelled` with `bool(...)`,
so a NULL would read as *modelled* — fail-open on a Tier 1 refusal. The transition's
existing read coerces NULL to `True`. Both columns are `NOT NULL` in `sql/262` and
`sql/335`, so this is defence in depth, but the failure direction is silent and the two
coercions must not be collapsed onto the weaker one.

### `result` stays `frozen`

`trial_register_superseded` compares the row's stored `trial_register_version` /
`declared_trials` against the CURRENT `TRIAL_REGISTER_VERSION` /
`TRIAL_REGISTER.declared_count`. That is a frozen field compared against a code constant,
which the `promotion_evidence` entry already declares does not make a field `today` — what
makes that one `today` is that its replay depends on the current DATE. A register
supersession bumping a constant and invalidating older results is the deliberate
supersession behaviour `check_promotable` already has.

⚠ `result` is a coarse policy key — one field carrying eight clauses. #2625 chose to key on
`PromotionCandidate` fields because that is the structural shape a new gate input arrives
as, and that stands. The mitigation is that the entry's reason now enumerates the clauses
and says which function replays each, so "the field is replayed" is checkable rather than
asserted.

## Acceptance

1. `unenforced_candidate_fields() == frozenset()`; `TestTheGapIsCounted` updated, not
   deleted.
2. The `result` entry's `⚠ PARTIAL` note is gone.
3. `TestTodayIsRestricted` pins `{promotion_evidence, holdout_evaluations,
   recorded_accesses}` with each member's qualifying shape.
4. `check_promotable`'s returned refusal tuples are unchanged by the extraction, pinned by
   the existing `tests/test_strategy_result.py` suite plus a test asserting each helper's
   output is a contiguous slice of the gate's.
5. Full-population: every stored row reconstructs, and `promote_strategy` exercised against
   dev inside a rolled-back transaction shows the new refusals firing.
6. `docs/settled-decisions.md`'s #2625 entry amended: the gap set is empty and the `today`
   restriction has three shapes.
