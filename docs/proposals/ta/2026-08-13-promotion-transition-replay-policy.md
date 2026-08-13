# The promotion transition's replay policy (#2625)

Status: proposed, 2026-08-13. Refs #2625, #2621, #2505, #2437.

#2621 made `promote_strategy` replay ONE of `check_promotable`'s inputs (the
universe) from a frozen record. #2625 asks for the rest. The blocker the
remaining inputs share is a policy, not five schemas: **their temporal rules
diverge, and nobody has written down which input replays against what.**

## Source rule

No external regulator governs this; it is our own settled machinery, so the
governing rules are cited from the repo rather than from a standard:

- `docs/settled-decisions.md` → "v1 strategy capital universe is US-only",
  which splits *"result production — enforced"* from *"the promotion
  transition — enforced since #2621"* and records the frozen-at-result-time
  choice and its three reasons.
- `app/services/strategy_result_universe.py` module docstring — the frozen
  rule, and the allowlist shape (`RECOGNISED_UNIVERSE_RULE_VERSIONS`).
- `docs/review-prevention-log.md` §"a temporal word in the requirement" — *"if
  the requirement contains one and the implementation is a parameter, the
  implementation does not meet it. What does: a persisted row, with its own
  timestamp and a digest over its bytes, loaded by identity at enforcement
  time."*
- §3.4 of the catalogue spec for the ambiguity-arm rule; criterion 5 for the
  hold-out access rule; criterion 9 for the quarantine arm pair.

## Measured before speccing (dev, at `126ab076`, this session)

```
strategy_results_store                      324   (hold_out 300 / in_sample 24)
  by quarantine_arm                         masked 162 / admitted 162
  by ambiguity_arm                          best_case 162 / worst_case 162
  by (universe_basis, carry, fx)            ('survivor_only', true, true) x 324
strategy_result_universe                      0   -> all 324 refuse evaluated_universe_unrecorded
strategy_promotions                           0
strategy_promotion_evidence                   0
strategy_preregistration_declarations         0
strategy_holdout_accesses                   304   (evaluate 300 / read 4)
```

Nothing is promotable today and nothing has ever been promoted, so every change
below is measured against a vacuous population. That is stated rather than
hidden: it means **no test here can be defended as "it passes on live data"**,
and the invariants have to be defended by revert-probe instead.

## Two corrections to #2625's inventory table

The issue's table is a prior session's conclusion, so it was re-falsified
before use (working-order 3c). Two rows are wrong:

1. **"hold-out evaluation/access counts — reading RECORDS an audited access"**
   conflates two functions. `result_ledger.holdout_access_counts` (`:1441`) is
   two pure `COUNT` statements and records nothing. The function that records is
   `quarantine_arms_compared` (`:1547`), and only on a `hold_out` identity —
   an `in_sample` identity records nothing, by an explicit branch.
2. The natural follow-on claim — *"so a replaying transition corrupts the gate's
   own input"* — is **false**. The recorded access is `access_kind='read'`, and
   `_COUNT_EVALUATE_ACCESSES` (`:422`) filters `access_kind = 'evaluate'`. A
   replay-induced read pollutes the audit trail; it does not move
   `recorded_accesses` and cannot change the verdict. The argument against
   re-reading is governance (the log becomes a count of our own automation),
   not arithmetic.

## The policy — three rules, and every gate input classified

The policy is keyed on `PromotionCandidate`'s **fields** — the gate's actual
inputs — not on refusal codes. Codes are a many-to-many projection of the
inputs (one input emits several codes; several inputs emit one), so a policy or
a guard keyed on codes can be satisfied while an input goes unclassified.

| gate input (`PromotionCandidate` field) | rule | why |
| --- | --- | --- |
| `evaluated_instrument_ids`, `validated_universe_ids` | **FROZEN** | #2621, settled. Today's `is_tradable` would let a later delisting retroactively invalidate a passing result. |
| `ambiguity_material` | **FROZEN** | property of an arm pair measured at result time; nothing about it is re-derivable later. This ticket persists its inputs. |
| `result` (the row's own stamps, metrics, deflation, control) | **FROZEN** | already columns on `strategy_results_store`; immutable once written. |
| `promotion_evidence` — cost staleness clause | **TODAY** | the record DECLARES a validity window (`cost_observed_on` / `cost_valid_through`), so a today-check is the declared rule, not an invented freshness one. Already implemented (`as_of=date.today()`). |
| `promotion_evidence` — every other clause | **FROZEN** | properties of the stored record, compared against constants. |
| `result.deflated` vs the trial register | **TODAY** | the register is deliberately supersedable — a newly declared trial count must invalidate an old deflation. Already implemented via the module constants. |
| `holdout_evaluations`, `recorded_accesses` | **NOT RE-READ** | see the gap below. |
| `quarantine_arms_compared` | **NOT RE-READ** | reading it on a `hold_out` identity writes a `read` row into the log criterion 5 audits. 300 of 324 stored rows are `hold_out`. |

**FROZEN / TODAY / NOT_RE_READ is the whole vocabulary.** A new gate input must
be classified into one of the three, and the coupling guard enforces that.

### ⚠ NOT_RE_READ is a GAP, not a solution — state it plainly

An input that is neither persisted nor re-read is an input **the transition does
not enforce at all**. It is not "safely skipped": for the two hold-out clauses
and criterion 9, `promote_strategy` is still trusting a write-time verdict that
died with `WrittenRow` — precisely the defect #2621 was filed about, surviving
in the two places where re-reading has a governance cost.

This ticket does not close that. Closing it means persisting a *derived* count
at result time (the #2621 move), which is its own decision about a
**history-dependent** input: both counts are scoped to
`(strategy_id, strategy_version)`, not to a result, so a later hold-out
evaluation retroactively changes what a replay of an *older* pinned result
would see. Frozen-at-result-time and today's-count genuinely differ here, and
neither is obviously right. Recorded as an explicit open item rather than
answered silently, and filed as a follow-up.

⚠ Related, and also not fixed here: `holdout_access_counts` runs its two
`COUNT`s as separate statements, so under a concurrent writer it can return a
pair that never simultaneously existed.

## Item 3 — does `promote_strategy` replay full `check_promotable`?

**No. Per-input replays, assembled in one declared place.** The reasons are
structural rather than stylistic:

1. `check_promotable` takes a `PromotionCandidate` carrying a whole
   `StrategyResult`. The only code that reconstructs one from the store is
   `result_ledger._result_from_row` (`:611`), reachable through exactly one
   public function, `read_holdout_results` (`:1408`) — which records an access
   **before** it reads, deliberately and documented. There is no in-sample
   reader at all. So "replay the full gate" requires either a new reader or the
   audited one, on a population that is 92.6% `hold_out`.
2. The gate has no single as-of. Two of its inputs are FROZEN and two are
   TODAY; a single `check_promotable(candidate)` call cannot express that, and
   collapsing them would silently pick one.
3. `check_promotable` is the **write-time** gate and must stay pure and
   database-free (its docstring and phase 7's order-path guard both depend on
   that). The transition's replay is a different function with a different
   input source, and conflating them is what would drift.

So the transition keeps gathering per-input refusals — but the classification
above stops that being "bolting inputs on one at a time", which is what #2625
warns against.

## Scope of this change

1. **`sql/339_strategy_result_ambiguity.sql`** — one immutable, hashed row per
   result, mirroring `sql/334`: `result_id` PK, `ambiguity_rule_version`,
   `comparison_basis`, the two arm Sharpes, `payload_sha256`, `created_at`, and
   an UPDATE/DELETE-refusing trigger.

   ⚠ **The record stores the INPUTS, not the verdict.** A stored boolean is
   unauditable — there is no way to check it. Storing the two Sharpes, the
   basis and the threshold means the verdict is re-derived by a pure function at
   replay, exactly as #2621 re-derives `evaluated ⊆ validated` from the two
   frozen sets rather than storing a boolean.

   ⚠ **`cohort_gap_threshold` IS a column, and is NULL on everything this
   runner writes.** The first draft omitted it on KISS grounds; that was wrong
   for two reasons found at checkpoint 1. Without it the verdict `True` is
   *unreachable*, so the transition could never emit `ambiguity_material` and
   the branch could not be tested at all; and a result genuinely measured
   against a cohort — which §3.4 requires and the synthetic-control work will
   supply — could not be recorded faithfully without a second migration. The
   column is read by the derivation and exercised by tests, so it is not a dead
   column; it is an unpopulated one, which is the honest state.

   ⚠ **`comparison_basis` is load-bearing and not inferable from the Sharpes.**
   `_ambiguity_material_for` returns `False` from the mere PRESENCE of a
   matching measurement whose `ambiguity_arm is None`, before any Sharpe is
   read and regardless of their values. Two equal Sharpes are NOT equivalent
   evidence — that cannot distinguish one copied shared measurement from two
   independently-evaluated equal arms. So the basis is stored explicitly and
   checked FIRST in the derivation, preserving the precedence.

2. **`app/services/strategy_result_ambiguity.py`** — `AmbiguityRecord`,
   `record_sha256`, `store_result_ambiguity`, `load_result_ambiguity`,
   `ambiguity_verdict` (pure), and `ambiguity_promotion_refusals`.

   ⚠ **Two layers, deliberately, because `None` means two different things.**
   `load_result_ambiguity` returns `AmbiguityRecord | None` where `None` is
   ABSENCE; `ambiguity_verdict(record)` returns `bool | None` where `None` is
   NOT-COMPARED. Collapsing them would make an unrecorded row indistinguishable
   from a measured-but-unjudged one, which is the same "not measured" versus
   "measured and bad" collapse `check_promotable` already refuses to make. All
   four states map to distinct outcomes and each is tested independently:

   | state | refusal |
   | --- | --- |
   | record absent | `ambiguity_verdict_unrecorded` |
   | verdict `None` | `ambiguity_arms_not_compared` |
   | verdict `True` | `ambiguity_material` |
   | verdict `False` | none |

   ⚠ **Corruption RAISES; absence REFUSES** — the #2621 asymmetry, kept for
   consistency. The cost is real and is named: a corrupt ambiguity record
   aborts before the universe and evidence refusals are gathered, so it MASKS
   them. That is the intended precedence (an integrity failure is not a
   governance verdict) but it does mean "all refusals gathered before raising"
   holds only among non-corrupt records.

   ⚠ **Governing rule for the derivation.** §3.4 fixes the materiality
   comparison (arm gap versus the random cohort's 95th-percentile gap). The
   treatment of the cases §3.4 does not reach — a shared measurement, and a
   missing cohort — is fixed by
   `app/services/backtest_run.py::_ambiguity_material_for`, which is the
   settled current implementation and is cited as the source rather than
   re-derived: shared measurement ⇒ `False`; equal Sharpes ⇒ `False`; missing
   Sharpe or absent cohort ⇒ `None`.

   ⚠ **What the record does NOT reproduce.** `_ambiguity_material_for` also
   RAISES on malformed arm structure (arms present that are not exactly the two
   declared ones; a namespace missing from an arm) and silently collapses
   duplicate same-arm measurements via a dict comprehension. Those are
   properties of the in-memory arm collection, not of a two-scalar record, and
   the record cannot and does not reproduce them. They stay enforced at write
   time, where the arms exist.

   ⚠ **Sibling consistency.** The verdict is a function of
   `(strategy_id, quarantine_arm, namespace)` and NOT of `ambiguity_arm` — so
   the two result rows differing only in ambiguity arm must carry identical
   records. That holds by construction today (one call, same inputs) and
   nothing enforced it, so the writer asserts it and a test pins it.

3. **`app/services/strategy_promotion_replay.py`** — the policy as data:
   `REPLAY_TEMPORAL_POLICY: Mapping[str, ReplayRule]` naming every
   `check_promotable` input and its rule, with the reason on each.

4. **Wiring** — `backtest_run._write_rows` stores the ambiguity record beside
   `_store_universe_record`, in the pair's own transaction; `promote_strategy`
   replays it beside the universe replay, gathering both refusal lists before
   raising so one missing input cannot mask the other.

5. **The coupling guard (the load-bearing test).** A pure test asserting that
   every **field of `PromotionCandidate`** is classified in
   `REPLAY_TEMPORAL_POLICY`, read via `dataclasses.fields()` rather than a
   hand-written list. Adding a gate input without deciding its temporal rule
   then fails a test that names this document. This is the #2612 shape: the
   control that fires on the edit a future author will actually make.

   ⚠ **Keyed on fields, not on refusal codes** — checkpoint 1 killed the first
   version, which asserted over refusal codes. Codes are a many-to-many
   projection of the inputs, so a new input that reuses an existing code would
   have passed a code-keyed guard while going unclassified. Fields are the
   structural key a new gate input actually arrives as.

## What this does NOT do

- It does not make anything promotable. All 324 stored rows still refuse on
  `universe_basis_not_survivorship_free`, `carry_unmodelled`, `fx_unmodelled`
  and `evaluated_universe_unrecorded`, and this adds a fifth
  (`ambiguity_verdict_unrecorded`) to every pre-existing row.
- It does not backfill. Pre-#2625 rows have no ambiguity record and refuse —
  the same fail-closed direction #2621 chose, for the same reason: a
  reconstructed record would be a claim about a measurement nobody made.
- It does not implement §3.4's cohort threshold — no writer supplies one, so
  every record this runner writes has `cohort_gap_threshold IS NULL`.
  ⚠ It is NOT claimed that every current row's verdict is `None`: the verdict
  is computed in memory from the arm collection and is not stored anywhere, so
  the distribution over the 324 existing rows cannot be measured without
  re-running the harness. The first draft asserted it; checkpoint 1 caught that
  it was never computed. What IS measured is that all 324 rows lack an
  ambiguity record and therefore refuse `ambiguity_verdict_unrecorded`.
- It does not enforce the two hold-out clauses or criterion 9 at the
  transition — see the NOT_RE_READ gap above.
- It does not touch `check_promotable` itself.
