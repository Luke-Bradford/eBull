# Preregistration supersession — a re-declaration path that cannot re-declare the terms

Ticket: #2634. Split out of #2631 item 4. Storage: `sql/333` (extended by `sql/337`).
Rules: `app/services/prereg_contract.py`. Enforcement: `app/services/result_ledger.py`.

## 1. The defect

`prereg_contract.declaration_refusals` returns `structural_refusal_policy_superseded`
whenever a frozen row's `structural_refusal_policy_version` differs from
`STRUCTURAL_REFUSAL_POLICY_VERSION`. `sql/333` bars UPDATE and DELETE and holds
`UNIQUE (strategy_id, strategy_version)`. Composed: **one policy bump strands every
declaration frozen before it, permanently**, and `result_ledger._refuse_incoherent_declaration`
then refuses that trial's every outcome look.

Main moved `structural-refusal-policy-2026-08-12-v1` → `…-v2-carry-fx-split` inside a
single day (#2363). At that cadence a candidate frozen today is stranded well before its
forward shadow completes.

Measured on dev, 2026-08-13:

```
select count(*) from strategy_preregistration_declarations;             -- 0
select count(*) from strategy_holdout_accesses;                         -- 304 (4 of kind 'read')
select count(*) from strategy_results_store where namespace='hold_out'; -- 0
select count(*) from strategy_live_gate_policies
 where declaration_id is not null;                                      -- 0
```

Nothing is stranded yet, which is why this lands before the two pending freezes
(`scripts/freeze_2582_schedule13d_declaration.py`,
`scripts/freeze_2616_precutoff_declarations.py`) are run.

## 2. What is NOT the fix

Both settled in #2631's comment and restated here so the next reader does not re-litigate:

- **Relax the nominal check to the substantive one.** `expected_structural_refusals_mismatch`
  covers every bump that changes this row's refusal *set*. It cannot see a bump that adds a
  stamp dimension, a predicate reinterpreted under the same code, or a rule that happens not
  to bind these stamps. The nominal check is a **provenance** guard. It stays.
- **Mint a new `strategy_version`.** The documented escape and still available. It is not
  free: it changes trial identity, strands the old trial anyway, and charges the shared
  trial register again (#2600), raising the deflated-Sharpe bar for every other candidate.

## 3. The design — an append-only chain whose terms cannot move

### 3.1 Rows point forward, never backward

A superseding declaration is a **new row** for the same `(strategy_id, strategy_version)`
naming its predecessor in `supersedes_declaration_id`. No existing row is touched, so the
`BEFORE UPDATE OR DELETE` immutability trigger stays intact and unchanged.

"Exactly one current declaration per trial" is then four declarative constraints, not a
mutable flag:

| constraint | what it forbids |
| --- | --- |
| `UNIQUE (supersedes_declaration_id)` | two rows superseding the same predecessor — a *tree*. Forces a linked list. |
| `UNIQUE (strategy_id, strategy_version) WHERE supersedes_declaration_id IS NULL` | a second *root* for one trial. This is the old `UNIQUE (strategy_id, strategy_version)`, narrowed. |
| `CHECK (supersedes_declaration_id IS NULL OR supersedes_declaration_id < declaration_id)` | **cycles.** See below. |
| FK `(supersedes_declaration_id, strategy_id, strategy_version)` → `(declaration_id, strategy_id, strategy_version)` | superseding a declaration belonging to a **different trial**. |

⚠⚠ **The CHECK is what makes the invariant provable, and the first draft of this spec did
not have it** (Codex checkpoint 1). Uniqueness plus the FK forbid branching but *not* a
cycle: rows inserted with explicit ids, or a multi-row INSERT forming a closed loop, satisfy
every uniqueness rule while leaving the trial with **zero** current declarations — a
different permanent wedge from the one this ticket fixes. Because `declaration_id` is
`BIGSERIAL` and a predecessor is always inserted first, every real edge points to a *smaller*
id; a cycle needs at least one edge that does not. One single-row CHECK therefore proves a
global graph property with no recursion, no depth limit and no cycle-detection code.

With cycles barred and a single root enforced, every row for a trial is reachable from that
root, so the chain is one acyclic list and the row nothing supersedes is unique.

⚠ The composite FK needs `UNIQUE (declaration_id, strategy_id, strategy_version)` on the
referenced table to exist at all — a PRIMARY KEY on `declaration_id` alone does not satisfy a
three-column reference. It is redundant given the PK and is there solely to support the FK.

⚠ The old `UNIQUE (strategy_id, strategy_version)` is **dropped** and replaced by the partial
index; keeping it would forbid the second row outright, which is the defect. Dropping it also
drops the index every read path uses to find a trial's declaration, so `sql/337` adds a plain
`(strategy_id, strategy_version)` index back.

### 3.2 A supersession may repair the version string and nothing else

The superseding row must be **term-for-term identical** to its predecessor except for:

- `structural_refusal_policy_version` — the field being repaired, and it must name the
  current constant;
- `expected_structural_refusals` — recomputed under the new policy, and already checked
  against the declared stamps by `expected_structural_refusals_mismatch`;
- `declared_by` — the re-declaration names its own declarer.

Everything else — `contract_version`, `prereg_purpose`, `declared_universe_basis`, both cost
stamps, both forward-shadow floors, the derivation — is compared field by field, as a set for
the refusal list, and any difference is `supersession_terms_changed`.

**This is what bounds the adaptivity the ticket warns about.** The concern is that an author
who has seen sample counts, missingness or corpus composition re-declares more favourably.
Under this rule there is nothing to re-declare: purpose, stamps and floors are exactly the
predecessor's, and a trial that wants different terms is a different trial — a new
`strategy_version`, unchanged as the identity boundary.

⚠ **The partition must be exhaustive or it rots.** A declaration field added later and named
in neither list would become silently mutable through supersession. `SUPERSESSION_MUTABLE_FIELDS`
is therefore compared against `PreregDeclaration`'s own field set in a test, the same way
`digest_payload` is (#2631).

⚠ `declared_by` moving does not lose the original declarer: the predecessor row is immutable
and still in the chain. The same is true of the root's `frozen_at`, which remains the trial's
actual preregistration time — the successor's is the time of the repair.

### 3.3 Only before the trial has been looked at, and the attestation carries the weight

`supersede_preregistration` refuses when any `strategy_holdout_accesses` row exists for the
trial, or when any `strategy_results_store` row for it sits in the `hold_out` namespace, and
requires a non-empty `supersession_attestation` plus a `supersession_reason` from a closed
vocabulary whose only member today is `structural_refusal_policy_superseded` — enforced by a
DB `CHECK`, not only in Python.

The hold-out-results check is a second disqualifier because the access ledger and the result
rows can disagree: a hold-out row written before #2599's chokepoint existed, or by a path that
bypassed it, is exposure with no access row to count.

⚠⚠ **The zero-access count is necessary and NOT sufficient, and the design does not pretend
otherwise.** That ledger records committed paved-path accesses. A direct `SELECT` against
`strategy_results_store` leaves no row (`sql/264`'s header measured that RLS does not bind
this app's superuser connection); a rolled-back transaction removes its own record; outcomes
may already sit in a signed artifact, an export, a log or another database. The counts are a
cheap automatic *disqualifier*. The attestation is the claim that no exposure happened by any
other route — and an attestation is a claim, not a proof. It is frozen with the row and
protected by the same immutability trigger, so it is at least attributable and unrewritable.

A one-member reason vocabulary is deliberate. Other reasons to re-declare are exactly the
adaptivity being forbidden; adding one is a migration and a visible act.

### 3.4 Concurrency

The hazard: an access commits between the zero-count and the INSERT.

Both sides take **`pg_advisory_xact_lock(hashtext(<strategy_id>/<strategy_version>))`** — the
pattern already in `app/api/strategies.py:1384` — so freeze, access and supersession serialise
per trial:

- `record_holdout_access`, the single chokepoint every paved door funnels through, takes it
  before its coherence check;
- `freeze_preregistration` and `supersede_preregistration` take it before reading anything.

Row locks were the first draft and are worse here. `FOR SHARE` is barred inside a `READ ONLY`
transaction (`verify_outcome_access_provenance` runs in `REPEATABLE READ READ ONLY`), it locks
**zero** rows for a trial that has no declaration yet — so it cannot order an access against a
concurrent *first* freeze — and "all rows of the trial" has no deterministic lock order across
plans, which is a deadlock shape. One advisory lock on the trial identity has none of those
properties. Hash collisions merely over-serialise two unrelated trials, which is harmless.

Outcomes, both orders:

- **access first** → supersession waits, then counts under a fresh snapshot, sees the access →
  `supersession_trial_already_exposed`. ✓
- **supersession first** → the access waits, then re-reads under a fresh statement snapshot and
  finds the *successor*, whose policy version is current → the look is authorised against the
  new revision, and the access row is attributed to it. ✓

⚠ Both of those depend on `READ COMMITTED`, where each statement takes a new snapshot. Under
`REPEATABLE READ` the post-lock re-read would return the pre-lock snapshot and the second
outcome would silently regress. `supersede_preregistration` therefore reads
`current_setting('transaction_isolation')` and refuses anything else rather than assuming its
caller's isolation level.

⚠ `UNIQUE (supersedes_declaration_id)` remains the backstop for a writer that skips the lock:
the loser gets a `UniqueViolation`, which is translated into
`supersession_predecessor_already_superseded` rather than escaping as a raw driver error.

⚠ `register_live_gate_policy` takes the strategy-control lock and not this one, so a
registration can race a supersession. It is not a hazard: the policy would bind the
predecessor, and §3.6 makes any row in the chain acceptable — every one of them carries
identical floors.

### 3.5 Attribution across the chain (ticket scope item 2)

`strategy_holdout_accesses` identifies the trial, not the revision. Two changes:

1. **`strategy_holdout_accesses.declaration_id`**, nullable, FK `ON DELETE RESTRICT`.
   `record_holdout_access` already loads the declaration through
   `_refuse_incoherent_declaration`, so it writes the id it actually checked — not a second
   load, which could resolve differently. A trial with no declaration records `NULL`, which is
   the pre-#2599 behaviour unchanged.
2. **`verify_outcome_access_provenance` requires `a.declaration_id = d.declaration_id`** in the
   join, and accepts any revision **in the trial's chain** for the caller-supplied
   `declaration_id`.

⚠⚠ The second half of that was missed on the first implementation pass and caught by Codex at
checkpoint 2: the join moved to `a.declaration_id` and this section was written, while the
Python guard twenty lines down still compared against the *current* revision. Both compare the
same key; only one was in the diff's line of sight. Left standing it would have refused every
signed artifact naming a predecessor — this ticket's own wedge, rebuilt inside the fix for it.

⚠ The first draft required the named declaration to be *current now*. That is the wrong claim
(Codex checkpoint 1): provenance needs "was current when the access happened", and
authorisation is `record_holdout_access`'s job. Requiring current-now would also break every
caller that persists a `declaration_id` into a signed artifact and verifies later — which
`scripts/evaluate_2582_schedule13d_outcomes.py`, `scripts/schedule13d_artifact.py` and
`scripts/sealed_rerun_gate.py` all do. Equality on the access row's own `declaration_id` says
exactly the intended thing and says it about the row itself.

That is a tightening, not a break: the join today produces no rows at all for any existing
access (there are 0 declarations), so nothing that currently passes stops passing. The 304
existing access rows keep `declaration_id IS NULL`.

### 3.6 The live gate

`assess_live_gate` reads the floor off *the declaration the policy points at*, deliberately,
so a second laxer declaration cannot retro-loosen an immutable policy. A supersession would
otherwise break that comparison (`policy.declaration_id != frozen.declaration_id`) and leave
the trial permanently `forward_shadow_floor_missing` — the same wedge one level up, since the
policy is itself immutable and cannot be re-registered.

Fix: the policy's declaration must be **in the current declaration's chain**. §3.2 guarantees
every row in a chain carries identical floors and purpose, so reading the current row cannot
loosen anything; and a laxer declaration cannot be a supersession at all — it needs a new
`strategy_version`, which is a different trial and a different policy. The existing digest and
coherence checks continue to run against the current row, unchanged.

`FrozenPreregistration` therefore carries `chain_declaration_ids` alongside the current
`declaration_id`. Membership is over the chain the loader walked from the single root, not
over "any row sharing the trial identity" — §3.1's constraints make those the same set, and
the loader asserts it rather than assuming it.

## 4. Refusal vocabulary

Pure, in `prereg_contract.supersession_refusals(predecessor, successor)`:

| code | meaning |
| --- | --- |
| `supersession_not_required` | the predecessor already names the current policy version — nothing to repair, and a no-op chain link is a row nobody needs |
| `supersession_policy_not_current` | the successor does not name the current policy version; superseding into another stale version repairs nothing |
| `supersession_terms_changed` | §3.2's invariant subset differs; the exception names the fields |

DB-side, in `result_ledger.supersede_preregistration`:

| code | meaning |
| --- | --- |
| `supersession_nothing_frozen` | no declaration exists for the trial; there is nothing to supersede |
| `supersession_trial_already_exposed` | at least one `strategy_holdout_accesses` row exists for the trial |
| `supersession_trial_has_holdout_results` | a `hold_out` namespace result row exists for the trial, with or without an access row |
| `supersession_predecessor_already_superseded` | lost a concurrent race; the `UNIQUE` backstop fired |

The successor's own `declaration_refusals` are surfaced in the same refusal, so a successor
that is incoherent in its own right fails naming why.

## 5. What this does NOT do

Stated because #2614 was filed for a docstring that overclaimed.

1. **It does not prove non-exposure.** §3.3. The counts are disqualifiers; the attestation is
   a claim.
2. **It does not repair a bump that ADDS a stamp dimension.** Measured: `declared_fx_unmodelled`
   was added by a later migration with `DEFAULT true`. An older row's value for such a column is
   a migration default, not a declaration; §3.2's terms-identity forces the successor to inherit
   it, and if the default makes the declaration incoherent — `true` produces a structural
   refusal, so a `capital_candidate` declaration would fail
   `ineligible_trial_not_declared_falsification` — the trial needs a new `strategy_version`.
   The default is the fail-closed direction, which is why inheriting it is safe rather than
   merely convenient.
3. **It does not un-strand a trial that was already looked at**, nor a live-gate policy
   registered with `declaration_id IS NULL` (there are none). Those keep the `strategy_version`
   escape.
4. **It does not change trial-register accounting.** A supersession charges nothing, because
   no look happened — which is the same fact §3.3 refuses on.
5. **It does not close the adaptivity channel owned by whoever bumps the policy.** A bump can
   reinterpret an unchanged value's meaning under the same code; supersession then carries the
   new meaning while claiming only a version string moved. Terms-identity bounds the
   *declarer*, not the *policy author*. Nor are `contract_version` and
   `forward_shadow_derivation` content hashes — they are identifiers, and the documents they
   name can change underneath a byte-identical field.
6. **It does not bind a writer that bypasses `result_ledger`.** The advisory lock is a
   convention between the functions that take it; a direct INSERT into
   `strategy_holdout_accesses` takes no lock and is counted only if it committed first.
7. **It does not decide whether a policy bump should carry a compatibility statement**
   (#2634's closing note). A bump that its author knows changes no stamp meaning still strands
   every row and still needs a supersession per trial. Left open deliberately: the evidence for
   it is a second bump we have not had yet.

## 6. Acceptance

- `sql/337` applies on dev; each constraint in §3.1 is probed, including the cycle CHECK.
- A frozen declaration under a stale policy version + a supersession under the current one →
  the trial's looks are authorised again, and `load_preregistration` returns the successor.
- Each refusal in §4 has a test, revert-probed.
- The old `UNIQUE (strategy_id, strategy_version)` is gone and a second *root* is still
  refused.
- `assess_live_gate` on a superseded trial still finds its floor.
