# #2614 — binding #2599's declaration gate to C-4, the sealed study that does not touch the ledger

Refs #2614. Refs #2599. Refs #2600. Refs #2582. Refs #2437.

## The premise, re-falsified before design (working order 3c)

#2614 states: *"C-4 can open its sealed outcomes today and neither the #2599 gate
nor the #2600 register will observe it."* **The first clause is false, measured at
`e6d8aa86`:**

```
$ PYTHONPATH=. uv run python -c "...require_outcome_gate(acknowledgement=ACKNOWLEDGEMENT, contract_path=...)"
GATE REFUSED: OutcomeGateRefusal c4-schedule13d-public-catalyst-v1 is absent from
  trial-register-2026-08-12-r4; declare the price-data search before reading outcomes
```

`evaluate_2582_schedule13d_outcomes.py:645` already refuses to build an
`OutcomeGate` unless C-4's trial id is in `TRIAL_REGISTER`, and it is not
(21 entries, `M = 259`). So C-4 cannot run today.

**The rest of #2614 stands, and the correction sharpens it.** The hole is
*latent*, not open: the one action that makes C-4 runnable — adding its register
entry, which the ticket's own scope item 3 requires — is also the action that
opens an ungated path, because nothing in the C-4 pipeline consults
`strategy_preregistration_declarations`. Verified:

```
$ grep -n "record_holdout_access\|require_outcome_access\|store_holdout_result\|load_preregistration" \
    scripts/evaluate_2582_schedule13d_outcomes.py scripts/schedule13d_*.py
(no output)

$ select count(*) from strategy_preregistration_declarations;  -->  0
```

**Consequence for sequencing: the register entry and the declaration check must
land in the same change.** Adding the entry first would unlock exactly the door
this ticket exists to close.

## The defect class is three scripts, not one

`grep` over `scripts/evaluate_*.py` and `scripts/*_outcomes.py` returns three
sealed outcome openers, none of which consults a declaration:

| script | trial | state |
| --- | --- | --- |
| `evaluate_2582_schedule13d_outcomes.py` | C-4 | **has not run** — gate refuses |
| `verify_2476_pead_outcomes.py` | `pead-historical-sue-net-income-v1` | ran pre-cutoff; charged 8, exact |
| `verify_2480_insider_outcomes.py` | `form4-code-p-opportunistic-purchase-v1` | ran pre-cutoff; charged 7, exact |

⚠ Correction (#2616): this row originally attributed the `verify_2480` look to
`insider-purchase-forward-returns-first-look-2026-08-09` (4, exact). Wrong: the
register's first-look entry is `scripts/verify_2437_insider_forward_returns.py`'s
distinct 4-horizon construction; the sealed portfolio run this script performs is
charged as `form4-code-p-opportunistic-purchase-v1` (7, exact) — see
`app/services/trial_register.py`, which cites issue #2480's sealed-outcome
comment as that entry's evidence.

The two `verify_*` scripts opened their outcomes before `TRIAL_REGISTER_CUTOFF`
(2026-08-12 07:00Z) and are already charged by #2600's reconstruction. #2599 does
not retroactively invalidate them (`sql/333` header: *"A trial with no row here
behaves exactly as it did before this migration"*). They are therefore an
**explicit, reasoned allowlist entry in the item-5 test**, not silent omissions,
and re-gating them is a follow-up ticket rather than this one's scope.

## Source rule

Every value below is read off a frozen artefact or measured on the full
population. Nothing is chosen here.

| decision | governing rule | value |
| --- | --- | --- |
| `strategy_id` | contract `candidate_id` | `c4-schedule13d-public-catalyst` |
| `strategy_version` | contract `contract_version` | `schedule13d-public-catalyst-v1` |
| `prereg_purpose` | contract `decision: historical_falsification_only` + `acceptance.historical_archive_can_promote_capital: false` | `falsification_only` |
| `declared_universe_basis` | contract `eligibility.current_is_tradable_required: true` and `historical_security_identity_limit: current_snapshot_only_not_point_in_time_common_share_proof` — the population is by construction restricted to instruments that survived to today | `survivor_only` |
| `declared_carry_unmodelled` | contract `position` charges `round_trip_adverse_cost_bps: 50` and names no carry, borrow or FX term | `true` |
| `expected_structural_refusals` | `structural_promotion_refusals(universe_basis="survivor_only", carry_unmodelled=True)` — computed, never written by hand | `("universe_basis_not_survivorship_free", "carry_unmodelled")` |
| `access_kind` | `sql/264` `strategy_holdout_accesses_evaluate_names_a_result`: an `evaluate` MUST name a `result_version`, and C-4 writes no result row | `read`, `result_version NULL` |

⚠ `declared_universe_basis` is decided by the **contract's own eligibility rule**,
not by a judgement about the `paperswithbacktest/Stocks-Daily-Price` corpus. Even
a survivorship-free corpus cannot rescue a population filtered on
`current_is_tradable`.

## Forward-shadow floor — derived, not chosen

`sql/333` CHECKs both floors `> 0` and `prereg_contract.ForwardShadowFloor`
forbids a default: *"Picking a central default would be the #2600 padded-floor
defect verbatim — a floor no artefact evidences."* So it is derived by
construction from two inputs, both already frozen or measured:

1. **The contract's own power calculation**, frozen and asserted by
   `verify_2582_schedule13d_preregistration.py:47-53`:
   `minimum_planning_effective_sample_size = 785`, from
   `round(7.84888 × (σ/effect)²)` at σ = 10.0%, effect = 1.0%, α = 0.05
   two-sided, power = 0.8. `7.84888 = (z₀.₉₇₅ + z₀.₈)²`.

2. **The measured, outcome-free arrival rate of C-4's own primary population**,
   full population, no sample. Reproducing command:

   ```
   PYTHONPATH=. uv run python -c "
   import psycopg; from app.config import settings
   from scripts.evaluate_2582_schedule13d_outcomes import load_source_events
   with psycopg.connect(settings.database_url) as c:
       c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
       ev=[e for e in load_source_events(c) if e.primary_source_refusal is None]; c.rollback()
   d=sorted({e.public_filing_date for e in ev})
   print(len(ev), len(d), (d[-1]-d[0]).days)"
   ```

   Measured 2026-08-12: **963 clean 13D events over 331 distinct public filing
   dates spanning 547 calendar days** (2024-12-18 → 2026-06-18). 1,184 total 13D
   source events, 1,068 unfiltered-eligible, 12,367 13G source events.

Arithmetic, both `ceil`:

```
min_independent_decision_dates = ceil(785 × 331 / 963) = ceil(269.818) = 270
min_calendar_weeks             = ceil(270 × 547 / 331) = ceil( 63.742) =  64
```

⚠ **Both are lower bounds, and the direction is the honest one.** The contract's
785 is an *effective* sample size, and its own definition
(`min_raw_n, sample_return_variance / pigeonhole_bootstrap_mean_variance`) makes
effective ≤ raw — so 785 raw events is the smallest raw count that could satisfy
it. Clustering by issuer and entry session pushes the true requirement up, never
down. A floor stated as a floor is what `TrialExactness.FLOOR` already models
elsewhere in this codebase.

⚠ The 270/64 literals are frozen into the declaration row, which is what freezing
means. `forward_shadow_derivation` carries the two input counts and the
reproducing command beside them, and a pure test re-derives 270 and 64 from
(785, 963, 331, 547) so the arithmetic cannot drift silently.

## Trial register entry — 7 searches, exact

`build_historical_falsification_report` (`scripts/schedule13d_report.py:288-320`)
names every arm that reads price windows, at this commit:

| # | arm | code |
| --- | --- | --- |
| 1 | 13D primary (clean) treatment | `load_price_windows(..., population="primary")` |
| 2 | 13D unfiltered-eligible robustness population | `load_price_windows(..., population="unfiltered")` |
| 3 | matched random-time challenger | `_random_comparison` |
| 4 | initial 13G rule 1b challenger | `_13g_comparison("1b", ...)` |
| 5 | initial 13G rule 1c challenger | `_13g_comparison("1c", ...)` |
| 6 | initial 13G rule `both` (attribution only) | `_13g_comparison("both", ...)` |
| 7 | initial 13G rule `unknown` (attribution only) | `_13g_comparison("unknown", ...)` |

⚠ **"Each arm reads its own bars" is NOT the justification, and stating it that
way was wrong** (caught at Codex checkpoint 1). `load_initial_13g_price_windows`
loads the 13G challenger population **once**; arms 4–7 partition and re-match
that single loaded set. Only arms 1, 2 and 3 read bars of their own. The
justification is two rules, applied explicitly:

- **New bars are a new search.** Arms 1, 2 and 3 each load a distinct price-window
  population (`population="primary"`, `population="unfiltered"`,
  `load_random_time_price_windows`). Three searches.
- **A separately-reported cell of one loaded population is charged per cell.** The
  register's own merged `short-horizon-search-session-2026-08-09` entry charges
  *"25 breadth cells, 12 confluence buckets, 13 individual conditions"* — buckets
  of one population, charged individually, because criterion 6 names *"manual
  eyeballing"*. The four 13G rule cells (1b, 1c, `both`, `unknown`) are exactly
  that shape and are separately reported by `PairedComparison`. Four searches.

⚠ **The contract's own Holm correction settles arms 3–5 against the fan-collapse
rule.** *"The robustness fan is one search, not four"* licenses collapse because
*"the flattering arm cannot be selected, so no maximum was taken over them"*.
`random_time`, `initial_13g_1b` and `initial_13g_1c` are jointly required, which
looks like that shape — but the contract applies
`holm_step_down_adjust_one_sided_p_values_for_random_13g_1b_and_13g_1c`, an
explicit multiple-testing correction. A study that corrects for three tests
internally cannot tell the register it performed one.

**What is deliberately NOT charged, and why** — stated so a later reader can
contest it rather than guess:

- The **8 non-paired decision gates** in `_decision_gates`
  (`effective_sample_size_gte_785`, `adverse_cost_clustered_lower_bound_gt_zero`,
  `profit_factor_gt_one`, `positive_result_excluding_best_1pct`, the two
  concentration gates, and the two stability gates) are all computed from the
  single `primary: OutcomeStatistics`. No new bars, and no arm among them can be
  selected — every one is conjunctive. This is the register's *"two inference
  treatments of the same 28 effects is 28, not 56"* rule.
- The **three non-overlapping 6-month stability windows** partition the primary
  population, and `two_of_three_6_month_means_positive` is a 2-of-3. That is not
  a flattering selection: the windows are predeclared, exhaustive over the source
  span, and the study must *additionally* pass
  `latest_nonoverlapping_6_month_mean_positive`, so no subset can be chosen to
  rescue a failure.
- The **sector concentration** figure, which the contract marks attribution-only
  and which reads no price bars beyond the primary set.

`exactness = EXACT`. The register admits *"a code-level grid at the commit that
ran it"*, and `build_historical_falsification_report` computes all seven arms
**unconditionally** — there is no branch that skips one — from a digest-frozen
contract, in a single deterministic run. ⚠ Codex's objection that *"a code grid
proves what can run, not what ran"* is the right question and is answered by that
unconditionality plus the freeze-before-run ordering the gate enforces; `FLOOR`
would be the wrong flag here, since the register warns that *"FLOOR on a fully
enumerated family invites a later reader to pad it"*.

`TRIAL_REGISTER_VERSION` bumps `trial-register-2026-08-12-r4` → `-r5`.
⚠ No stored result regresses **on this dev database**, which is the only
population that can be measured from here: all 244 rows in
`strategy_results_store` already carry a superseded version (`2026-08-07`: 108,
`2026-08-10`: 48, `2026-08-11-r3`: 48, `NULL`: 40 — none carry r4), so they are
already `trial_register_superseded` and the bump changes nothing about them. The
claim is not asserted for any other deployment.

## Design

### 1. `scripts/freeze_2582_schedule13d_declaration.py` — a separate one-shot

⚠ **The gate must NOT freeze the declaration itself.** Auto-freezing at look time
destroys the only property the declaration has — that it predates the look — and
is the #2599 checkpoint-1 defect (*"a caller can construct a favourable
declaration after seeing/reading outcomes"*) rebuilt with the constructor moved.
So freezing is a distinct script, run once, that:

- verifies the frozen contract (`load_and_verify`, digest `8f4424be…`),
- builds the `PreregDeclaration` with `expected_structural_refusals` **computed**
  by `structural_promotion_refusals`, never spelled out,
- calls `freeze_preregistration`, which refuses an incoherent declaration at the
  only time refusing is cheap, and commits.

⚠ **A bare `UniqueViolation` is not good enough on a re-run.** `sql/333`'s UNIQUE
constraint is correct — one declaration per trial, so a caller cannot pick
whichever one the outcome favours — but a retry after an uncertain commit then
fails *indistinguishably* from a genuine conflict. So on `UniqueViolation` the
script reloads the stored declaration and compares `declaration_sha256` against
the one it would have frozen: equal exits 0 reporting `already_frozen_identical`,
different exits non-zero naming both digests. Same failure, two very different
operator actions.

⚠⚠ **CORRECTION (2026-08-13, #2631): this script cannot be run "anytime", and
the operator was told in session that it could.** The frozen row records
`STRUCTURAL_REFUSAL_POLICY_VERSION`, and `prereg_contract.declaration_refusals`
returns `structural_refusal_policy_superseded` the moment that string stops
matching the current constant — for good, because `sql/333` bars UPDATE and
DELETE and holds the identity key, so no corrected row can replace it. Main
moved v1 → v2 on 2026-08-12 while C-4's freeze sat one command away, and nothing
in the script, its `--dry-run` output or this doc mentioned the coupling.

**Recovery cost, in full — it is not just a rename.** A new `strategy_version`
changes the trial's identity, leaves the old trial permanently inaccessible, and
charges the shared trial register a second time (#2600), raising the
deflated-Sharpe bar for every other candidate. There is no cheaper path.

So the runbook line is: **run `--dry-run` first and read
`structural_refusal_policy_version` in its output** (every digest input is
printed there now, by construction from `PreregDeclaration.digest_payload`), and
freeze only when no change to the structural refusal policy is in flight.
`scripts/_prereg_freeze_guard.assert_policy_version_merged` refuses
automatically when this tree's constant is not the one on `origin/main` — after
`git fetch origin main`, because an unfetched tracking ref would compare the
repository to a stale copy of itself and match for the wrong reason. ⚠ It
does **not** see the case that actually occurred — a bump on an unpushed branch
in another worktree — because no check that reads this repository can see a
commit that is not in it. There the dry-run and this paragraph are the control.

### 2. Gate wiring — one committed access, then a read-only re-check

`require_outcome_gate` gains a `conn` and becomes the boundary crossing:

1. existing checks (acknowledgement, contract digest, trial id in register),
2. `require_outcome_access(conn, HoldoutAccess(..., access_kind="read", result_version=None, ...))`
   — refuses `preregistration_not_frozen`, re-checks coherence *and* digest via
   `_refuse_incoherent_declaration`, and writes the audit row,
3. returns `OutcomeGate(..., declaration_id, access_id)`.

⚠⚠ **`require_outcome_access` does NOT commit — it writes in the caller's
transaction — so commit ownership has to be named, not assumed** (Codex
checkpoint 1). Two facts collide: the INSERT leaves the connection inside a
transaction, and `SET TRANSACTION ISOLATION LEVEL ... READ ONLY` is only valid as
the **first statement of a transaction**. So an uncommitted gate INSERT does not
merely risk an unlogged look — it makes `evaluate_historical_falsification` fail
outright at its first statement.

Concretely, and owned by `run_2582_schedule13d_outcomes.main` on **one**
connection:

```
gate = require_outcome_gate(conn, acknowledgement=..., contract_path=...)   # INSERTs
conn.commit()                                                              # <- the boundary
report = evaluate_historical_falsification(conn, gate)                     # SET TRANSACTION ... READ ONLY
```

One connection, so the committed access row is trivially visible to the
validation that follows; no cross-connection handoff to reason about.

⚠ **One access row, not four.** `_load_requested_price_windows` is called four
times per run; logging four `read` rows would make the audit count a function of
internal call structure rather than of looks taken.

`_load_requested_price_windows` — the single chokepoint all four populations
funnel through, mirroring #2599's own placement argument — additionally calls a
new read-only `_validate_gate_provenance(conn, gate)`, which checks **two rows,
not one**:

1. **The declaration**, re-loaded from the table by trial identity: present,
   coherent, digest-intact, and carrying the `declaration_id` the gate names.
2. **The access row**, re-loaded by the `access_id` the gate names: it must
   exist, bind to the same `(strategy_id, strategy_version)`, be
   `access_kind = 'read'`, and satisfy **`frozen_at < accessed_at`** against the
   declaration.

⚠ **Checking only the declaration would be bypassable, and that was the first
draft** (Codex checkpoint 1). `OutcomeGate` is a plain frozen dataclass a caller
can construct; given a real `declaration_id` it could open every price window
without `require_outcome_access` ever running, leaving no audit row. Re-loading
the access row by id is what makes the returned `access_id` an enforcement rather
than decoration — and because a rolled-back INSERT leaves no visible row, the
same check is what proves the access **committed**.

⚠ **`frozen_at < accessed_at` is asserted, not assumed.** Statement ordering
usually gives it for free, which is exactly why it should be stated: the
declaration predating the look is the entire property, and an invariant nobody
asserts is an invariant nobody notices breaking. Both timestamps are
server-side (`DEFAULT now()`), so no client clock enters.

### 3. Doc correction (scope item 4)

`result_ledger.py:905`'s *"which is every path we have written"* and the matching
sentence in `docs/proposals/ta/2026-08-12-preregistration-declaration-gate.md`
become what is actually true: **every path that writes a result row**. A sealed
study that keeps its results in a signed artifact is a second path and needs its
own gate.

### 4. Convention test (scope item 5)

`tests/test_sealed_outcome_scripts_are_gated.py` globs `scripts/evaluate_*.py`
and `scripts/*_outcomes.py`, and asserts each either **calls** a declaration
check (`require_outcome_access` / `require_outcome_gate`) or appears in an
explicit allowlist mapping script → reason. The allowlist holds exactly the two
pre-cutoff `verify_*` scripts with their register entries named, so a NEW sealed
opener fails the test by default.

⚠ **The check walks the AST for a CALL node, not the source text for a
substring** (Codex checkpoint 1). A textual match passes on a mention in a
comment, an unused import, a docstring or a dead branch — which is a test that
certifies the exact thing it is meant to catch.

⚠ **Stated limit, since this ticket exists because a limit was overstated.** The
glob is lexical: a sealed opener under another name, in `app/`, in a notebook, or
run as ad-hoc SQL is not caught. The test narrows the door that three scripts
walked through, and does not close the room.

## Tests

Pure (no DB): declaration field derivation from the frozen contract; the
270/64 arithmetic re-derived from (785, 963, 331, 547); register entry present,
`searches == 7`, `EXACT`, version bumped; the convention test.

DB-backed: gate refuses `preregistration_not_frozen` with no declaration;
succeeds with one and writes exactly one `read` row with NULL `result_version`;
`_validate_declaration` refuses a digest-tampered row (nested savepoints — a
probe after a bare `rollback()` matches zero rows and proves nothing, #2599).

Revert-probe every invariant test, asserting `count(old) == 1` before injecting.

## Checkpoint-1 findings answered rather than fixed

Codex raised these; each is a real question with an answer already settled
elsewhere in the repo, so the resolution is a citation, not a code change.

- **"Logging before reading records attempted looks, not actual accesses."**
  Correct, and it is the intended direction. `read_holdout_results`'s docstring
  already settles it: *"THE ACCESS IS RECORDED EVEN WHEN THE READ RETURNS
  NOTHING. Looking is the event criterion 5 governs, and logging only successful
  looks would make the log a function of what happened to be stored rather than
  of what was asked for."* An access row that survives a crashed evaluation is
  the conservative error.
- **"`survivor_only` is inferred, not mapped."** It is the fail-closed default,
  not an inference. `structural_promotion_refusals` implements #2288 —
  *"An unlabelled result is treated as survivor_only, never as validated"* — and
  `PROMOTABLE_UNIVERSE_BASES` is an allowlist of one (`strategy_result.py:214`),
  so anything that is not positively `survivorship_free` refuses identically.
  C-4's contract cannot establish `survivorship_free` while filtering on
  `current_is_tradable_required`.
- **"`declared_carry_unmodelled = true` is inferred from omission."** It is read
  off the stamp's own definition: `strategy_result.py:805-806` — *"carry and FX
  are NULL, not zero, so no result charging neither is promotable."* C-4 charges
  a flat 50 bps and no carry or FX term, so the stamp is true by definition. The
  inference would run the other way: declaring `false` on a contract with no
  carry term is what would need an argument.
- **"SEC treatments (13D/A, joint reporters, 13G rule classification, `filed_at`
  as the public clock) are uncited."** Out of scope, deliberately. Those are the
  frozen #2582 contract and census, merged at `55c75dce` and pinned here by
  digest `8f4424be…`. This ticket binds a gate to that study; re-opening its
  source rules would change the bytes the digest protects.
- **"The chokepoint is repository-local; direct SQL still reads the bars."**
  Yes — and correcting precisely that overstatement is scope item 4. The
  enforced claim after this change is *every path that loads C-4's price windows
  through the evaluator*, and the docstring will say so.
- **"No advisory lock; concurrent invocations record several accesses."** Not
  taken. C-4 is a one-shot operator script over a frozen contract; several
  recorded accesses is the honest log of several looks, which is the direction
  criterion 5 wants. A lock would suppress audit rows to buy tidiness.
- **"No DB relation binds an access row to a declaration."** True;
  `strategy_holdout_accesses` has no `declaration_id` column. They are bound by
  trial identity plus the asserted `frozen_at < accessed_at`. Adding the column
  is a schema change across a table with 224 live rows and belongs to its own
  ticket, not to this one.

## Acceptance

⚠ **Stated as what is enforced, not as chronology it cannot prove.** "C-4 cannot
open outcomes without a frozen declaration" is the intent; the provable claim is:

- `run_2582_schedule13d_outcomes` and every `_load_requested_price_windows`
  caller refuse unless a coherent, digest-intact declaration for
  `c4-schedule13d-public-catalyst` / `schedule13d-public-catalyst-v1` exists,
  was frozen strictly before the recorded access, and that access row is
  committed and bound to the same trial;
- C-4's 7 arms appear in `TRIAL_REGISTER` at `-r5`;
- `result_ledger.py:905`'s documented limit matches the enforced one;
- a new `scripts/evaluate_*` / `scripts/*_outcomes.py` fails the convention test
  unless it calls a declaration check or is allowlisted with a reason.

What remains outside it, unchanged from #2599's own stated limit: a direct
`SELECT` against the price tables, and any opener that does not go through these
helpers.
