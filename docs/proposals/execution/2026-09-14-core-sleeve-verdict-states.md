# Core sleeve: name every terminal state of #2833's verdict (#3037)

Status: proposal, unshipped. Target: land before the sealed verifier opens — currently
bounded at 2026-09-18T00:00Z, which is a **moving lower bound on an incomplete
population**, not a scheduled date (a missed observation pushes it later).

Revision 2, after Codex checkpoint 1 (21 findings). The changes that mattered are marked ⓒ.

## Problem

`CoreSelectionState = Literal["evidence_collecting", "ready", "unavailable"]`
(`app/services/strategy_core_selection.py:37`) has three values. #2833's verdict has five
distinguishable outcomes. The two it cannot name are the two that exist at the gate.

Measured on the running dev stack, 2026-09-14:

| site | reports | truth |
| --- | --- | --- |
| `StrategyPortfolioLens.tsx:473` | badge `Cash` | 1 of 5 common dates; verdict sealed |
| `load_core_selection`, window closed, nothing transcribed | `evidence_collecting`, bound in the past | verdict openable now |
| `load_core_selection`, verdict `cash` recorded | `unavailable` + "must name a declared candidate" | a correct, reviewed outcome |

`cash` is not the absence of a verdict. `scripts/verify_2833_core_selection.py:223` emits
`"outcome": "pass" if selected is not None else "cash"` — two terminal answers, and the
surface can encode one.

## Source rule

There is no external formulation for "what states does a preregistered selection study
have". The authority is **the verifier's own emitted vocabulary**, which is this repo's
settled artefact: `evidence_collecting` / `pass` / `cash`, plus the `opens_at` boundary it
gates on (`:167`). The state machine below is fixed BY CONSTRUCTION against that script
and names nothing the script does not.

The one genuinely new state, `awaiting_verdict`, is the gap between the verifier's
`opens_at` and the human transcription of its result. It introduces no constant: it keys
on `earliest_possible_verdict_at`, which for a complete window is already
`_midnight_after(verdict_window_close_date)` — byte-for-byte the verifier's `opens_at`.

## ⓒ Two axes, not one — `declared_outcome` is separate from `state`

Checkpoint 1's deepest finding: `state` was being asked to carry both *what the study
answered* and *whether the sleeve can operate*. Those come apart. A reviewed `pass` naming
an LSE candidate is `unavailable` (its venue is not session-checkable, #2603/#2312) — and
under a single axis the surface then cannot say a verdict was ever reached.

So the response carries both:

- `declared_outcome: Literal["pass", "cash"] | None` — what was transcribed, independent
  of whether it can be acted on. `None` means nothing has been transcribed.
- `state` — operational, as below.

## The state machine

Precedence is top-down; the **first matching row wins**, and the rows are not disjoint.

| # | state | condition | `ready` |
| --- | --- | --- | --- |
| 1 | `unavailable` | `missing_candidate_ids`, or the declared constants are inconsistent, or a `pass` names a sleeve that is not session-checkable | no |
| 2 | `ready` | outcome `pass`, instrument id in candidates with a coverage row, non-blank evidence ref, venue session-checkable | **yes** |
| 3 | `cash` | outcome `cash`, non-blank evidence ref | no |
| 4 | `awaiting_verdict` | no outcome declared, the fifth common date exists, and `now >= earliest_possible_verdict_at` | no |
| 5 | `evidence_collecting` | otherwise | no |

`ready` remains true for exactly one state. `require_selected_core_instrument` and
`_core_pool_activation_ready` key on `.ready`; the predicate reaching `ready` is unchanged
except for the added `outcome == "pass"` term, which the migration below supplies
everywhere it was previously implied.

ⓒ **Row overlaps are real and row 1 is why they are safe.** A valid `pass` where a
*different* candidate is missing from `instruments` matches rows 1 and 2; `cash` with an
instrument id set matches 1 and 3; an undeclared outcome with a stray evidence ref and a
closed window matches 1 and 4. Each resolves to `unavailable`. Every one of those three
combinations gets its own test — precedence that is only true by reading order is
precedence that a later edit silently reverses.

ⓒ Consequently row 4 is **not** an unconditional "iff": it is reached only when the
constants are also consistent.

### ⓒ Row 4's guard, and the corrected rationale

The guard is `verdict_window_close_date is not None and now >= earliest_possible_verdict_at`.

Checkpoint 1 corrected the reasoning I had written for it. With a single `now`, an
incomplete window always yields a strictly future bound, so the completeness term is
**defensive rather than load-bearing today** — and my stated reason ("on the projected day
the bound is `midnight_after(today)`") was wrong, since several remaining sessions push
the bound past today. It stays because it is the honest predicate: the fact being asserted
is "the fifth common date exists", which is what `earliest_possible_verdict_at:146` and
`evaluate:157` both branch on. A guard that is correct only via an argument about the
other branch's arithmetic is one refactor from being wrong.

### ⓒ One clock

`load_core_selection` captures **one** timezone-aware `now` and passes it to both the
state classification and `earliest_possible_verdict_at`. Two `datetime.now()` calls could
straddle midnight and report `evidence_collecting` beside a bound that has passed — the
exact disagreement `CoreSelection.earliest_possible_verdict_at`'s docstring already
forbids ("a row cannot report 3/5 next to a bound derived from a later population").
Naive-stamp rejection is unchanged.

⚠ Noted, not fixed: the loader reads application time while the verifier reads database
`now()`. Both run on one box here, and unifying them is a wider change than this ticket.

## Recording the outcome

Three hand-written constants, transcribed after the verifier is opened:

```python
SELECTED_CORE_OUTCOME: Final[Literal["pass", "cash"] | None] = None
SELECTED_CORE_INSTRUMENT_ID: Final[int | None] = None
SELECTED_CORE_EVIDENCE_REF: Final[str | None] = None
```

Consistency rules. All produce `unavailable` with a `configuration_error`, and ⓒ **the
first failing rule in this order supplies the message** — so a half-written verdict
reports one specific fault rather than whichever check happened to run last:

1. `SELECTED_CORE_OUTCOME` is neither `None`, `"pass"` nor `"cash"` → unrecognised
   outcome. ⓒ Checked at **runtime**: `Literal` is a typecheck-time claim, and a typo'd
   outcome that fell through to `evidence_collecting` would be the silent-wrong-state case
   this whole ticket is about.
2. `SELECTED_CORE_OUTCOME is None` and either other constant is set → half-written verdict.
3. evidence ref absent or blank after `strip()` → unreviewed outcome.
4. `outcome == "cash"` and an instrument id is set → contradictory verdict.
5. `outcome == "pass"` and the instrument id is absent, or not in
   `CORE_SELECTION_CANDIDATE_IDS` → not a declared candidate.
6. `outcome == "pass"` and no coverage row exists for that id → candidate missing from `instruments`.
7. `outcome == "pass"` and the named sleeve's `asset_class` is not session-checkable →
   the existing #2603 refusal, wording unchanged.

⚠ **`cash` is NOT inferred from "ref set, id absent".** That encoding makes a genuine
mis-edit (someone sets the ref and forgets the id) indistinguishable from a declared cash
verdict, which is the current defect turned inside out. The outcome is stated explicitly
or it is not stated.

### ⓒ REBUTTED: do not gate transcription on our own coverage read

Checkpoint 1 asked for a rule refusing a `pass`/`cash` transcribed before five common
dates exist. Declined, and the reason is the point of the design: **the sealed
declaration is the authority for what was measured, our coverage read is not.** Pruned,
re-ingested or re-bucketed observations must never retract a reviewed verdict — a verdict
that evaporates because a row moved is strictly worse than one transcribed early, and the
runbook (`docs/operator/runbooks/core-sleeve-enablement.md:41`) already places that trust
in the reviewed transcription step.

### ⓒ Output-field invariants

- `cash`: `evidence_ref` **retained**, `selected_instrument_id` and `selected_symbol` null.
  Today's materialisation drops the ref whenever the selection is not complete, which
  would erase the only pointer to the study that produced the answer.
- `unavailable`: `declared_outcome` retained (that is the axis's purpose); selected
  identity fields null, because they are by definition not trustworthy in that state.
- `ready`: unchanged.

### ⓒ Migration — this narrows unless every site moves

Adding a required `outcome` term means an existing valid `(instrument_id, evidence_ref)`
pair stops being `ready`. That would narrow mandate enablement, pool activation and both
executor selection checks. Every site that sets the two constants moves in this PR:

- `tests/test_2603_core_selection.py` (selection fixtures)
- `tests/fixtures/core_restart.py` (restart harness)
- `docs/operator/runbooks/core-sleeve-enablement.md` (the operator's transcription step)

A test asserts the new predicate with `outcome="pass"` is **equivalent** to the old
two-constant predicate, so the migration is proved rather than assumed.

## API + blockers

`CoreSleeveResponse` gains `declared_outcome` and widens `state`.

Two new blocker codes, because `core_evidence_collecting`'s detail sentence is false in
both new states:

- `core_verdict_untranscribed` — "#2833's five-date window closed at {t}. The sealed
  verifier can be opened; no reviewed outcome has been recorded yet."
- `core_verdict_cash` — ⓒ "#2833's reviewed verdict is cash: no candidate passed every
  declared rule. No core sleeve is adopted." **Not** "failed the 60 bps bar" — that was
  false. `verify_2833_core_selection.py:178-204` refuses on `incomplete_population`,
  `spread_unmeasured`, `fx_unmodelled` and `not_proved_real_long_x1` as well as
  `cost_above_60_bps`, and a candidate can be cheap and still fail.

ⓒ **`core_verdict_cash` must not change `execution_action` or `can_resume`.** Recovery of
a pending order does not depend on selection readiness
(`app/api/strategies.py:3753`, `strategy_core_executor.py:348`), so a cash verdict can
legitimately coexist with `execution_action="resume"`. The blocker is descriptive of
*new entry*, not of the whole surface, and its detail says so.

### ⓒ `require_selected_core_instrument`'s error

Today it raises "cannot be enabled until #2833 completes its five-trading-day cost
verdict" for every non-ready state — false once the verdict IS complete and says cash. It
becomes state-specific: cash names the verdict, `unavailable` names the configuration
error, and only `evidence_collecting` / `awaiting_verdict` keep a wait-shaped message.

## Frontend

`StrategyPortfolioLens.tsx:473` stops deriving a verdict from `!ready`. One badge per
state:

| state | badge | tone |
| --- | --- | --- |
| `ready` | `Ready` | ok |
| `cash` | `Cash` | warn |
| `awaiting_verdict` | `Verdict due` | warn |
| `evidence_collecting` | `Collecting evidence` | warn |
| `unavailable` | `Unavailable` | warn |

ⓒ The same defect runs through the card's copy, and fixing only the badge leaves the
sentence that says it in longhand:

- `:488` `Instrument` tile — `selected_symbol ?? "Cash"` / hint `"No sleeve adopted"`.
  Becomes `Cash` only in the `cash` state; `—` otherwise, with a hint that matches the
  state rather than asserting a future one.
- `:483` `"Provisional until the sealed verifier opens"` — false once it has opened.
- `:229` control copy `"locked until the core instrument passes #2833"` — implies a
  pending positive result even when the study has completed negatively.

`types.ts` unions widen to match; a bare `string` there would be the #1808 class in reverse.

## Tests

Pure-logic and table-driven (no DB), over a classifier taking
`(outcome, instrument_id, evidence_ref, asset_class, selected_candidate_present,
missing_candidate_ids, window_close_date, now)` — ⓒ the last three added because the
first draft's inputs could not distinguish "the selected candidate is absent" from
"another candidate is absent", and could not reach the boundary cases at all.

Covered: every row of the state table; each of the seven consistency rules and the
message it wins with; the three overlap combinations above; the fifth date before /
exactly at / after midnight; a sixth date not moving an opened boundary; ⓒ the
old-vs-new predicate equivalence for `ready`; and the evidence-ref retention invariants.

ⓒ The readiness property is **not** "exactly one state has `ready=True`" — that says
nothing about which inputs reach it. It is the equivalence test above plus a refusal
assertion for every invalid combination.

Frontend: one test per badge state. ⓒ The "no `Cash`" assertion is scoped to the badge and
instrument tile, not the page — `StrategyPortfolioPanels.tsx:533` legitimately renders
"Cash reserve", and a global assertion would fail for the wrong reason.

## Out of scope

- Which candidate wins. The sealed population is not read.
- ⓒ #2312's LSE work stays parked, and the argument is a **prior, not a proof**. The
  pre-declaration 2026-08-24 observations the declaration explicitly excludes as
  already-seen give p75 spreads `SPY.RTH` 0.262 bps · `CSPX.L` 1.666 · `IUSA.L` 2.681.
  That is evidence about a different window; an LSE name can still win on the sealed one,
  and `SPY.RTH`'s non-cost refusal paths are `incomplete_population`, `fx_unmodelled` and
  `not_proved_real_long_x1`, not completeness alone. The park is a value-order judgement,
  not a proof that LSE cannot win.
- Writing any verdict constant. This ships all three `None`, i.e. inert.
- Unifying the loader's application clock with the verifier's database clock.
