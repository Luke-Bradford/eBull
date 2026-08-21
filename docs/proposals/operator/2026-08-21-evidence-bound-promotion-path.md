# Evidence-bound operator promotion path (#2770)

Status: proposal, 2026-08-21. Revised after Codex checkpoint 1 (52 findings; the
dispositions that changed the design are marked ⓒ below). Implements the ordered operator
path from a registered capital candidate to `paper_enabled`. Does not touch the live gate.

## Problem

`promote_strategy` (`app/services/strategy_control_plane.py:422`) is a strict primitive,
but no production caller advances a strategy forward. The only API caller is
`POST /strategies/{id}/lifecycle` (`app/api/strategies.py:3420`), which targets `paused`
and `retired` only. So `None → research_candidate → historical_validated →
forward_observation → paper_enabled` is unreachable from outside the test suite, and
`/strategies` can allocate an already-`paper_enabled` strategy but cannot get one there.

Exposing `promote_strategy` directly is unsafe. It accepts caller-supplied `result_ids`
and validates each *individually*. A caller could pin a favourable subset — say the three
windows where the strategy looked good — and every pinned row would pass its own checks
while the promotion rested on a cherry-picked denominator.

## Source rule

Nothing here is reasoned out from first principles; all three components are existing
declarations.

1. **The six windows** — `strategy_recent_evidence.RECENT_EVIDENCE_WINDOWS`
   (`primary-2022-plus`, `rolling-36m`, `rolling-24m`, `year-2022`, `year-2023`,
   `year-2024`), each validated at construction against `HOLDOUT_BOUNDARY`,
   `EVALUATION_WINDOW_END` and `INTRADER_CAPTURE_DATE`.
2. **The four arms** — the ambiguity × quarantine cross product
   (`best_case|worst_case` × `masked|admitted`), which `app/api/strategies.py:1990-1996`
   already treats as the completeness condition for an `EvidenceWindow` to read `complete`.
3. **The comparability predicate** — `_current_identity_pins()`
   (`app/api/strategies.py:1712`). ⓒ Its own docstring is the rule: *"These ARE the result
   identity — differing on any of them means the numbers are not comparable."* It names
   `namespace='hold_out'`, `corpus_version`, `cost_model_id`, `sizing_rule`,
   `benchmark_rule`, `return_basis`, `ambiguity_rule_version`,
   `position_rule_set_version`, `outcome_rule_set_version`, `input_rule_set_version`.

So the denominator is **24 identity combinations, all on one comparability basis**. This
adds no new rule; it moves the existing one out of the read model into a transaction that
can refuse. ⓒ Codex raised cross-arm/cross-window coherence (findings 3-5, 49, 52) as
unaddressed — binding the pins closes it without inventing a parallel check.

⚠ `_current_identity_pins()` currently lives in `app/api/strategies.py`. A service must not
import an API module, so it **moves to `app/services/strategy_result_identity.py`** and
`app/api/strategies.py` imports it from there. Its docstring's stated purpose ("named once
so the prior-version reader cannot drift from the current-version one") is exactly why the
third reader must bind the same object rather than copy it.

The stage order is `strategy_control_plane._NEXT_STAGE`, and single-entry is settled:
*"Live-gate evidence windows are single-entry (#2612)"* — a pair arrives at
`forward_observation` at most once and `paper_enabled` at most once, enforced by the DAG
and by the partial UNIQUE `idx_strategy_promotions_one_successor` on
`(strategy_id, strategy_version, from_stage)`.

## State measured at proposal time (2026-08-21 — ⓒ transient, do not read as durable)

```sql
select strategy_id, evidence_window_id, ambiguity_arm, quarantine_arm, purpose, count(*)
from strategy_results_store group by 1,2,3,4,5;
```

16 groups across s1-s4, **every one with `evidence_window_id IS NULL`** and
`purpose = 'harness_validation'`. Zero pinned recent-evidence rows exist for any strategy,
and all six windows read `status: "missing"` on `/strategies/overview`. Backtest run 112352
(`refresh_recent: true`) is writing them now.

⚠ Consequence, stated plainly: this path is buildable and unit-testable now but **cannot be
exercised end to end** until 112352 lands the matrix *and* a manifest entry becomes
`capital_candidate` (all ten are `harness_validation`). The gate must exist before evidence
can pass through it. The PR will say so rather than implying an operator ran it.

### ⓒ Duplicates on the pinned identity are real — verified on the full population

Codex flagged an internal contradiction: "complete 24-combination bundle" plus "duplicates
refuse" plus "a re-run may legitimately add rows" cannot all hold. Checked, and the
contradiction is real:

```sql
select count(*), count(distinct (strategy_id, strategy_version, namespace,
       window_start, window_end, ambiguity_arm, quarantine_arm,
       corpus_version, cost_model_id))
from strategy_results_store;                              -- 328 | 268
```

Ten groups carry exactly 2 rows on one identity, two distinct `result_version`s each — an
s1 re-run. The store's uniqueness is `strategy_results_unique (strategy_id,
strategy_version, result_version)`, **not** the window/arm identity, so a re-run adds
rather than replaces. `refresh_recent: true` re-runs are routine, and pinned results are
`ON DELETE RESTRICT`, so "refuse on duplicate" would make any re-run strategy permanently
unpromotable.

**Resolution: the loader selects the latest row per identity** —
`DISTINCT ON (window_start, window_end, ambiguity_arm, quarantine_arm) … ORDER BY …,
result_id DESC`. `result_id` is the primary key, so the order is total and deterministic.
Latest-wins is the safe direction: a re-run that measured worse supersedes a better older
row, and the caller cannot influence the choice. The exact selection stays auditable
through the pinned ids and the evidence digest.

⚠ Incidental, not this ticket's to fix: the read model tolerates the same duplicates
silently — `app/api/strategies.py:1989` builds `arm_keys` as a **set**, so eight rows on
four identities render `complete`. Noted on the PR; no ticket, per the standing order.

## Design

### New module `app/services/strategy_operator_promotion.py`

```python
OperatorAction = Literal[
    "register_research_candidate",
    "validate_historical",
    "start_forward_observation",
    "enable_paper",
]
```

- `allowed_operator_action(stage) -> OperatorAction | None` — ⓒ derived from `_NEXT_STAGE`
  by taking each stage's successors **minus** `{paused, retired, live_enabled}`, which are
  the lifecycle and dedicated-gate edges this path deliberately excludes. The guard test
  asserts that filtered projection equals the action map, in both directions; Codex
  (44) was right that a naive bidirectional identity is ill-defined.
- `recent_evidence_refusals(rows) -> tuple[str, ...]` — pure, over
  `(evidence_window_id, ambiguity_arm, quarantine_arm, result_id)`:
  - `recent_evidence_window_missing:{window_id}` — window contributes nothing. ⓒ When a
    window is wholly absent this is emitted **alone**, not with four `arm_missing`
    companions (Codex 8: diagnostic cardinality).
  - `recent_evidence_arm_missing:{window_id}/{ambiguity}/{quarantine}` — window present,
    arm absent.
  - `recent_evidence_window_unknown:{window_id}` — a hold-out row on this comparability
    basis naming a window outside the declared six. ⓒ Reachable, because the loader
    selects on `namespace`/pins and **not** on a window-id whitelist (Codex 2 was right
    that a whitelist SQL filter makes this undetectable); classification happens in Python.
  - `recent_evidence_arm_unknown:{window_id}/{ambiguity}/{quarantine}`.
  Rows with a null window id or arm are ignored by the loader's `WHERE` and cannot reach
  here; the function still refuses defensively on a null arm rather than crashing.
  Refusals are returned in a stable sorted order so a diff of two runs is readable.
  Empty tuple means complete. **No "close enough" tier.**
- `recent_evidence_ref(strategy_id, strategy_version, pairs) -> str` — ⓒ
  `recent-evidence-v1+{sha256 hex, full 64 chars}` over
  `json.dumps([...], sort_keys=True, separators=(",", ":"))` of the sorted
  `[window_id, ambiguity, quarantine, result_id]` lists plus the strategy identity.
  Canonical JSON removes the delimiter ambiguity Codex raised (9); the digest is not
  truncated (10); and `GOVERNANCE_GATE_VERSION` is **excluded** from the payload (11, 13) —
  the digest identifies the evidence *set*, and the gate version is already recorded on the
  promotion row in its own column. Same set ⇒ same ref, whatever the gate.
- `load_authoritative_recent_evidence(conn, strategy_id, strategy_version)` →
  `RecentEvidenceBundle(result_ids, evidence_ref, refusals)`. One SELECT, binding
  `current_identity_pins()` and `evidence_window_id IS NOT NULL`, `DISTINCT ON` as above.
  **Caller-supplied ids never enter.**
- `advance_strategy(conn, *, strategy_id, action, advanced_by, reason)`:
  1. resolve the current `strategy_version` from the registry — never from the request; an
     unknown `strategy_id` refuses with `unknown_strategy` (Codex 15);
  2. `lock_strategy_control` — the same per-version advisory lock `promote_strategy` takes;
  3. `stage = current_stage(...)`, read **after** the lock; refuse unless
     `allowed_operator_action(stage) == action`;
  4. ⓒ the `capital_candidate` purpose is required **only for the three evidence actions**.
     `research_candidate` is not in `_EXTERNAL_EVIDENCE_STAGES`, so the primitive permits a
     non-candidate to register; refusing it early would silently narrow the declared DAG
     (Codex 17);
  5. assemble evidence per action (below);
  6. call `promote_strategy`.

⚠ ⓒ The advisory lock serialises *promotions*, not result or assessment writers (Codex 18).
A hold-out row may commit between the loader's SELECT and the INSERT. That is the same
non-atomicity `promote_strategy` already documents for criterion 5's counts, and the
consequence is bounded: the pinned set and its digest describe exactly what was read, so a
later row makes the *next* promotion different, never this one wrong.

### Evidence per action

| action | to_stage | evidence bound | `pinned_result_count` |
| --- | --- | --- | --- |
| `register_research_candidate` | `research_candidate` | none | 0 |
| `validate_historical` | `historical_validated` | complete 24-combination bundle | 24 |
| `start_forward_observation` | `forward_observation` | complete bundle ⊇ the set pinned at `historical_validated` | 24 |
| `enable_paper` | `paper_enabled` | fresh passing prospective assessment | ⓒ 0 — paper pins no results (Codex 39) |

**"Must not weaken or replace historical evidence"** compares **identities**, not result
ids: resolve the `result_id`s recorded against the `historical_validated` promotion back
to their `(window, ambiguity, quarantine)` combinations and refuse
`recent_evidence_weakened:{window}/{ambiguity}/{quarantine}` for any no longer covered.

⚠ ⓒ **An id comparison is wrong, and Codex checkpoint 2 caught it as a P1 in the first
implementation.** A re-run between the two promotions replaces every id;
`select_latest_rows` returns the new ones; every previously pinned id then reads as
"dropped". Because the store is append-only and pins are `ON DELETE RESTRICT`, that
verdict is permanent — one routine `refresh_recent` re-run would block
`start_forward_observation` forever. This is the same failure the duplicate rule above
was written to avoid, reintroduced two functions later, which is worth recording: the
rule is *superseding a row is not weakening the evidence; ceasing to cover a window or
an arm is.*

The identity check cannot fire while both steps demand a complete matrix over the same
declared windows — two complete matrices have identical identity sets. What it catches is
the declared set MOVING between the steps: shrink `RECENT_EVIDENCE_WINDOWS` and the
second "complete" matrix is smaller than the first, which is a real weakening and is
invisible to every completeness check.

**Paper approval** ⓒ requires all of:

- a current effective assessment policy, and a `passed` assessment for this exact
  `(strategy_id, strategy_version)`;
- freshness by the **same predicate the overview applies** (`app/api/strategies.py:2147`):
  `checked_at >= as_of - max_assessment_age_days` and `checked_at <= as_of + 5s`. The 5 s
  future tolerance is inherited deliberately, not by accident (Codex 28) — one predicate,
  one behaviour, and the page cannot say "fresh" where the transaction says "stale";
- ⓒ `checked_at >= the promoted_at of this version's forward_observation` — an assessment
  computed before forward observation began is not evidence *from* it (Codex 24). Without
  this, all four advances can run back-to-back on backtest evidence alone.
- ⓒ where several passing assessments qualify, the **most recent `checked_at`** is pinned,
  ties broken on the greater `assessment_id` (Codex 29).

Refusals reuse the existing vocabulary — `prospective_assessment_policy_missing`,
`prospective_assessment_missing`, `prospective_assessment_not_passed`,
`prospective_assessment_stale` — plus `prospective_assessment_predates_forward_observation`.
`evidence_ref` is `prospective-assessment-v1+{assessment_id}@{policy_id}` (Codex 26).

⓪ **Not built, deliberately**: a minimum forward-observation duration (Codex 25). Duration
floors belong to the live gate — #2599's contract-frozen forward-shadow floor reads
`forward_days` off these windows — and paper is deliberately the cheaper rung. Adding a
second floor here would put the same policy in two places.

### API

`POST /strategies/{strategy_id}/advance`, `require_session` — an operator authorisation
recorded with a named `changed_by`, matching `update_core_mandate`'s reasoning that a
service token has no operator identity to attribute a promotion to.

Body: `{action: OperatorAction, reason: str}`. **No `to_stage`, no `result_ids`, no
`strategy_version`** — the three inputs a browser must not supply. `reason` is
`min_length=1`, whitespace-stripped, `max_length=500`.

Responses: 200 `{strategy_id, strategy_version, from_stage, stage, promotion_id,
evidence_ref, pinned_result_count}`; **409** for every `StrategyControlError`; 404 for an
unknown strategy; 422 for a malformed action (FastAPI's own enum validation).

⓪ ⓒ `UniqueViolation` is **not** caught (Codex 34-37). The advisory lock serialises the
two requests, so the second reads the advanced stage and refuses with
`invalid promotion transition` before any INSERT. Catching it would need constraint-name
discrimination and a savepoint to avoid leaving the transaction aborted, to handle a race
the lock already excludes; the indexes stay as the backstop they were designed to be. Two
identical submissions therefore give one 200 and one 409 — duplicate *prevention*, which is
what the ticket asks for, and not idempotent replay, which it does not.

`live_enabled` is unreachable: not an `OperatorAction`, and `promote_strategy` refuses it
independently.

### Read surface

`StrategyOverview` gains `next_operator_action: OperatorAction | null` and
`next_operator_action_refusals: list[str]`. ⓒ The action is **named alongside its
refusals**, not nulled by them (Codex 42); null means a terminal stage with no forward
edge. ⓒ This is advisory: it is computed from the same pure functions the write path uses,
but a page cannot be transactionally coupled to a later request (Codex 30, 40, 41), so the
transaction stays authoritative and a stale page gets a 409 it must render.

⚠ ⓒ **The paper step must report the assessment refusals the overview has already
computed** (Codex checkpoint 2, P2). Without that, reaching `forward_observation` renders
an enabled "Approve for paper trading" button however missing, failed or stale the
assessment is — one click for a guaranteed 409. The refusals passed in are necessarily a
SUBSET of what the transaction checks (the overview does not compute
`prospective_assessment_predates_forward_observation`, which needs the promotion
timestamp), so an enabled button claims that nothing KNOWN refuses, not that the request
will succeed.

`/strategies` renders the action in the research section with its refusals, disabled while
in flight, and refetches on success.

## Tests

Pure (fast tier), per "prefer pure policy over real DBs":

- matrix table tests — complete; window absent (one refusal, not five); arm absent;
  duplicate identity resolved to latest; unknown window; unknown arm; empty input; refusal
  ordering stable;
- `evidence_ref` — determinism, order-insensitivity, sensitivity to one swapped id, and
  invariance to `GOVERNANCE_GATE_VERSION`;
- `allowed_operator_action` vs the filtered `_NEXT_STAGE` projection, both directions;
- the weakening comparison — superset passes, dropped id refuses;
- assessment selection — missing policy, missing, not passed, stale, predating forward
  observation, most-recent-wins among several passing.

DB (`-m db`), one integration test per genuinely new SQL mechanism:

- the loader returns exactly one row per identity in the presence of a seeded duplicate,
  and it is the higher `result_id`;
- a partial matrix refuses and writes no promotion;
- the endpoint exposes no `result_ids` field (structural anti-cherry-pick assertion);
- a second identical submission returns 409 and leaves exactly one promotion row.

Frontend: the action renders with refusals; absent for a `harness_validation` strategy;
a 409 surfaces as an error and the row refetches.

## Out of scope

Live promotion, the paper executor, funding decisions, and flipping any manifest entry to
`capital_candidate` — that is an evidence decision on its own ticket.
