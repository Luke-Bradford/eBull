# eBull project instructions

## Project role

You are helping build **eBull**, a long-horizon AI-assisted investment engine for eToro.

## Non-negotiables

- This is not a day-trading toy.
- Research can be AI-heavy.
- Execution must be deterministic and hard-rule constrained.
- Every trade path must be auditable.
- Prefer simple, testable systems over fragile cleverness.
- Do not add libraries casually.
- Keep dependencies justified and minimal.
- Do not silently ignore review comments.

## Risk posture

- Demo-first
- Small-capital live later
- Long only in v1
- No leverage
- No shorting
- No silent bypass of failed checks

## Build priorities

1. Tradable universe
2. Market data
3. Filings and news ingestion
4. Thesis engine
5. Ranking engine
6. Portfolio manager
7. Execution guard
8. Ledger and tax engine

## Definition of done

Work is not done until all of the following are true:

1. The implementation matches the issue and current repo decisions.
2. The code has been self-reviewed against the engineering skills.
3. Lint, format, typecheck, and tests all pass locally.
4. The PR description is complete and self-contained.
5. Review comments are all resolved as:
   - `FIXED {commit_sha}`
   - `DEFERRED #{issue_number}`
   - `REBUTTED {reason}`
6. No warning or nitpick is left hanging silently.
7. Any recurring review finding is extracted into the prevention log or a relevant skill before merge.

## Working order for every task

Follow this order unless the user explicitly says otherwise:

1. Read the issue.
2. Read `docs/settled-decisions.md`. State which decisions apply and how the plan preserves them. If none apply, say so explicitly.
3. Read `docs/review-prevention-log.md`. State which entries are relevant. If none apply, say so explicitly.
4. If implementation pressure suggests changing a settled decision or risks repeating a prevention entry, stop and surface it before coding.
5. Read the relevant engineering skills before writing code.
6. Make schema/interface changes first.
7. Implement service logic.
8. Write or update tests.
9. Self-review the diff using the pre-flight review skill.
10. Run local checks.
11. Write a complete PR description.
12. Follow the branch and PR workflow below — push, poll, wait, resolve, repeat until APPROVE on the most recent commit with CI green.

## Branch and PR workflow

1. Create a branch before touching code.
   - `feature/{issue-number}-short-description`
   - `fix/{issue-number}-short-description`
2. Commit only on that branch.
3. Push and open a PR.
After every push, poll:
- `gh pr view {pr_number} --comments`
- `gh pr checks {pr_number}`

Do not push again until:
- the Claude review has posted
- CI results are visible
- all review comments have been read

Do not push a follow-up commit for CI alone without first reading the review comments on the latest commit.
If the review has not posted yet, wait and poll again rather than continuing blindly.
4. Wait for Claude review and CI on the latest commit.
5. Resolve every review comment explicitly.
6. Re-run local checks before every follow-up push.
7. Merge only after review is satisfied on the most recent commit and CI is green.

## Codex second-opinion — mandatory checkpoints

Codex runs at exactly three points in the workflow. Non-negotiable.

1. **Before writing code** — describe the implementation plan (or spec) to Codex; read its feedback; fix issues before starting. Invocation: `codex.cmd exec "Review this plan for <feature>. Spec: <path>. Focus on correctness gaps and invariant violations. Reply terse."`
2. **Before first push** — after self-review + local gates pass, run `codex.cmd exec review` on the branch. Fix anything real before pushing.
3. **Before merging on a rebuttal-only round** — if the latest review's findings are all rebuttals (no code changes pending), run Codex to confirm the rebuttals are sound. Without this step, rebuttals are unverified and may hide real bugs the review bot *did* catch in disguise.

When Codex is NOT required:
- Follow-up pushes that fix review comments (the review bot will re-check).
- Routine edits after Codex already reviewed the plan + first diff and there is no rebuttal-only round pending.

Invocation rule: always use `codex.cmd exec` (non-interactive). Never bare `codex` (requires terminal).

## Review decision tree — who to consult in what order

```
Self-review (diff + engineering skills)
  ↓
Codex review (checkpoint 2: before first push)
  ↓
Push + wait for Claude review bot + CI
  ↓
Bot findings? → Triage each: FIXED / DEFERRED / REBUTTED
  ↓
Any rebuttals on latest review?
  ├─ No  → all fixed → merge when green + APPROVE on latest commit
  └─ Yes → Codex review (checkpoint 3: before rebuttal-only merge)
            ↓
            Codex confirms rebuttals sound? → merge
            Codex finds new issues? → fix, re-push, restart loop
  ↓
Ask the user for final approval only AFTER Codex has signed off on rebuttals.
```

Never ask the user to approve a merge that Codex has not yet second-opinioned on the rebuttal round.

## Review comment resolution contract

Every review comment must end in exactly one of these states:

- `FIXED {commit_sha}`
- `DEFERRED #{issue_number}`
- `REBUTTED {reason}`

There is no fourth state.
Do not ignore comments because they feel minor or annoying.
Do not leave warnings or nitpicks untracked.
If a comment is wrong, push back clearly and specifically.

Every PREVENTION comment must end in exactly one of these states:

- `EXTRACTED {file}` — lesson added to a skill, workflow doc, checklist, or `docs/review-prevention-log.md`
- `ALREADY_COVERED {file}` — rule already exists; cite the exact file
- `REBUTTED {reason}` — lesson does not apply; explain specifically

PREVENTION comments cannot be silently acknowledged.
Reusable engineering lessons go into skill files.
Recurring repo-specific mistakes go into `docs/review-prevention-log.md`.
Either way, the exact file must be named in the resolution reply.

## Pre-push checklist

Run these before every push:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass.

`uv run pytest` includes `tests/smoke/test_app_boots.py`, which drives
the FastAPI lifespan through `TestClient` against the real dev DB.
This is the gate that catches lifespan-only failures (bad SQL in
`master_key.bootstrap`, broken imports under `app/main.py`, migration
state mismatches) which unit tests with mocked cursors will silently
miss. If the smoke test fails, the running server is broken — fix the
root cause, do not skip it.

If the PR touches `frontend/`, also run:

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

Both must pass.

## Required engineering skills

Read and apply these before pushing:

- `.claude/skills/engineering/pre-flight-review.md`
- `.claude/skills/engineering/pr-authoring.md`
- `.claude/skills/engineering/review-resolution.md`
- `.claude/skills/engineering/python-hygiene.md`
- `.claude/skills/engineering/sql-correctness.md`
- `.claude/skills/engineering/test-quality.md`

### Frontend skills (read on any ticket touching `frontend/`)

- `.claude/skills/frontend/async-data-loading.md`
- `.claude/skills/frontend/loading-error-empty-states.md`
- `.claude/skills/frontend/safety-state-ui.md`
- `.claude/skills/frontend/api-shape-and-types.md`
- `.claude/skills/frontend/operator-ui-conventions.md`

## Settled decisions

→ Covered in the Working order above (steps 2 and 4).

## Repo discipline

- Keep provider interfaces clean.
- Keep domain logic out of providers.
- Keep migrations explicit and minimal.
- Version model outputs where required.
- Persist enough structured evidence for auditability.
- Use tech-debt issues when a review point is consciously deferred.

## Output preference

When implementing a module:
- start with schema and interfaces
- then service logic
- then tests
- then integration glue

When replying to review:
- say exactly what changed
- include the commit SHA
- if not fixing now, link the tech-debt issue
- if disagreeing, explain why concretely
