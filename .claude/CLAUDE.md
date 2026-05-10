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

### ETL / parser / schema-migration additional clauses

Any change that touches filings ETL, parsers, ingest pipelines, or schema migrations affecting ownership / fundamentals / observations data is **not done** until ALL of the following are also true:

8. **Smoke-tested against 3-5 known instruments** in dev DB. Default panel: `AAPL`, `GME`, `MSFT`, `JPM`, `HD`. The PR description records which instruments were exercised and the operator-visible figure observed.
9. **Cross-source verified for at least one fixture** against an independent reputable source (e.g. gurufocus, marketbeat, EdgarTools golden file, SEC EDGAR direct). PR description records the source + the figure compared.
10. **Backfill executed** — not "queued for nightly", not "will run next cron". For schema/parser changes affecting ownership or observations: run `POST /jobs/sec_rebuild/run` with the appropriate scope (instrument, filer, or source) on dev DB. PR description records the job invocation + outcome.
11. **Operator-visible figure verified on the live chart / endpoint** after backfill. Concretely: hit the relevant rollup endpoint (e.g. `/instruments/{symbol}/ownership-rollup`) and confirm the figure renders correctly with the new data path.
12. **PR description records the verification step + commit SHA** for each of clauses 8-11. Reviewers should be able to read the PR and know exactly which instruments + sources + figures were checked, and at which commit.

### Operator runbook — after schema / parser change

When a PR lands that changes how ownership, insider, institutional, blockholder, treasury, or DEF 14A data is parsed or stored, the operator follow-up is:

1. **Identify scope:** which `(subject, source)` triples need re-ingest? If parser-version bumped on Form 4, scope = `{ "source": "sec_form4" }`. If a single CIK had a tombstone-resolution fix, scope = `{ "instrument_id": <id>, "source": "<src>" }`.
2. **Trigger rebuild:** `POST /jobs/sec_rebuild/run` with the appropriate JSON body. The job resets the relevant scheduler rows + manifest rows to `pending` and lets the manifest worker drain them.
3. **Wait for drain:** the worker is rate-limited at 10 req/s shared. Monitor via `/jobs/sec_manifest_worker/status` (or equivalent) until pending count for the scope reaches zero.
4. **Verify operator-visible figure:** hit the relevant rollup endpoint and confirm the figure renders. For ownership changes specifically, smoke `/instruments/<symbol>/ownership-rollup` for the panel of 3-5 known instruments.
5. **Cross-source confirm:** spot-check at least one figure against an independent source.

If any step fails, do NOT consider the PR fully landed even after merge — open a follow-up ticket and reference the merge SHA.

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

1. **Before writing code** — two Codex passes:
   - **After spec is written, before user final-approves:** `codex.cmd exec "Review this spec for <feature>. Path: docs/superpowers/specs/<...>.md. Focus on correctness gaps, invariant violations, missing edge cases. Reply terse."` Fix issues before presenting spec to user for sign-off.
   - **After implementation plan is written, before first task dispatch:** same invocation against the plan doc. Catches plan-shape bugs (bad task decomposition, missing dependency, wrong contract) before any subagent starts coding.
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
            Codex + author both agree rebuttals sound + nothing else to do → merge
            Codex finds new issues? → fix, re-push, restart loop
            Codex agrees with bot against author? → fix, re-push, restart loop
```

Rule: if Codex and the author both agree the remaining bot findings are unfounded rebuttals and there is nothing else to action, that's sufficient to merge — no user rubber-stamp required. Only escalate to the user when there is a genuine judgment call Codex cannot resolve (architecture trade-off, scope decision, settled-decision change).

Never merge on rebuttal-only rounds without Codex sign-off. Never cite "the bot is wrong" as sole justification — Codex must independently agree.

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

A repo pre-push hook at `.githooks/pre-push` enforces the first
three automatically. Wire once per clone:

```bash
git config core.hooksPath .githooks
```

The hook skips pytest (~110s) — CI runs it. Bypass with `--no-verify`
only for genuine emergencies; the hook exists to stop the
push-fail-fix-repush cycle that re-spends Anthropic credits on the
review bot.

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
pnpm --dir frontend test:unit
```

Both must pass.

`test:unit` excludes `src/pages/SetupPage.test.tsx` (heavy integration). CI runs the full `test` script on push — integration tests still gate merge. Run `pnpm --dir frontend test` locally when explicitly debugging integration coverage.

## Required engineering skills

Read and apply these before pushing:

- `.claude/skills/engineering/pre-flight-review.md`
- `.claude/skills/engineering/pre-pr-fresh-agent-review.md` ← MANDATORY before push for filings ETL / schema migrations / identity resolution / observations work. Loads financial-plumbing + data-engineer + data-scientist + adversarial lenses up front so Codex catches what the bot would otherwise find post-merge.
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

### Data foundation skills (read before SEC ingest / schema / parser / metric work)

- `.claude/skills/data-sources/sec-edgar.md` — source-of-truth: endpoints, formats, identifiers, gotchas (DD-MMM-YYYY dates, 13F PRN/SH, VALUE-cutover 2023-01-03, 13D/G XML mandate, etc.), rate-limit discipline, reference impls.
- `.claude/skills/data-sources/edgartools.md` — library reference: coverage matrix, API cheat-sheet, Pydantic validation cliff (#932), version pinning, decision tree for use-vs-roll-our-own.
- `.claude/skills/data-engineer/SKILL.md` — what we own: schema invariants, two-layer ownership model, write-through pattern, settled-decisions cross-reference, "where does X come from?" FAQ, admin-page operator UX FAQ. Discoverable as the `data-engineer` skill.
- `.claude/skills/metrics-analyst/SKILL.md` — every operator-visible metric: source → transform → table → endpoint → chart, with caveats and validation steps. Discoverable as the `metrics-analyst` skill.

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
