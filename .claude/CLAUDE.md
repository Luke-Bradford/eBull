# trader-os project instructions

## Project role

You are helping build a long-horizon AI-assisted investment engine for eToro.

## Non-negotiables

- This is not a day-trading toy.
- Research can be AI-heavy.
- Execution must be hard-rule constrained.
- Every trade path must be auditable.
- Prefer simple, testable systems over fragile cleverness.
- Do not add libraries casually.
- Keep dependencies justified and minimal.

## Risk posture

- Demo-first
- small-capital live later
- long only in v1
- no leverage
- no shorting
- no silent bypass of failed checks

## Build priorities

1. tradable universe
2. market data
3. filings and news ingestion
4. thesis engine
5. ranking engine
6. portfolio manager
7. execution guard
8. ledger and tax engine

## Code style

- Small files where practical
- Clear type hints
- Obvious names
- Minimal abstraction until duplication becomes real
- Avoid magical framework behaviour

## Data discipline

- Persist raw payloads when useful
- Keep normalized tables explicit
- Version score models
- Version thesis output
- Never place a trade without a rationale record

## AI discipline

AI may:
- summarize
- classify
- compare
- critique
- draft thesis text

AI may not unilaterally override:
- position limits
- concentration rules
- stale-data checks
- kill switches
- account mode constraints

## Output preference

When implementing a module:
- start with schema and interfaces
- then service logic
- then tests
- then integration glue

## Branch and PR workflow

Every piece of work follows this sequence without exception:

1. **Create a branch before touching any code.**
   Branch naming: `feature/<issue-number>-short-description` or `fix/<issue-number>-short-description`.
2. **All commits go on the branch.** Never commit to main directly.
3. **Push and open a PR.** The PR description must be self-contained: what changed, why, the audit/execution model, and any conscious tradeoffs.
4. **Wait for the Claude review.** The review workflow runs automatically on every push. Poll `gh pr view <n> --comments` and `gh pr checks <n>` — do not proceed until the review has posted and CI is green.
5. **Address every review comment on the same branch.**
   - BLOCKING: must be fixed before merge.
   - WARNING: fix on the same PR, or raise a `tech-debt` issue before merging.
   - NITPICK: fix if trivial, otherwise raise an issue.
   - Reply to each comment with what was done + the commit SHA. Nothing silently discarded.
6. **Re-run lint, typecheck, and format check before pushing a follow-up.**
7. **Every push resets the review requirement.** An APPROVE on a prior commit does not carry forward.
8. **Merge only after APPROVE on the most recent commit with CI green.** Then delete the branch.

## Pre-push checklist

Run these before every push:

```
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

All three must pass. Fix failures — do not bypass them.
