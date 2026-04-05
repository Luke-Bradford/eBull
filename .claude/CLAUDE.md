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
   - WARNING: fix on the same PR, or raise a `tech-debt` issue before merging. Cannot merge with unactioned WARNINGs.
   - NITPICK: fix if trivial (it usually is); if genuinely out of scope, raise a `tech-debt` issue. Cannot merge with unactioned NITPICKs.
   - PREVENTION: extract every prevention note to the pre-push checklist or the relevant skill file before merging.
   - Reply to each comment with what was done + the commit SHA. Nothing silently discarded.
6. **Re-run lint, typecheck, and format check before pushing a follow-up.**
7. **Every push resets the review requirement.** An APPROVE on a prior commit does not carry forward.
8. **Merge only after:**
   - APPROVE on the most recent commit
   - All BLOCKINGs fixed
   - All WARNINGs fixed OR have a `tech-debt` issue number in the reply
   - All NITPICKs fixed OR have a `tech-debt` issue number in the reply
   - All PREVENTION notes extracted to checklist/skill files
   - CI green on the most recent commit

## Pre-push checklist

Run these before every push:

```
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass. Fix failures — do not bypass them.

If `uv` is not on PATH in the Claude Code bash shell, ask the user to add the uv
install directory to `~/.bashrc` (e.g. `export PATH="$HOME/.local/bin:$PATH"` on
Linux/Mac, or the equivalent Windows Git Bash path). Run `where uv` or `which uv`
to find the location. Do not skip the checks — pushing to CI without them wastes
review rounds on issues that should be caught locally.

## Handling review comments

After every push, poll `gh pr view <n> --comments` and `gh pr checks <n>`.

When the review posts:
- Read every comment before doing anything else.
- Address ALL severities — BLOCKING, WARNING, and NITPICK.
- BLOCKING: fix on the same PR before any further push.
- WARNING: fix on the same PR, or open a `tech-debt` labelled issue and reference it
  in a reply before merging.
- NITPICK: fix if trivial (it usually is); if genuinely out of scope, open a
  `tech-debt` issue and reference it in a reply. Never silently leave it.
- Reply to each comment with what was done + the commit SHA.

Do not push a follow-up commit to address CI failures without first reading the
review — the review may already be posted, and pushing again without reading it
wastes a review round.

## Self-review before pushing

Before committing, open `git diff origin/HEAD` and read top to bottom. The review agent reads the same diff fresh with no knowledge of intent — match that posture.

Full checklist with rules and examples: `.claude/skills/engineering/pre-push-checklist.md`

Detailed standards by category:
- SQL correctness: `.claude/skills/engineering/sql-correctness.md`
- Python hygiene: `.claude/skills/engineering/python-hygiene.md`
- Test quality: `.claude/skills/engineering/test-quality.md`
