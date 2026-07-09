# pr-authoring

Mandatory skill for writing PR descriptions.

## Goal

A reviewer should be able to understand the change, its invariants, and its tradeoffs without reading your mind.

If the PR description is weak, the review quality drops and the review rounds increase.

## Required PR sections

Every PR description must cover these sections. Exact heading wording may vary (recent merged PRs use e.g. `## What` / `## Security model`), but each area must be explicitly present. The repo template `.github/pull_request_template.md` additionally supplies the Issue-reference line (`Closes #N` — see the CI gate below) and the checklist.

### What changed
Describe the code changes concretely.
Name the main files or modules touched.

### Why
Explain the problem being solved and why this approach was chosen.

### Schema / migration impact
List:
- new migrations
- schema changes
- data backfill expectations
- compatibility notes

If none, say "None".

### Invariants checked
State the important correctness rules you preserved.

Examples:
- atomic versioning
- idempotent upserts
- no external I/O inside transactions
- latest-row queries ordered explicitly
- one audit row per invocation

### Failure paths considered
List the failure cases you handled.

Examples:
- empty table
- missing optional identifiers
- provider unavailable
- stale recommendation
- missing quote fallback

### Security model
State the security story explicitly: input surfaces touched, and whether any execution path is affected (if so, confirm the execution guard is called and `decision_audit` is written before any order is staged). If none, say "No execution path touched." (Template heading: "Security and audit model".)

### Tests added
Summarise the behaviour covered by tests.
Do not just say "added tests".

### Conscious tradeoffs
Explain what you intentionally did **not** do.

Examples:
- "Used JSONB column instead of separate table in v1"
- "Skipped full-text filing persistence; kept provider payload only"

### Tech debt opened
List any explicitly deferred items with issue numbers.
If none, say "None".

**Filings-ETL / parser / schema-migration PRs** must also record the dev-verify evidence per project CLAUDE.md "Definition of done" clauses 8-12: instruments smoked (default panel AAPL/GME/MSFT/JPM/HD) + figure observed, cross-source figure compared + source, `POST /jobs/sec_rebuild/run` invocation + outcome, rollup-endpoint check — each with the commit SHA it was verified at.

## PR description quality bar

A good PR description answers:
- what changed
- why now
- what can fail
- what was consciously deferred
- how the reviewer can evaluate correctness

A bad PR description is just a restatement of the ticket title.

## `verify-issue-link` CI gate — every `#N` in the TITLE needs a verb-link in the body

The `verify-issue-link` job (`.github/workflows/pr-issue-link.yml`) greps **every** `#N` out of the PR *title* and fails unless each appears in the body preceded by a recognised verb + whitespace/colon. Actual verb regex (matched case-insensitively): `(close[sd]?|closing|fix(e[sd]|ing)?|resolve[sd]?|resolving|refs?|references?|track[sd]?|tracking|part of|umbrella)[[:space:]:]+#N` — bare `Fix #N` / `Umbrella: #N` pass; a left non-word boundary blocks substrings like `unfixes #N`; a trailing digit boundary means `Closes #869` does **not** satisfy a title `#86`. Inline code, fenced code blocks, and HTML comments are stripped from the body first, so a `Closes #N` inside backticks or `<!-- -->` does **not** count.

Trap: a title like `feat(#1384): … (#1343 PR-B)` references **two** issues. `Closes #1384` satisfies one; the other (`#1343`) must also be verb-linked — bare prose ("half of #1343") fails. Fix: add `Part of #1343.` (or `Refs #1343.`). Don't reference a closed/parent issue in the title unless you'll verb-link it. (PR #1385.)

## `perf-claim-lint` CI gate — perf-claim PRs need artifacts + 3 body sections

Triggered by PR label `perf` OR a `## Performance impact` header in the body. Requires committed baselines `var/perf_baselines/<ticket>-<sha>.{txt,json,manifest.yaml}` with row counts meeting `scripts/perf_bench/floors.yaml`, plus line-exact body sections `## Sibling-shape audit`, `## Rollback criteria`, `## Post-deploy SLO`. Full protocol: `.claude/skills/engineering/etl-perf-claims.md`.

## Pre-submit check

Before opening or updating the PR, ask:
- would a reviewer know the intended invariants from this description?
- would they know why the design is shaped this way?
- would they know what was deferred?
- would they know what tests prove it?

If not, improve the description before pushing again.
