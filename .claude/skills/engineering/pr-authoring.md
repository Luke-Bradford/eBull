# pr-authoring

Mandatory skill for writing PR descriptions.

## Goal

A reviewer should be able to understand the change, its invariants, and its tradeoffs without reading your mind.

If the PR description is weak, the review quality drops and the review rounds increase.

## Required PR sections

Every PR description must contain these headings.

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

## PR description quality bar

A good PR description answers:
- what changed
- why now
- what can fail
- what was consciously deferred
- how the reviewer can evaluate correctness

A bad PR description is just a restatement of the ticket title.

## `verify-issue-link` CI gate — every `#N` in the TITLE needs a verb-link in the body

The `verify-issue-link` job greps **every** `#N` out of the PR *title* and fails unless each appears in the body preceded by a recognised verb (`close[sd]`/`closing`/`fix(es|ed|ing)`/`resolve[sd]`/`resolving`/`refs?`/`references?`/`track[sd]`/`tracking`/`part of`/`umbrella`) + whitespace/colon + `#N`. Inline code and fenced/comment blocks are stripped first, so a `Closes #N` inside backticks or `<!-- -->` does **not** count.

Trap: a title like `feat(#1384): … (#1343 PR-B)` references **two** issues. `Closes #1384` satisfies one; the other (`#1343`) must also be verb-linked — bare prose ("half of #1343") fails. Fix: add `Part of #1343.` (or `Refs #1343.`). Don't reference a closed/parent issue in the title unless you'll verb-link it. (PR #1385.)

## Pre-submit check

Before opening or updating the PR, ask:
- would a reviewer know the intended invariants from this description?
- would they know why the design is shaped this way?
- would they know what was deferred?
- would they know what tests prove it?

If not, improve the description before pushing again.
