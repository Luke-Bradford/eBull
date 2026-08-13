# review-resolution

Mandatory skill for handling PR review comments.

## Goal

End every review comment in an explicit terminal state.
No silent ignores.
No vague acknowledgements.
No "I'll leave that for later" without a ticket.

## Read the whole review first (finding-count check)

Before triaging a round (PR #2090 lesson, 2026-07-17):

1. Fetch the FULL review comment body — `gh pr view N --comments` read
   untruncated, never through a fixed `grep -A N` window (a long review
   truncates silently and the tail findings drop).
2. Count the findings (`### [` headings) and make the resolution set
   match that count exactly before posting it.
3. A REQUEST CHANGES verdict with any unread tail = stop and re-fetch.

## Allowed terminal states

Every review comment must end in exactly one of these forms:

- `FIXED {commit_sha}`
- `DEFERRED #{issue_number}`
- `REBUTTED {reason}`

There is no fourth state.

## Meaning of each state

### FIXED

Use when the issue was addressed in code on the same PR.

Reply format:

```text
FIXED {commit_sha} — {what_changed}
```

### DEFERRED

Use only when the point is real but out of scope or intentionally postponed.

Requirements:

- open a tech-debt issue first
- use the issue number in the reply
- explain why it is safe to defer

Reply format:

```text
DEFERRED #123 — {why_safe_to_defer}
```

### REBUTTED

Use when the review point is not actually correct.

Requirements:

- be specific
- cite the actual code path / invariant / schema / test
- do not hand-wave

Reply format:

```text
REBUTTED — {reason}
```

## Severity handling

### BLOCKING

- must be FIXED or REBUTTED before merge
- do not defer blocking issues unless the user explicitly agrees

### WARNING

- fix on the PR if reasonable
- otherwise open tech debt and DEFER explicitly

### NITPICK

- fix it if trivial
- if truly out of scope, DEFER explicitly
- do not ignore because it is "just a nit"

### PREVENTION

- handle the immediate point
- then extract the rule into:
  - a skill file under `.claude/skills/`
  - `docs/review-prevention-log.md`
  - or the pre-flight review checklist (`.claude/skills/engineering/pre-flight-review.md`)

## Required workflow after review lands

1. Read all comments before touching code.
2. Group them by file / bug class.
3. Fix same-class problems, not just the single commented line.
4. Re-run local checks.
5. Push the fix commit.
6. Reply to every comment with one terminal state.
7. **Wait for the re-review to post on the new commit** — poll `gh pr view {pr_number} --comments` and `gh pr checks {pr_number}` until both the review workflow and CI complete. Do not proceed until the re-review result is visible.
   - Review workflow `conclusion: FAILURE` with **no** comment posted = infra failure (check the Actions run log — e.g. `stop_reason=max_tokens`, see `.github/workflows/claude-review.yml`), NOT a review verdict. Fix/re-run the workflow; do not treat it as approval or as findings.
   - Doc-only diffs (only `*.md`/`*.mdx`/`*.rst`/`docs/**`/LICENSE/CHANGELOG/NOTICE changed) get a "review skipped" notice instead of findings — that is the expected terminal state, not a hang.
8. If the re-review requests further changes, repeat from step 1.
9. If a prevention lesson emerged, update the prevention log or a skill before merge.

## Bad behaviour to avoid

Do not:

- ignore a comment because it feels pedantic
- assume a warning can stay unresolved without a ticket
- fix something silently and leave no reply
- reply "done" without saying what changed
- rebut a comment without concrete reasoning
- push another commit before reading the review

## PREVENTION comment resolution

Every PREVENTION comment must end in exactly one of these states:

- `EXTRACTED {file}` — lesson added to a skill, workflow doc, checklist, or `docs/review-prevention-log.md`
- `ALREADY_COVERED {file}` — rule already exists in that file; cite the exact file path
- `REBUTTED {reason}` — prevention note does not apply; explain specifically why

### Rules

PREVENTION comments cannot be silently acknowledged.
"Noted" or "good point" is not a terminal state.

Reusable engineering lessons (language, SQL, test patterns that recur across repos) go into skill files under `.claude/skills/engineering/`.

Recurring repo-specific mistakes (bug classes that keep appearing in eBull PRs specifically) go into `docs/review-prevention-log.md`.

The exact file must be named in the resolution reply.

### Reply format

```text
EXTRACTED docs/review-prevention-log.md — added entry "JOIN fan-out inflates aggregate totals"
ALREADY_COVERED .claude/skills/engineering/python-hygiene.md — "Production invariants" section
REBUTTED — this applies to ML pipelines; eBull uses deterministic scoring with no batch normalisation
```

## Definition of review complete

A review is only complete when:

- every comment has a terminal state (FIXED / DEFERRED / REBUTTED)
- every PREVENTION comment has a terminal state (EXTRACTED / ALREADY_COVERED / REBUTTED)
- all fixes are on the latest commit
- all deferrals have issue numbers
- all rebuttals are specific
- prevention notes have been extracted where relevant

## A REBUTTAL can be right for the wrong reason — and the wrong reason outlives it

The resolution contract asks whether the rebuttal's *conclusion* is sound. That is
not sufficient, because a rebuttal is written into a PR comment and usually into a
code comment, where its **reasoning** becomes the next session's premise. A correct
verdict resting on a false claim about the code is worse than a wrong verdict: the
verdict gets re-derived, the claim gets trusted.

So checkpoint 3 has two questions, not one:

1. Is the conclusion right?
2. **Is every factual claim in the rebuttal actually true of the code?** Open the
   file and check each one. Do not rebut from a mental model of what the module
   does.

Precedent (2026-08-05, #2218). I rebutted "this bucket could mask a stall" with:
*"it is reachable ONLY on a 200 with per-item no-match, because the resolver raises
on every transport, HTTP and parse failure."* The conclusion held — the real
justification is a 60,011-vs-3,027 distribution that makes the alternative fire on
nearly every run — but the reasoning was false in three ways: `_parse_entry` also
returns `None` for a per-item `{"error": ...}` and for schema drift, `zip(...,
strict=True)` raises an unwrapped `ValueError`, and `_pick_us_primary` can raise
`AttributeError`. Codex found all three at checkpoint 3. Left standing, the code
comment would have told the next reader that a bucket containing swallowed API
errors contains none — and that bucket writes a permanent tombstone (#2304).

**When the reasoning is corrected but the conclusion survives, say both plainly** in
the resolution reply: what was wrong, why the decision does not change, and where
the real defect is now tracked. A silent swap of justification reads as agreement.

⚠ Corollary: if the rebuttal's real reason turns out to be "the alternative would
fire too often", that is a **measurement**, not an argument. State the query and its
numbers, not the intuition.
