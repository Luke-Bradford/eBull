# pre-flight-review

Mandatory self-review skill to run before every push.

## Goal

Catch the review agent's likely objections **before** opening another review round.

This is not optional.
Run this against the actual diff before every push.

## Process

Open the current diff and read it top to bottom with reviewer posture:

```bash
git diff origin/main...HEAD
```

If the branch base is not `main`, compare against the correct base branch.

For each changed file, answer the checklist below.
Do not push until every item is either:
- satisfied
- fixed
- or explicitly deferred with a tech-debt issue

## Checklist by bug class

### A. First-row / empty-state correctness
For every changed function that reads or writes DB state:
- what happens if the table is empty?
- what happens if the query returns zero rows?
- what happens if an optional field is `NULL` / `None`?
- if this is the first row for a key, does versioning / insert logic still work?

### B. SQL correctness
Check for:
- two-step `MAX(...) + 1` versioning
- `fetchone()` without `ORDER BY ... LIMIT 1`
- positional row access like `row[0]`
- `json.dumps(...)` being written into `jsonb`
- external I/O inside DB transactions
- incorrect NULL comparisons
- unsafe SQL string formatting
- missing same-class scan after fixing one SQL bug

If any of these appear, stop and fix them.

### C. Python hygiene
Check for:
- missing `from __future__ import annotations`
- mutable parameter types where `Sequence[...]` is correct
- magic string action names instead of shared `Literal` aliases
- helper functions raising in ways that kill whole batch runs
- shared-resource evaluation loops missing mutable accumulators
- duplicated rationale/explanation string formatting
- misleading logs after dedup/filtering

### D. Test quality
For every changed public function or behaviour:
- is there a test for the happy path?
- is there a test for the empty/first-row case?
- is there a test for missing optional data?
- is there a test for the failure path?
- if DB write + return value both exist, is there a test proving they match?
- are mocks shaped like the real library?
- is time patched if `_utcnow()` is involved?

A test that only proves "no crash" is not good enough.

### E. Auditability
Check:
- does the change preserve enough structured data for later debugging?
- is there a stable rationale / explanation path?
- if something is versioned, is the version source deterministic?
- if something is scored or recommended, is the model/version/evidence traceable?

### F. Concurrency / idempotency
Ask:
- what happens if this code runs twice?
- will a retry duplicate rows?
- is the upsert key correct?
- is the write atomic where it needs to be?
- could two writers race and produce inconsistent versions?

### G. Interface cleanliness
Check:
- are providers still thin adapters?
- is domain logic leaking into providers?
- is DB access leaking into HTTP clients?
- are abstractions still simpler than the alternatives?

### H. Scope discipline
Ask:
- did the change solve the issue without sneaking in unrelated redesign?
- did I leave a problem half-fixed?
- did I create hidden tech debt without recording it?

### I. Settled decision check
Before coding, read `docs/settled-decisions.md` and list every decision that applies to this issue.

For each relevant decision:
- state what the rule is
- state how the planned implementation preserves it

If no decisions apply, say so explicitly.

If your plan changes a settled decision:
- stop
- surface the deviation explicitly before coding
- do not silently proceed with a different interpretation

Typical examples:
- kill switch semantics
- thesis freshness logic
- provider boundary rules
- scoring model style
- action-specific execution guard rules

### J. Prevention log check
Before coding, read `docs/review-prevention-log.md` and identify any entries relevant to this issue.

For each relevant entry:
- state the bug class
- state how the implementation avoids repeating it

If no entries are relevant, say so explicitly.

Do not skip this step because the issue seems unrelated.
The prevention log captures recurring mistakes — the bugs that look "obviously fine" until they are not.

### K. Frontend diff branch

If the diff touches `frontend/`, also read and apply these before pushing:

- `.claude/skills/frontend/async-data-loading.md` — `useAsync` composition, no combined loading gates, one source → one error surface
- `.claude/skills/frontend/loading-error-empty-states.md` — all three states required, layout symmetry, no exception text in DOM
- `.claude/skills/frontend/safety-state-ui.md` — kill-switch / halt / risk banners must survive refetch via cached snapshots
- `.claude/skills/frontend/api-shape-and-types.md` — `types.ts` mirrors Pydantic `response_model` in the same PR; `apiFetch` path contract; auth stays in the client
- `.claude/skills/frontend/operator-ui-conventions.md` — formatters, color semantics, density, status pill vocabulary

For frontend pages, also grep:

```bash
grep -nE '\.loading\s*\|\|' frontend/src/pages/*.tsx     # combined loading gates
grep -nE '\.error\b' frontend/src/pages/*.tsx            # duplicate error surfaces
```

Each match must be deliberate.

## Required same-class scan

After finding one query/rule/test hazard in a file, grep the entire file for the same hazard and confirm each occurrence is either correct or fixed before pushing.

Examples:
- if one `fetchone()` needed `ORDER BY`, inspect every `fetchone()` in the file
- if one `row[0]` was wrong, inspect every row access in the file
- if one helper needed graceful failure, inspect similar helpers nearby
- if one test used the wrong mock shape, inspect similar tests in the file

A partial fix is not enough — push only when every occurrence is accounted for.

## Pre-push output

Before pushing, be able to state:

- first-row case checked
- missing-data case checked
- SQL hazards checked
- tests added/updated
- audit trail preserved
- same-class scan completed
- any remaining gaps logged as tech debt
- settled decisions checked and preserved, or explicit deviation raised before coding
- prevention log checked; relevant entries identified or confirmed none apply
- if `frontend/` touched: frontend skills (K) read and applied

If you cannot honestly say that, do not push yet.
