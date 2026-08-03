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

## Engineering discipline (non-negotiable)

- **Grep before cite.** Every file:line, function name, table name, env var, import path — verified at write time, not from memory.
- **Source-rule before design.** Every ownership / filings / metric data-treatment decision (how to resolve / aggregate / classify / de-dup / denominate) is fixed by the SOURCE'S OWN documented rule — the SEC reg (cite the Item / Rule #), EDGAR / form docs + the filing's actual structure, edgartools, or our settled invariants (data-engineer I-series, prevention-log). FIND + CITE that rule before speccing. Do NOT reason it out from first principles (that shipped the wrong #1627 def14a-additive model; SEC Item 403 gave the right one in #1659), do NOT guess, do NOT hand the operator a heuristic menu. Verify the signal on the FULL population, never a sample (#1659: a 22-cluster sample said "FP≈0", the 210-cluster scan falsified it). When a quirk class keeps needing per-case patches (#1644/#1645/#1652/#1659), question the MODEL, not the case. Surface to the operator ONLY: visual taste / scope-sequencing / irreversible loss / a settled-decision reversal — and then with the RESEARCHED answer + recommendation, not a menu. Every ownership/filings/metric spec MUST carry a **"Source rule"** section citing the governing reg/Item or EDGAR doc, and (where a signal's safety is in question) a **"Full-population verification"** note.
- **Test before claim.** Run the command, read the output. "X works" requires running X first.
- **Reuse > reinvent.** Before writing a new helper / script / test pattern, grep `scripts/`, `app/`, sibling files for the same shape. If close-enough exists, use it.
- **Standard-filing reuse check (before hand-rolling or patching a parser for the Nth time).** SEC forms are STANDARDIZED — a parsing edge case is almost always already solved (or provably hard) in the wild. Before writing/patching a parser for a standard filing: (1) check whether edgartools already extracts it AND **test that empirically on OUR actual failing case** — the edgartools-skill quality notes ("quality variable" etc.) are starting points, not verdicts (#2097: edgartools' own SCT extractor *also* leaked the title into the name — flattening the newline was its right core move, comma-splitting was its bug; our role-keyword split beat it); (2) check for a **structured / XBRL source** that sidesteps HTML scraping (e.g. Item 402(v) Pay-vs-Performance dimensional NEO-name tags). Adopt the tool only if it wins on our data; if hand-rolling, cite the tooling/technique you compared against. **A recurring per-case patch (#2088/#2094/#2097 SCT name-splitting) is the trigger to run this check** — question the model, not the case.
- **KISS.** Smallest code that solves the problem. No "what if" abstractions. No multi-paragraph design docs for 5-LOC changes.
- **Minimum prose.** Plans / PR descriptions / commit messages / replies: facts not narrative. Reviewer doesn't need a chapter.

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
4b. **Research the source rule + falsify the premise FIRST — before writing the spec.** For ownership/filings/metric work: look up the governing SEC reg/Item or EDGAR/form rule (per "Source-rule before design"), and verify the issue's premise + any inferred signal against the SOURCE and the FULL population (query the dev DB / read the filing structure), not a sample. The handoff premise is falsified more often than not (member_of_group #1645, dimensional dei #1646/#1623, market-cap-as-float #1662, def14a-additive #1659) — and a wrong premise sinks the spec. Do this BEFORE step 6, not after Codex ckpt-1 (#1659 burned a spec + impl on a heuristic the full-population scan then killed).

4c. **An INHERITED root-cause is a premise too — re-falsify it before building on it.** Step 4b was being read as "falsify the ISSUE's premise", which leaves a hole: a fix that a PRIOR SESSION root-caused, wrote up on-issue, and handed off as "ready to implement" arrives already carrying an author's confidence, so it gets implemented instead of tested. **A prior session's conclusion is evidence, not a finding.** Re-run its discriminator against the full population before writing any code — it is one query and it is the cheapest step in the ticket. Precedent (2026-08-03, #2182 B1): a handoff labelled "ROOT CAUSE CONFIRMED" proposed gating period-row creation on "≥1 duration fact"; the full-population check showed that gate suppressing **9,236 legitimate prior-year comparative balance sheets** (Reg S-X 3-01(a) requires two years of balance sheet, all instant facts) to catch **1,584** real shells. It was the SECOND falsified discriminator on that ticket after `months_covered IS NULL` (38.9% precision). Two failed keys in a row is the signal to question the MODEL, not to try a third key.
5. Read the relevant engineering skills before writing code.
6. Make schema/interface changes first.
7. Implement service logic.
8. Write or update tests.
9. Self-review the diff using the pre-flight review skill.
10. Run local checks.
11. Write a complete PR description.
12. Follow the branch and PR workflow below — push, poll, wait, resolve, repeat until APPROVE on the most recent commit with CI green.

## Terminal step of a ticket — never leave state to reconstruct

`/insights` (2026-07-25) found most fires ended with "a PR still awaiting CI or a
batch half-drained, leaving you to reconstruct state next session". That
reconstruction is pure re-paid cost.

A ticket is finished only when one of these is true:

1. merged, with CI green on the merged commit and the branch deleted; or
2. **a handoff comment is posted on the PR** giving the exact remaining
   commands, the acceptance queries with their expected values, and any
   prerequisite (a migration that must be applied first, a job that must be
   running). Written so the next session runs it without re-deriving anything.

"PR opened" and "backfill started" are not terminal states. If credits or time
run out mid-drain, spend the last of them on the handoff note, not on one more
poll — the note is what makes the remaining work cheap.

## Long-running work belongs in a worktree

Anything long-running or experimental — full-population A/B runs, corpus
backfills, flaky-test discrimination, anything a sibling session might race —
goes in `git worktree add`, not the shared `~/Dev/eBull` checkout. Concurrent
edits otherwise contaminate the run, and a subagent or sibling can clobber
uncommitted work (`/insights` 2026-07-25; the 07-16 race).

Exception: work that must be exercised against the running dev stack, which
serves `~/Dev/eBull` only. Branch in the main checkout for the dev-verify step,
and re-detach at `origin/main` afterwards.

## Standing retrospective checkpoint (#2075)

After each epic close, or every ~10 merged PRs, run one "inheriting-team
audit" pass: anything to contest? process inadequate anywhere? spec
intent orphaned in prose? Findings become tickets IMMEDIATELY (template:
the 2026-07-17 batch #2066-#2075). Do not fold findings into unrelated
PRs or leave them in session notes — a finding without a ticket is lost.

**The same rule binds INCIDENTAL findings, not just scheduled audits.** A
defect noticed while doing something else — a query run in passing, a
number that looked wrong on a panel, a job whose `success` did not match
its output — becomes a ticket in that session, before the current task
resumes. It does not go in the PR description of an unrelated change and
it does not go in a memory file. Precedent (2026-08-03): five minutes of
ad-hoc dev-DB queries during an unrelated assessment surfaced three
unticketed defects — #2213 (OpenFIGI resolution dark for 7 weeks behind
`success`-reporting sweeps), #2214 (ETF filer typing starved, 1 of
11,465) and #2215 — none of which any scheduled pass would have found,
because nothing was failing. **The health signals this app reports are
self-consistent — a job that no-ops and reports success is invisible to
every automated check we have.** Manual spot-measurement is currently the
only detector for that class, so its output must be captured.

## Subagents: delegate GATHERING, never CONCLUDING

Correctness in this repo depends on context that does not travel: 74 settled
decisions, 283 prevention-log entries, and per-metric caveats spread across the
data-foundation skills. A subagent starts without them and cannot know what it has
not read.

So the split is not by difficulty, it is by **what the output is**:

- **Safe to delegate — measurement and location.** "Run this query and return the
  distribution." "Find every caller of X." "List the files matching this shape."
  "Fetch these 20 endpoints and tabulate one field." The result is checkable
  against the source, and wrong output is obvious.
- **Do NOT delegate — causal claims, severity, priority, or a fix.** "Why is Y
  happening", "is this a bug", "which of these matters most", "propose the fix".
  A subagent will produce a fluent, plausible answer built on the fraction of the
  decision-history it happened to read, and you will not be able to tell.

The evidence is not hypothetical, and it is not about subagents being weak — the
MAIN agent made exactly this error on 2026-08-03. I had correct measurements
(`coverage.state = unknown_universe` on all five golden-panel instruments) and drew
a confident causal conclusion (#2213 causes it) that was false, because I had read
`metrics-analyst` but not `settled-decisions.md` or the producing function's
docstring. Same failure mode a subagent has by construction, in an agent that at
least *could* have read the context.

Practical consequences:

1. Fan out read-only measurement freely — it is the cheapest parallelism available
   and this session had four independent ones (fundamentals gaps, ownership state,
   frontend inventory, instruction-file audit).
2. Bring the numbers back and do the reasoning **in the main thread**, where the
   settled decisions are.
3. If you must delegate an investigation that ends in a judgement, pass the
   relevant settled decisions and skill sections INTO the prompt. An agent cannot
   check a rule it was never given.
4. Prefer a Codex pass over the assembled reasoning to a subagent producing the
   reasoning — Codex attacks a framing you supply, which is the failure mode you
   actually have.

## Parallel-session coordination (#2075; 07-16 race lesson)

Multiple sessions may hold overlapping handoffs. Non-negotiable:

1. **Stake ownership on-issue at session start** for any ticket you will
   work: one-line issue comment BEFORE coding. A ticket with a fresh
   stake comment belongs to that session — pick something else.
2. **Re-read the issue's latest comments IMMEDIATELY before posting**
   any long-lived comment (evidence, verdict, close-out) — not just at
   session start. The 07-16 duplicate landed 62s after the sibling's
   despite a stake existing for hours.
3. Before ANY close-out side effect (issue comment/close, merge,
   re-detach), re-check live state via gh/git — a sibling may have done
   it already.

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

## Never assert a CAUSE without checking whether the effect is a settled decision

Working-order step 2 ("read `docs/settled-decisions.md`") was being applied only
to tickets being worked, not to causal claims made while investigating. Those
are where it matters most, because a cause-claim written into a ticket becomes
the next session's acceptance criterion.

Before writing "X causes Y" about any operator-visible symptom:

1. `grep` `docs/settled-decisions.md` for Y;
2. read the **docstring of the function that produces Y** — in this repo the
   disposal rationale is usually written there, at length, by whoever settled it.

Precedent (2026-08-03, #2213): I filed "the OpenFIGI stall is the direct cause of
`coverage.state = unknown_universe` on the ownership card". It is not.
`_read_universe_estimates` returns hard-coded `None` for every category, and its
docstring records committee verdict #790 (disposed 2026-06-17): *"`unknown_universe`
IS the truthful state — faking a denominator is the lie."* The state is
unconditional and correct. Left standing, that claim would have handed the next
session an acceptance signal that can never move — and pointed them at reopening a
closed decision. Caught by a Codex pass on the assessment, not by any test.

**A symptom you find surprising is as likely to be a decision you have not read as
a bug.** Check which, before you write it down.

## Codex second-opinion — mandatory checkpoints

Codex runs at exactly three points in the workflow. Non-negotiable.

**Where it actually pays (2026-08-03 evidence).** The three checkpoints below are
all DIFF-shaped, and on that diff Codex found nothing while the review bot found a
real WARNING. The same day, a Codex pass over an *assessment* — findings, causal
claims, ticket framing, execution order — caught a false causal claim (#2213), a
missed ordering dependency, an under-prioritised bug that turned out to be the
session's largest defect (#2217), and two tickets scoped around a symptom rather
than their shared root cause (#2218). One session is not a trend, but the shape is
worth acting on: **Codex earns most on JUDGEMENT artefacts (specs, plans, priority
orders, causal claims) and least on line-level diffs, where the review bot already
sits.** When you have an assessment or a plan, hand Codex the numbers and the
reasoning and ask it to attack the framing — not just "review this diff".

1. **Before writing code** — two Codex passes:
   - **After spec is written, before user final-approves:** `codex exec "Review this spec for <feature>. Path: docs/specs/<area>/<topic>.md (live spec) OR docs/proposals/<area>/<topic>.md (unshipped). Focus on correctness gaps, invariant violations, missing edge cases. For any ownership/filings/metric data-treatment decision: FLAG it if the spec infers the treatment from first principles where a documented source rule exists (SEC reg/Item, EDGAR, form spec) and is not cited; FLAG any signal whose safety rests on a sample rather than a full-population check. Reply terse."` Fix issues before presenting spec to user for sign-off.
   - **After implementation plan is written, before first task dispatch:** same invocation against the plan doc. Catches plan-shape bugs (bad task decomposition, missing dependency, wrong contract) before any subagent starts coding.
2. **Before first push** — after self-review + local gates pass, run `codex exec review` on the branch. Fix anything real before pushing.
3. **Before merging on a rebuttal-only round** — if the latest review's findings are all rebuttals (no code changes pending), run Codex to confirm the rebuttals are sound. Without this step, rebuttals are unverified and may hide real bugs the review bot *did* catch in disguise.

When Codex is NOT required:
- Follow-up pushes that fix review comments (the review bot will re-check).
- Routine edits after Codex already reviewed the plan + first diff and there is no rebuttal-only round pending.

Invocation rule: always use `codex exec` (non-interactive). Never bare `codex` with no subcommand (requires interactive terminal).
`codex exec review` needs a target — `codex exec review --base origin/main` for a branch diff. Bare `review` errors with
"Specify --uncommitted, --base, --commit, or provide custom review instructions".

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
uv run pytest -m "not db"        # fast tier: pure-logic, no Postgres (~60-90s under load)
uv run pytest tests/smoke        # app boots against the dev DB
```

Long-running corpus jobs (rewash, full-population A/B) must be launched with the tool's own
background mode. A `nohup … &` started inside an ordinary tool call is killed when that call's
process group is cleaned up — a 45-minute backfill silently dies part-way and the version
distribution is the only tell.

A repo pre-push hook at `.githooks/pre-push` enforces all of these plus
the chokepoint-lint scripts automatically. Wire once per clone:

```bash
git config core.hooksPath .githooks
```

### Test tiering (operator decision 2026-06-07)

The push gate runs ONLY the **fast tier** (`-m "not db"`) + the smoke
test. The `db` marker is auto-applied at collection
(`tests/conftest.py::pytest_collection_modifyitems`) to any test that
pulls a real-DB fixture or whose module touches `psycopg.connect` /
the test-DB URL / `run_migrations` / `TestClient`.

The **DB-backed integration tier** (`-m db`, ~44% of the suite) is OFF
the per-push path — it was the hour-plus, xdist-flaky, routinely
`--no-verify`'d cost that paid no rent on every push. Run it
deliberately when you change DB/SQL/ingest/schema code:

```bash
docker compose --profile test up -d postgres-test   # once per session
uv run pytest -m db tests/test_<touched>.py ...      # touched modules + neighbours
```

⚠ **The cost premise changed on 2026-08-03 (#1568): the full tier is now
~3.5 min, down from 66 min.** Whether that puts it back on the push gate
is an OPERATOR call (it changes every push), not an agent one — until
they decide, the gate is unchanged. But for a broad-surface diff there
is no longer any excuse to skip the whole tier. Run it in file-scoped
batches, never bare `-m db` (which has wedged this box twice):

```bash
find tests -name 'test_*.py' | sort | split -l 40 - /tmp/chunk_
for f in /tmp/chunk_*; do uv run pytest -m db -q $(tr '\n' ' ' < "$f"); done
```

Gate on the EXIT CODE — this repo's pytest config suppresses the final
`N passed` line, so the durations block is the last thing printed.

CI does NOT run pytest (removed 2026-05-05). `--no-verify` is for
genuine emergencies only (precedent: #1387).

**Writing tests going forward — lean.** Default to pure-logic tests
(no DB): extract the decision into a pure function and table-test it
(see prevention-log entry on "prefer pure policy over real DBs"). Add
DB-backed tests sparingly — ONE integration test per genuinely-new SQL
mechanism, not a file per code path. Lean on the dev-verify step
(exercise the real endpoint/job on dev) for operator-visible behaviour
rather than a thick integration suite. The smoke test
(`tests/smoke/test_app_boots.py`) drives the FastAPI lifespan against
the real dev DB and catches lifespan-only failures (bad SQL in
`master_key.bootstrap`, broken imports under `app/main.py`, migration
state mismatches) that mocked-cursor unit tests silently miss — if it
fails, the running server is broken; fix the root cause, do not skip it.

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
- `.claude/skills/engineering/full-population-ab.md` — MANDATORY before claiming any parser / ETL / scoring change is safe. Distinct-entity metric (never row count), inspect the gain side, parse-vs-STORED as a separate arm, never simulate the control. #2140 ran 8 rounds; #2158 ran 3.
- `.claude/skills/engineering/pre-push-checklist.md` — the canonical pre-push gate list (SQL/Python/test checks + review-comment handling); mirrors `.githooks/pre-push`.
- `.claude/skills/engineering/bash-script-hygiene.md` — read when editing any `scripts/*.sh`, especially the chokepoint-lint guards (shellcheck `-S warning` floor, `set -e` in `$(…)`).

### Frontend skills (read on any ticket touching `frontend/`)

- `.claude/skills/frontend/async-data-loading.md`
- `.claude/skills/frontend/loading-error-empty-states.md`
- `.claude/skills/frontend/safety-state-ui.md`
- `.claude/skills/frontend/api-shape-and-types.md`
- `.claude/skills/frontend/operator-ui-conventions.md`
- `.claude/skills/frontend/design-system.md` — standing surface/card/badge/chart/density decisions (visual v2). Assembling the system is engineering, NOT a taste-gate.
- `.claude/skills/frontend/information-architecture.md` — standing nav/page-consolidation decisions (lens hub + view presets). Consolidating existing pages is engineering, NOT a taste-gate.

### Data foundation skills (read before SEC ingest / schema / parser / metric work)

- `.claude/skills/data-sources/sec-edgar.md` — source-of-truth: endpoints, formats, identifiers, gotchas (DD-MMM-YYYY dates, 13F PRN/SH, VALUE-cutover 2023-01-03, 13D/G XML mandate, etc.), rate-limit discipline, reference impls.
- `.claude/skills/data-sources/etoro-api.md` — MANDATORY before citing any eToro API capability: live-portal verification protocol (llms.txt + per-endpoint .md, WebFetch not curl), known spec drift. Never claim "not supported by the public API" from memory.
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

## Skill ownership (project-local skills)

Files under `.claude/skills/**` are **project-local engineering substrate**. The agent OWNS them:

1. When a gap is observed mid-task (empirical finding contradicts the skill; new pattern emerges; recurring trap surfaces), update the skill **inline** in the same session, in the same PR. Do NOT defer with "I'll update the skill later" — that's how skills go stale.
2. When a skill is found to make a claim the codebase no longer honours, correct it as a routine maintenance edit. No separate approval needed.
3. When a new prevention-log lesson surfaces mid-task, extract it into the relevant skill AND `docs/review-prevention-log.md` in the SAME PR — never let the lesson live only in the PR description.
4. New skills land at `.claude/skills/<area>/<name>.md` with a single-line `## When to use` heading + the actionable content. No frontmatter unless the skill is discoverable via the Skill tool (in which case YAML frontmatter with `name:` + `description:` is required).
5. Skill edits do NOT need a separate ticket. Bundle them with whatever PR exposed the gap.

The `permissions.allow` block in `.claude/settings.local.json` already grants `Edit` / `Write` on the whole tree — no per-skill approval prompt should appear. If one does, the friction is in the upstream skill-tool layer, not in project permissions; the answer is to update the skill FILE via Edit/Write rather than invoking the Skill tool.

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
