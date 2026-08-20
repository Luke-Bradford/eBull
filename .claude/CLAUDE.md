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
- **Shorting: PERMITTED for research and paper trading** (operator decision, 2026-08-09,
  reversing "long only in v1" / "no shorting"). ⚠ Gated on the same bar as everything
  else: *"we need to trust the strategies though"* — validated in backtest, then paper,
  before any cash. ⚠⚠ A short on eToro is a **CFD**, not stock: contract with the broker,
  no ownership. Easy-to-borrow costs spread only; **hard-to-borrow (>10% annual) accrues a
  daily fee at 21:00 GMT, tripled at weekends** — and a name that just fell 12% is the
  archetypal hard-to-borrow candidate, so the cost model cannot reuse the long one.
  Shorting is also restricted by share availability, volatility halts, and region.
  ⚠⚠ Short losses are unbounded, so a mean return is not a sufficient basis for a short
  strategy — the tail and the delisting attribution decide (see #2437).
- **No leverage — still barred, and deliberately sequenced after validation** (operator,
  2026-08-09: *"I'd lean to leverage once we have validated that we have a high level of
  success first"*). ⚠ Note a CFD short at x1 is unleveraged exposure and does not breach
  this; anything above x1 does.
- No silent bypass of failed checks

## Engineering discipline (non-negotiable)

- **Grep before cite.** Every file:line, function name, table name, env var, import path — verified at write time, not from memory.
- **Source-rule before design.** Every ownership / filings / metric data-treatment decision (how to resolve / aggregate / classify / de-dup / denominate) is fixed by the SOURCE'S OWN documented rule — the SEC reg (cite the Item / Rule #), EDGAR / form docs + the filing's actual structure, edgartools, or our settled invariants (data-engineer I-series, prevention-log). FIND + CITE that rule before speccing. Do NOT reason it out from first principles (that shipped the wrong #1627 def14a-additive model; SEC Item 403 gave the right one in #1659), do NOT guess, do NOT hand the operator a heuristic menu. Verify the signal on the FULL population, never a sample (#1659: a 22-cluster sample said "FP≈0", the 210-cluster scan falsified it). When a quirk class keeps needing per-case patches (#1644/#1645/#1652/#1659), question the MODEL, not the case. Surface to the operator ONLY: visual taste / scope-sequencing / irreversible loss / a settled-decision reversal — and then with the RESEARCHED answer + recommendation, not a menu. Every ownership/filings/metric spec MUST carry a **"Source rule"** section citing the governing reg/Item or EDGAR doc, and (where a signal's safety is in question) a **"Full-population verification"** note. ⚠ **This binds QUANT/TA formulations too, not just SEC data.** The trigger is "am I about to pick a threshold, ratio or window", and it does not care whether a regulator is involved. Precedent (2026-08-05, #2279): a spec defined the volatility regime as a 20th/80th-percentile BandWidth cut. Invented. Bollinger's published rule is BandWidth at its lowest/highest in **six months (126 trading days)** — the Squeeze and the Bulge (*Bollinger on Bollinger Bands*, ch. 21). Caught by Codex at checkpoint 1, not by any gate. Where a published formulation genuinely does NOT exist (level clustering has none), say so explicitly and fix the rule **by construction**, freezing the constants in a version hash — do not invent a citation and do not leave the choice implicit.
- **The data-source skills are the FIRST stop, not a backstop — read them BEFORE citing any reg or measuring any figure.** Any work touching ownership / filings / fundamentals / metrics data MUST pass through `.claude/skills/data-sources/sec-edgar.md` (plus `edgartools.md` / `etoro-api.md` where that source applies) before a rule is cited, a column is chosen, or a number is reported. **Reciting an SEC reg from memory is NOT source-rule compliance.** The skill carries what the reg does not: which stored column actually holds the as-of date, the structured-data mandate floors that bound our coverage, and the unit traps. Precedent (2026-08-03, #2231): a spec measured split exposure on `filed_at` and cited ASC 260 / Rule 13f-1 from memory. The skill's per-form as-of rules gave `period_end` — which moved the blast radius +37% (28,356 → 38,955) and turned DEF 14A from "0 rows affected" into 1,469, an entire category wrongly declared safe. The same pass surfaced the 13D/G structured-XML mandate floor (2024-12-18, which explains `min(filed_at)` on `ownership_blockholders_current` to the day) and the 13F PRN bond-principal trap (§7.2 — rescaling a PRN row is meaningless; already excluded at ingest, must stay excluded). **Test: if a spec, ticket comment or PR cites an SEC reg/Item with no matching skill-section reference, this step has not been done.** A second opinion does not substitute — Codex reasoned its way to "`filed_at` is probably wrong for 13F"; only the skill + corpus gave the column, the 39-day median gap and the mandate date.
- **Test before claim.** Run the command, read the output. "X works" requires running X first.
- **The full-population rule binds DESCRIPTIVE claims, not just gates.** "Verify the signal on the FULL population" was being read as covering decisions — a discriminator, a threshold, a filter — and not the sentences written around them. Both halves are load-bearing, because a characterising claim becomes the next session's premise just as surely as a gate does. Precedent (2026-08-05, #2282 2c): I read ONE Form 25's stub EX-99 exhibit and wrote "most exchange filings attach a stub" — a population-of-one claim, which is the defect regardless of whether it happens to be true. ⚠ **This example was itself written ambiguously and nearly caused a wrong "correction" the next session (2026-08-05, #2279).** It cited "395 of 1,282" directly after the quoted claim, which reads as the stub rate refuting "most". It is not: `select count(*), count(suspension_date) from sec_form25_register` returns **1,282 / 395**, and 395 is the count from which a suspension date was PARSED — i.e. the NON-stub side. The complement is 887 (69.2%), so the original sentence is probably right and was nearly "fixed". Note even 887 is an upper bound on stubs (a NULL suspension date can also mean a real exhibit the regex missed), which is exactly why the honest form is to state the query and its two numbers rather than a percentage with an implied subject. **A lesson entry is a claim too: cite the query, not a bare figure whose subject the reader has to infer.** Same session, same shape: I inspected one series and wrote "every one of the 15 hits is a successor entity or a later occupant" — the split is 12 spanning / 4 starting after, and several of the spanning ones are `(a)(3)` holdco reorganisations where a continuous series is CORRECT. **Test: if a sentence contains "most", "usually", "every", "always" or "rarely" about source data, it is a measurement. Run it or delete the quantifier.**
- **Never hardcode a derived statistic into prose, a comment or a docstring — compute it, or omit it.** A number written by hand goes stale silently the moment the derivation changes, and it goes stale in the place a reader trusts most. Precedent (2026-08-05, #2282 2c): adding a `fund` security class moved a resolution-bias skew from +10 points to +6.6 and left FOUR hand-written copies wrong at once — a migration comment, a skill table, a test docstring and the script's own output. The fix that holds is structural: `--census` now computes the statistic (including its z) at run time, so a re-harvest cannot leave it lying. Where a figure genuinely must be written down (a migration header, a skill), state the command that reproduces it next to it.
- **A classifier over source text IS a data-treatment decision** — so "source-rule before design" fires BEFORE you write the regex, not after review asks. The trigger is easy to miss because it does not look like resolve/aggregate/de-dup: you are about to map free text to a closed vocabulary. Check for a structured field first and record that you checked. Precedent (2026-08-05, #2282 2c): a security-class classifier over `<descriptionClassSecurity>` was written before confirming no structured security-type field exists in a Form 25 (it does not — the SGML header carries only submission type, conformed name, SIC and file number). The answer happened to be "no structured source", but the check ran late and only after a prompt.
- **A CONFIG change is verified against the REAL config file, never a fixture.** The dev-verify rule already says "exercise the real endpoint"; config has the same shape and was not covered, because a settings change looks like a pure-code change. It is not — its behaviour is a function of a file that only exists on the operator's machine. Precedent (2026-08-05, #2286): adding an `alias_generator` to `Settings` was correct, tested, and moved `service_token` from a 64-character credential to `""` — because the working `.env` had a blank `EBULL_SERVICE_TOKEN=` (copied from `.env.example`, never filled) sitting above a bare `SERVICE_TOKEN=<value>`, and a blank var is PRESENT so it won the alias race. **No fixture has that duplicate; only the real file does.** Caught by running `Settings()` against the real `.env` and printing lengths, not values. The fix was `env_ignore_empty=True`. Concretely: after any change to `app/config.py`, instantiate `Settings()` against the real `.env` and diff the security-relevant fields before and after (`git stash` the change to get the "before"). ⚠ Print **lengths and booleans, never values**.
- **A pipeline hides the exit code of the command you care about.** `cmd | tail -2` returns `tail`'s status, which is always 0, so `cmd | tail && next` runs `next` even when `cmd` failed. Already in the archive gotchas as `cmd | head`; re-committed on 2026-08-05 (#2279) with `uv run pyright 2>&1 | tail -2`, which hid a real type error until the pre-push hook caught it a step later. **Never pipe a gate command.** Either let it print in full, or gate on `${PIPESTATUS[0]}`, or redirect to a file and grep the file. This applies to every `ruff`/`pyright`/`pytest` invocation chained with `&&`. ⚠ **The same pipe also BUFFERS, which blinds a long-running job** — re-committed 2026-08-05 (phase 2a): a 7-minute full-corpus sweep was launched as `… | tail -22`, so its flushed progress lines went nowhere and the output file sat at 0 bytes while the run was healthy. Seven minutes were spent diagnosing a stall that did not exist. **Never pipe a background measurement either** — write it whole and read the tail of the FILE.
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
2. Read `docs/settled-decisions.md` and `docs/review-prevention-log.md` for anything touching the files or behaviour in scope. **Apply what you find; cite an entry only where it actually shapes or blocks the plan.** Do not narrate a survey — "no settled decision applies here" as a standalone sentence is the ritual form of this step and is worth nothing. What the step is FOR is catching the case where a decision contradicts what you were about to do.
   ⚠ Reading "for anything relevant" has a known blind spot: you cannot judge relevance for a decision you do not know exists. That is why the separate rule below ("Never assert a CAUSE without checking whether the effect is a settled decision") greps for the EFFECT by name — it is the backstop for exactly this, and it is not optional.
3. If implementation pressure suggests changing a settled decision or risks repeating a prevention entry, stop and surface it before coding.
3a. **Read the data-source skill BEFORE the research step, whenever the ticket touches ownership / filings / fundamentals / metrics data.** `.claude/skills/data-sources/sec-edgar.md` first, plus `edgartools.md` / `etoro-api.md` where that source applies. This is step 3a and not part of step 4 deliberately: step 4's skills inform how you WRITE the code, whereas this one decides WHICH COLUMN you measure and therefore what every number in steps 3b onward means. Reading it afterwards means re-running the measurements. See the "data-source skills are the FIRST stop" rule above for the #2231 precedent where skipping this reported a whole ownership category as unaffected.

3b. **Research the source rule + falsify the premise FIRST — before writing the spec.** For ownership/filings/metric work: look up the governing SEC reg/Item or EDGAR/form rule (per "Source-rule before design"), and verify the issue's premise + any inferred signal against the SOURCE and the FULL population (query the dev DB / read the filing structure), not a sample. The handoff premise is falsified more often than not (member_of_group #1645, dimensional dei #1646/#1623, market-cap-as-float #1662, def14a-additive #1659) — and a wrong premise sinks the spec. Do this BEFORE writing code, not after a second-opinion pass (#1659 burned a spec + impl on a heuristic the full-population scan then killed).

3c. **An INHERITED root-cause is a premise too — re-falsify it before building on it.** Step 3b was being read as "falsify the ISSUE's premise", which leaves a hole: a fix that a PRIOR SESSION root-caused, wrote up on-issue, and handed off as "ready to implement" arrives already carrying an author's confidence, so it gets implemented instead of tested. **A prior session's conclusion is evidence, not a finding.** Re-run its discriminator against the full population before writing any code — it is one query and it is the cheapest step in the ticket. Precedent (2026-08-03, #2182 B1): a handoff labelled "ROOT CAUSE CONFIRMED" proposed gating period-row creation on "≥1 duration fact"; the full-population check showed that gate suppressing **9,236 legitimate prior-year comparative balance sheets** (Reg S-X 3-01(a) requires two years of balance sheet, all instant facts) to catch **1,584** real shells. It was the SECOND falsified discriminator on that ticket after `months_covered IS NULL` (38.9% precision). Two failed keys in a row is the signal to question the MODEL, not to try a third key.
4. Read the relevant engineering skills before writing code.
5. Make schema/interface changes first.
6. Implement service logic.
7. Write or update tests.
8. Self-review the diff using the pre-flight review skill.
9. Run local checks.
10. Write a complete PR description.
11. Follow the branch and PR workflow below — push, poll, wait, resolve, repeat until the review is SATISFIED on the most recent commit with CI green. "Satisfied" means an APPROVE, **or** the bot's doc-only skip notice ("Doc-only diff — engineering review skipped to save tokens"), which is a terminal state, not a pending one — a doc-only PR can never receive an APPROVE, so reading this gate literally makes such PRs permanently unmergeable.

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

## Answer the question yourself when you hold the facts — "wants an operator call" is not a status

The gating rule already exists twice — "Surface to the operator ONLY: visual taste /
scope-sequencing / irreversible loss / a settled-decision reversal", and
"operator-gated = **EVIDENCE**-gated, not person-gated". What keeps failing is not the
rule, it is the moment it is applied: a question gets labelled "needs an explicit call"
**at the point of noticing it**, before anyone checks whether the facts to answer it are
already available. The label then travels — into a ticket comment, a handoff note, a
session summary — and every later reader inherits "blocked on the operator" as if it were
a measured state. It is not. It is an unstarted research step wearing a status.

**Before writing that a decision needs the operator, do the research that would answer it.**
Then one of two things is true: you now have the answer and there is nothing to escalate,
or you have a specific, evidenced conflict that genuinely sits in one of the four gated
categories — and you present it with the researched recommendation, never as a menu.

Concretely, these are NOT operator calls, whatever they feel like:

- picking a threshold, window or parameter → **source-rule before design** governs it; find
  the published formulation, or fix it **by construction** and freeze it in a version hash.
- minting a new `model_version` / strategy id on evidence → evidence-gated. Ship it on
  skill-invariants + full-population A/B + cross-source + rollback intact.
- choosing an execution order, a scope, a schema shape, a fix approach → yours.
- "which of these two designs" where both are measurable → **measure them**.

Only these are person-gated: live-trade or capital exposure, irreversible data loss,
reversing a settled decision, and visual/product taste.

⚠ **Two phrasings that are always a tell**: *"that wants an explicit call before X starts"*
and *"doesn't need a decision tonight"*. The first outsources a question you have not yet
tried to answer; the second reframes your own inaction as a schedule. Precedent
(2026-08-07, supervisor session): stage 5e-4's `max_hold_bars` was carried across
**three** consecutive session summaries as "a strategy-identity decision, so it wants an
explicit call", and the operator had to ask "what is the question?" before anyone looked
for one. Nothing about it was person-gated — the strategy has 0 ledger rows, so a new
version costs nothing, and the purge/embargo window it feeds has a published formulation.
The cost of the label was three sessions of a real blocker sitting unexamined.

**Test: if you cannot state the question in one sentence AND name the evidence that would
settle it, you have not researched it enough to escalate it.**

## Standing retrospective checkpoint (#2075)

After each epic close, or every ~10 merged PRs, run one "inheriting-team
audit" pass: anything to contest? process inadequate anywhere? spec
intent orphaned in prose? Findings become tickets IMMEDIATELY (template:
the 2026-07-17 batch #2066-#2075). Do not fold findings into unrelated
PRs or leave them in session notes — a finding without a ticket is lost.

**The same rule binds INCIDENTAL findings, not just scheduled audits — but
capture them without derailing the current task.** A defect noticed while
doing something else (a query run in passing, a number that looked wrong
on a panel, a job whose `success` did not match its output) must not be
left only in a PR description of an unrelated change, or in a memory file.

File it immediately when it is high-risk, cheap to write up, or would be
lost once the context is gone. Otherwise record the evidence in a short
handoff note and file it at the end of the task. **Do not turn a narrow
ticket into an audit** — scope expands readily under agentic work, and "I found
something while I was in there" is the most common way a one-file fix
becomes a session. Precedent (2026-08-03): five minutes of
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
   frontend inventory, instruction-file audit). **But do not delegate work you can
   finish yourself in a handful of tool calls, and never spawn an agent to verify or
   double-check your own work** — delegation is cheap to reach for and self-verification
   already happens, so both patterns cost tokens and time without improving
   the result. If one agent can do it, use one, not several.
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

## Review-intensity ladder — pick the rung, then stop

**This is the single source for "how much review does this change need". Every other
file defers to it.** The old set applied near-maximum scrutiny to everything, which
looks safe and is not: it buries the signal from the checks that matter, and
self-verification already happens, so a blanket pass costs tokens without improving the result.

Match the rung to what could actually go wrong. Climbing higher than the change warrants
is a defect, not diligence.

| the change is… | what it gets |
| --- | --- |
| **A narrow diff** — one or two files, mechanical, no data semantics | self-review + the deterministic pre-push hook + the review bot. **Nothing else.** No second-opinion pass, no lens library, no extra agent. |
| **A behavioural change with data semantics** — service logic, endpoints, scoring inputs | the above + the relevant domain skill read for the surface touched |
| **A corpus change** — parser, ETL, schema migration, metric derivation | the above + full-population A/B (`full-population-ab.md`) + Definition-of-Done clauses 8-12 evidence table. **This rung is non-negotiable; it is evidence, not reassurance.** |
| **A judgement artefact** — spec, plan, causal claim, priority order, acceptance criteria | a second opinion on the FRAMING (this is where Codex has actually paid — see below) |
| **A high-stakes cross-domain plan** | committee review, capped at 2-3 lenses unless the operator explicitly asks for the full panel |
| **A rebuttal-only merge round** | a second opinion on the REBUTTED CLAIMS ONLY — never a whole-diff re-review |

Two rules that cut across every rung:

- **Never ask a reviewer to be conservative.** No "only real bugs", no "high-severity
  only", no "skip nits". A good reviewer takes it literally and reports less. Ask for everything;
  classify severity yourself afterwards.
- **Never run a second agent to check your own work.** If you can review it yourself in a
  handful of tool calls, do that. Deterministic gates (hook, CI, A/B harness) are not
  "another pass" — they produce evidence you do not already have.

## Branch and PR workflow

**How much review this PR needs is decided by the review-intensity ladder above, not
by this section.** What follows is the mechanical sequence, which is the same at every
rung; the ladder decides what feeds into it.

1. Create a branch before touching code.
   - `feature/{issue-number}-short-description`
   - `fix/{issue-number}-short-description`
   - `docs/{issue-number}-short-description` or `docs/{short-description}` — instruction
     set, skills, prevention log, specs. Maintenance of the operating documents often has
     no ticket, and inventing one to satisfy a naming rule is worse than the rule.
   - `chore/{short-description}` — tooling, CI, dependency bumps.
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
7. Merge only after review is satisfied on the most recent commit and CI is green
   ("satisfied" = APPROVE, or the doc-only skip notice — see Working order step 11).

### Composition with the global `~/.claude/CLAUDE.md`

That file states the same workflow for every project the operator works on. Where the two
disagree, **this file wins for eBull work** — it knows the repo's CI gates and the review
bot's actual behaviour. Two specific reconciliations (raised 2026-08-03):

- **"Close all linked issues" on merge** — do NOT apply literally. This repo's PR template
  and the `pr-issue-link` CI gate deliberately distinguish `Closes/Fixes/Resolves` (issue
  closes) from `Refs/Part of/Umbrella` (issue stays open). Closing a referenced umbrella
  or parent issue because a child PR merged is a regression, not compliance. Close what
  the PR resolves; leave the rest.
- **Branch naming** — the global file lists only `feature/` and `fix/`. This file adds
  `docs/` and `chore/`, because instruction-set and tooling maintenance frequently has no
  ticket and minting one to satisfy a naming rule is worse than the rule.

Both are worth fixing in the global file too, but that is the operator's to edit — it
affects all their projects, not just this one.

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

> ✅ **RESOLVED 2026-08-03 (operator delegated the call). Checkpoint 2 is now
> LADDER-SCOPED, not universal.** The contradiction was: the ladder says a narrow diff
> gets "self-review + pre-push hook + review bot, nothing else", while checkpoint 2
> mandated `codex exec review` on *every* branch before first push.
>
> The resolution is the RE-TARGET that was already on the table, made concrete:
>
> - **Checkpoints 1 and 3 are unconditional.** Both are judgement-artefact shaped, and
>   that is where Codex demonstrably pays. Checkpoint 1 on the #2231 spec caught an
>   INVERTED RATIO DIRECTION — the spec said multiply where the arithmetic requires
>   divide, which would have moved every stale ownership row the wrong way by ratio² —
>   plus `filed_at`-vs-`period_end` as-of semantics. Neither is a thing a diff reviewer
>   would see.
> - **Checkpoint 2 fires only at the ladder's "behavioural change with data semantics"
>   rung and above** — service logic, endpoints, scoring inputs, parsers, ETL, schema
>   migrations, metric derivation. A narrow mechanical diff (one or two files, no data
>   semantics) gets self-review + hook + bot, per the ladder, and nothing else.
>
> Rationale: the ladder is the single source for review intensity, and a blanket
> pre-push Codex pass is precisely the "run a second agent to check your own work"
> pattern the working-style rules forbid. Scoping it by rung keeps Codex where it earns
> and removes it where the review bot already sits. The prior evidence ("3-for-3 on
> semantics-carrying diffs, nothing on test-only") points the same way — those wins were
> all at or above the behavioural rung.

**Where it actually pays (2026-08-03 evidence).** Checkpoint 2 is DIFF-shaped, and on
that diff Codex found nothing while the review bot found a real WARNING. The same day, a Codex pass over an *assessment* — findings, causal
claims, ticket framing, execution order — caught a false causal claim (#2213), a
missed ordering dependency, an under-prioritised bug that turned out to be the
session's largest defect (#2217), and two tickets scoped around a symptom rather
than their shared root cause (#2218). One session is not a trend, but the shape is
worth acting on: **Codex earns most on JUDGEMENT artefacts (specs, plans, priority
orders, causal claims) and least on line-level diffs, where the review bot already
sits.** When you have an assessment or a plan, hand Codex the numbers and the
reasoning and ask it to attack the framing — not just "review this diff".

1. **Before writing code** — two Codex passes:
   - **After spec is written, before user final-approves:** `codex exec "Review this spec for <feature>. Path: docs/specs/<area>/<topic>.md (live spec) OR docs/proposals/<area>/<topic>.md (unshipped). Report EVERY plausible correctness gap, invariant violation and missing edge case — do not pre-filter by severity or confidence, I will classify afterwards. For any ownership/filings/metric data-treatment decision: FLAG it if the spec infers the treatment from first principles where a documented source rule exists (SEC reg/Item, EDGAR, form spec) and is not cited; FLAG any signal whose safety rests on a sample rather than a full-population check. Reply terse."` Fix issues before presenting spec to user for sign-off.
   - **After implementation plan is written, before first task dispatch:** same invocation against the plan doc. Catches plan-shape bugs (bad task decomposition, missing dependency, wrong contract) before any subagent starts coding.
2. **Before first push — ONLY at the ladder's "behavioural change with data semantics" rung or above** (service logic, endpoints, scoring inputs, parsers, ETL, schema migrations, metric derivation). After self-review + local gates pass, run `codex exec review --base origin/main` on the branch and fix anything real before pushing. **A narrow mechanical diff does NOT get this** — one or two files, no data semantics, gets self-review + pre-push hook + review bot and nothing else, per the ladder. Resolved 2026-08-03; see the note above.
3. **Before merging on a rebuttal-only round** — if the latest review's findings are all rebuttals (no code changes pending), run Codex to confirm the rebuttals are sound. Without this step, rebuttals are unverified and may hide real bugs the review bot *did* catch in disguise.

When Codex is NOT required:
- Follow-up pushes that fix review comments (the review bot will re-check).
- Routine edits after Codex already reviewed the plan + first diff and there is no rebuttal-only round pending.

Invocation rule: always use `codex exec` (non-interactive). Never bare `codex` with no subcommand (requires interactive terminal).
`codex exec review` needs a target — `codex exec review --base origin/main` for a branch diff. Bare `review` errors with
"Specify --uncommitted, --base, --commit, or provide custom review instructions".

⚠ **`git add -A` BEFORE `codex exec review --base origin/main`.** The diff it reads does not
include untracked files, so a NEW file — which on a schema change is exactly the migration —
is invisible to it. On #2262 this cost a full round: Codex returned a confident P1 ("the query
joins `instrument_price_supply` but the patch adds no migration creating it, so
`daily_candle_refresh` will fail with `UndefinedTable`"), which was correct about the diff it
was given and wrong about the branch. Re-run with the file staged: no findings. The failure is
silent in the direction that matters — a review that cannot see your new file reports on a
codebase that does not exist, and everything it says will sound plausible.

## Review decision tree — who to consult in what order

```
Self-review (diff + engineering skills)
  ↓
Is the diff at the "behavioural change with data semantics" rung or above?
  ├─ No (narrow, mechanical) → skip to push
  └─ Yes → Codex review (checkpoint 2: before first push)
  ↓
Push + wait for Claude review bot + CI
  ↓
Bot findings? → Triage each: FIXED / DEFERRED / REBUTTED
  ↓
Any rebuttals on latest review?
  ├─ No  → all fixed → merge when green + review SATISFIED on latest commit
  │                      (APPROVE, or the doc-only skip notice — see step 11)
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
~3.5 min, down from 66 min.** ✅ **RESOLVED 2026-08-03: the db tier goes
back on the push gate WHEN #2224 is fixed, and not before.** The blocker
is not cost, it is determinism — #2224 is ~2 intermittent failures per
full run, different tests each time, and a gate that fails randomly
trains you to `--no-verify`, which is strictly worse than no gate. The
trigger is objective, so no further decision is needed: **when #2224 is
closed and the full tier has run clean 3 consecutive times, add `-m db`
to `.githooks/pre-push` and to the pre-push checklist above.** Until
then the gate is fast tier + smoke, unchanged.

For a broad-surface diff there is no longer any excuse to skip the whole
tier manually. Run it in file-scoped batches, never bare `-m db` (which
has wedged this box twice):

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

Read and apply these before pushing — **scoped by the review-intensity ladder above**,
not all of them on every diff. A narrow change needs the pre-push checklist and
nothing more:

- `.claude/skills/engineering/pre-flight-review.md`
- `.claude/skills/engineering/pre-pr-fresh-agent-review.md` — a LENS LIBRARY (financial-plumbing / data-engineer / data-scientist / adversarial) for filings ETL, schema, identity-resolution and observations diffs. **No longer a mandatory pre-push gate** (revised 2026-08-03): a blanket second-agent pass before every push is the "use a subagent to verify" pattern that causes over-verification, and it contradicted this file's own "delegate gathering, never concluding" rule. Reach for the lenses when a diff on those surfaces is genuinely large or unfamiliar.
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

### Quant / strategy skills (read before proposing or defending any strategy)

- `.claude/skills/quant/strategy-evidence.md` — **MANDATORY before proposing, speccing or defending any trading strategy.** The replication literature has already tested most of what we would think of and says most of it fails: Hou/Xue/Zhang (65-82% of 452 anomalies fail under value weighting), Harvey/Liu/Zhu (t > 3.0; 9 of 313 survive), Novy-Marx/Velikov (**turnover above ~50%/month rarely survives costs** — check this FIRST, it is one stored column and disqualifies faster than any backtest), Frazzini/Israel/Moskowitz (short-term reversal is the most cost-constrained family), Cederburg et al. (vol scaling works on momentum, not elsewhere), Cohen/Malloy/Pomorski (**routine insider trades have zero predictive power; opportunistic ones pay ~82 bps/month**). ⚠ Four of those independently predicted, ex ante, the ranking our own backtest produced — s2 inside the turnover bar and the only one beating buy-and-hold, s1 12× over it, s3 6.7× over. Carries the family-viability table and the pre-spec checklist.

### Data foundation skills (read before SEC ingest / schema / parser / metric work)

- `.claude/skills/data-sources/sec-edgar.md` — source-of-truth: endpoints, formats, identifiers, gotchas (DD-MMM-YYYY dates, 13F PRN/SH, VALUE-cutover 2023-01-03, 13D/G XML mandate, etc.), rate-limit discipline, reference impls.
- `.claude/skills/data-sources/etoro-api.md` — MANDATORY before citing any eToro API capability: live-portal verification protocol (llms.txt + per-endpoint .md, WebFetch not curl), known spec drift. Never claim "not supported by the public API" from memory. ⚠⚠ **We HAVE intraday history — `get_intraday_candles`, 1000 bars/request, `FourHours` reaches ~8 months back, volume populated, extended hours included.** "We only have daily candles" is FALSE and has been corrected four times; it is the single most repeated factual error on this codebase. `price_intraday` being empty is a BUILD gap, not a data-availability gap. See the skill's "WE HAVE INTRADAY HISTORY" section for the measured per-interval reach table.
- `.claude/skills/data-sources/edgartools.md` — library reference: coverage matrix, API cheat-sheet, Pydantic validation cliff (#932), version pinning, decision tree for use-vs-roll-our-own.
- `.claude/skills/data-sources/research-price-corpus.md` — MANDATORY before evaluating or arguing about any historical price source for backtesting. The measured landscape of both kinds of source: live FEEDS (#2284: ten of them, every free one **0/382** on the delisted cohort) and frozen ARCHIVES (#2346: three free GitHub archives serving **258/259**, because a scrape taken while a name was live keeps it after the delisting). Plus the fingerprint technique that proves a "free archive" is a Yahoo copy, the rate-limit-vs-corpus-size arithmetic that disqualifies most APIs in one step, the `Q`-suffix bankruptcy symbol rule, and the cohort acceptance test (`tests/fixtures/form25_2023_cohort.csv`) to run before adopting anything. ⚠ **The one-line answer changed on 2026-08-07** — do not cite the old "no free source has it" conclusion. **Distinct from `market-data/SKILL.md`, which owns the eToro execution venue.**
- `.claude/skills/data-engineer/SKILL.md` — what we own: schema invariants, two-layer ownership model, write-through pattern, settled-decisions cross-reference, "where does X come from?" FAQ, admin-page operator UX FAQ. Discoverable as the `data-engineer` skill.
- `.claude/skills/metrics-analyst/SKILL.md` — every operator-visible metric: source → transform → table → endpoint → chart, with caveats and validation steps. Discoverable as the `metrics-analyst` skill.

## Settled decisions

→ Covered in the Working order above (steps 2 and 3), and by the rule "Never assert a CAUSE without checking whether the effect is a settled decision".

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

## Final reminder — read this last

**These rules are STATED in the global `~/.claude/CLAUDE.md` ("Working with Claude
"Working style"), which is their single source.** They are echoed here — as a list of
names, not a restatement — because this file is long and a short reminder near the end of
a long prompt is worth more than one buried at the top.
If a rule below ever disagrees with the global file, the global file is right and this
list is stale.

- **Be concise** — replies and written documents both.
- **Do the narrow task** — at the scope asked, without quietly widening it.
- **Don't add verification the model already does** — no re-checking your own answer, no
  second agent to confirm your own work.
- **Ask a reviewer for everything, filter afterwards** — never "only real bugs".

The eBull-specific half is the **review-intensity ladder** above: it decides which of
this repo's gates a given change actually needs. The deterministic ones it selects —
pre-push hook, full-population A/B on corpus changes, the review-comment resolution
contract — are evidence, not reassurance, and none of the above weakens them.
