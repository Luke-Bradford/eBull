# eBull project instructions

eBull is a long-horizon automated investment engine for one UK retail investor on eToro, run as a small systematic fund. It is not a day-trading tool.

The operator wants it hands-off and profitable. The agents are the specialists: decide the specifics, report outcomes plainly, and bring back only the decisions listed under "What the operator decides". How the fund's roles divide the work is in `.claude/skills/quant/desk.md`; start there for anything that could change what the engine holds.

The global `~/.claude/CLAUDE.md` also applies. Where the two conflict, this file wins for eBull and says so explicitly. Edits to the global file are the operator's, because it covers other projects. The previous version of this file, with the incident history behind many rules, is archived at `docs/archive/2026-10-04-claude-md-before-opus-5-5-rewrite.md`. Rules here stand on their reasons.

## Objective and risk posture

- **Objective.** Beat SPY total return, net of all costs, within the mandate's risk limits, on the engine's own capital. A strategy is also compared against a random-basket control from the same universe, so selection is separated from simply owning stocks (2026-10-01).
- **Research before new exposure.** No strategy opens positions, demo included, until it has passed a historical backtest on point-in-time, survivorship-free data net of our costs, meeting `quant/research-process.md` and the 2026-08-23 evidence bar (`docs/settled-decisions.md`). Exits and protective actions are never blocked. Execution plumbing is tested with its own small harness, never by running an unproven strategy.
- **Demo first, small live capital later.**
  - The operator decides whether live capital is funded and how much.
  - Within funded capital, the `approval_mode` mandate flag decides who approves promotion and allocation (#2843). Under `autonomous`, the engine acts on evidence alone.
  - The execution guard, kill switch and #2844 sandbox stay fail-closed under either mode. The sandbox bounds engine exposure by the assigned capital: fixed, or compounding with realised P&L. The operator calls this the only safety net they want.
  - Operator alerts are refusal surfaces under #2843's validity contract: one-sentence decision, complete evidence, recommendation, safe default. There are no routine check-ins.
- **No leverage until strategies are validated.** A x1 CFD short is unleveraged and allowed for research and paper only.
  - A short on eToro is a CFD: a contract with the broker, with no ownership.
  - Easy-to-borrow names cost the spread only. Hard-to-borrow names (> 10%/yr) accrue a daily fee at 21:00 GMT, tripled at weekends.
  - Availability, volatility halts and region restrict shorts.
  - Short losses are unbounded, so tails and delisting attribution decide a short strategy, not its mean.
- **Broker-side SL/TP on every non-core position** (operator rule). The core index sleeve uses catastrophe levels of −50% / +200%. Slow factor books use wide, volatility-based levels (`quant/portfolio-construction-and-risk.md`).
- **Execution is deterministic and auditable.** Research may be AI-heavy. Every order passes hard-rule gates and carries an entry ticket stating the factors or rule that selected it, its evidence and its exit; "beta" is not a selection reason. A failed check is never bypassed silently.
- **The engine manages only its own book.** The operator's manual holdings and copy-mirrors are not engine positions and no engine figure depends on them. Account-level reporting, NAV and reconciliation still include them.
- **Strategy families.** Event-form families (insider, 13D, merger, PEAD, shock-event) were cut on 2026-08-22. They are back in scope for demo research as factors (2026-10-01), with their recorded lessons as priors (`quant/strategy-menu.md`).
- **Keep the system simple and testable.**
  - Add a dependency only with a stated reason.
  - Keep provider interfaces clean and domain logic out of providers.
  - Keep migrations explicit and minimal.
  - Version model outputs.
  - Persist enough structured evidence to audit every decision.

## What the operator decides

Only these:
- funding or exposing live capital;
- irreversible data loss;
- reversing a settled decision (`docs/settled-decisions.md`);
- visual and product taste.

Bring each with the researched answer and a recommendation, never a menu.

Everything else is yours to decide: strategy design, data and factor choice, thresholds, schema shapes, sequencing, fix approaches and demo actions.
- **Choosing between two measurable designs:** measure both.
- **A new strategy or model version:** ship it on evidence, meaning skill invariants, a full-population A/B, a cross-source check and an intact rollback.
- **Assembling the design system or consolidating existing pages:** engineering, not a new taste approval.
- **A question that seems to "need an explicit call":** research it first. If you cannot state it in one sentence and name the evidence that would settle it, you have not researched it enough to raise it.

## How to work

1. **Read the issue, its latest comments and the code it touches.** The finished work must match the issue and the current settled decisions.
2. **Check `docs/settled-decisions.md` and `docs/review-prevention-log.md`** for the files and behaviour in scope. Apply what you find; cite an entry only where it shapes or blocks the plan. If the work would reverse a settled decision or repeat a prevention entry, stop and surface it before coding.
3. **Before writing any causal claim** ("X causes Y"), grep the settled decisions for Y and read the docstring of the function that produces Y. A surprising effect is often a decision you have not read.
4. **For data work, read the data-source skill before choosing a column, citing a rule or reporting a number:**
   - `data-sources/sec-edgar.md`;
   - `edgartools.md` and `etoro-api.md` where those sources apply;
   - `research-price-corpus.md` for any historical price source.

   These skills record which stored column holds the as-of date, structured-data mandate floors, unit traps and acceptance tests. Two examples: as-of `period_end` versus `filed_at` per form; 13F PRN bond-principal rows stay excluded from share rescaling. A regulation recited from memory is not a source rule. A spec that cites an SEC Item with no matching skill-section reference has skipped this step.
   - For any eToro capability, check the live portal (`llms.txt` plus the per-endpoint Markdown, via WebFetch). We hold intraday history: `get_intraday_candles` serves 1,000 bars per request, and FourHours bars reach about 8 months back, with volume and extended hours. An empty `price_intraday` is a build gap, not a data gap.
5. **Falsify the premise on the full population before designing.** That covers the issue's premise and any root cause a previous session handed over. A prior session's conclusion is evidence, not a finding. Two failed discriminators in a row mean the model is wrong, not the key.
6. **Fix every data-treatment decision by its source's documented rule**: how to resolve, aggregate, classify, de-duplicate, denominate, or pick a threshold, ratio or window. The source is the SEC Item or Rule, the form spec, a published formulation (for example, Bollinger's Squeeze and Bulge use a 126-session extreme) or a settled invariant.
   - Where none exists (level clustering has none), say so, fix the rule by construction, and freeze it in a version hash.
   - A spec touching ownership, filings or metrics carries a "Source rule" section that cites the governing rule. Where a signal's safety is in question, it also carries a "Full-population verification" note.
7. Read the relevant engineering and domain skills (§ Skills). Then build in this order: schema and interfaces, service logic, tests, integration.
8. Self-review against `engineering/pre-flight-review.md`, run the local gates, and write the PR description per `engineering/pr-authoring.md`. Then follow the PR workflow to a terminal state.

## Engineering rules

Each of these exists because its failure is silent:
- **Grep before cite.** Every file:line, function, table, env var and import is verified when written.
- **Test before claim.** "X works" means you ran X and read the output.
- **Quantifiers are measurements.** A sentence about source data saying "most", "every", "always" or "rarely" needs its query and both numbers, with the subject of each number clear. Otherwise the quantifier goes.
  - A parsed-date count is not a stub count.
  - A NULL can mean a parser miss.
  - A continuous series across an `(a)(3)` holding-company reorganisation is correct.
- **Never hard-code a derived statistic** into prose, comments or docstrings. Compute it, or write the command that reproduces it beside it.
- **Print values in full before reasoning about their size.** Truncated output has produced false defect reports.
- **A text classifier is a data-treatment decision.** Check for a structured field first and record that you did. For example, a Form 25 has no structured security-type field: its SGML header carries submission type, conformed name, SIC and file number.
- **Config changes are verified against the real `.env`, not a fixture.**
  - After changing `app/config.py`, instantiate `Settings()` against the real file before and after the change (`git stash` gives the before) and compare the security-relevant fields.
  - Print lengths and booleans, never values.
  - A blank variable is present and wins an alias race; the code uses `env_ignore_empty=True` for that reason.
- **A pipe hides the exit code and buffers output.** Do not pipe a gate command or a long-running job. Either let it print, gate on the producer's pipe status (`$pipestatus` in zsh, `${PIPESTATUS[0]}` in bash), or redirect to a file and read the file. Pytest here does not print a final pass count, so gate on its exit code.
- **Reuse before writing.** Grep `app/`, `scripts/` and sibling files for the same shape first.
  - **Before patching a parser for a standard SEC form again:** test edgartools on our failing case, and check for a structured or XBRL source. Adopt a tool only if it wins on our data, and when hand-rolling, cite what you compared against.
  - **A recurring per-case patch** means the model is wrong.
- **Keep it small.** Write the least code that solves the problem, and use plain prose that carries the facts.

## Corpus changes

This covers parsers, ETL, schema migrations and metric derivation. Before claiming a scoring change is safe, a full-population A/B is mandatory (`engineering/full-population-ab.md`).

A change touching ownership, insider, institutional, blockholder, treasury, DEF 14A, fundamentals or observations data is done only when the PR records, with commit SHAs:
1. **Smoke test:** run on the dev DB against a default panel of AAPL, GME, MSFT, JPM and HD, recording the operator-visible figure observed.
2. **Cross-source check:** one figure checked against a named independent source (EdgarTools golden file, SEC EDGAR, gurufocus or marketbeat), recording the source and the figure compared.
3. **Backfill run:** `POST /jobs/sec_rebuild/run` on the dev DB.
   - Scope it like `{"source": "sec_form4"}` or `{"instrument_id": N, "source": "…"}`; it resets scheduler and manifest rows to `pending`.
   - Wait until the scope's pending count reaches zero (`/jobs/sec_manifest_worker/status`; the worker shares a 10 req/s limit).
   - Non-SEC corpus changes use their own documented backfill job.
4. **Live re-check:** the figure re-checked on the live endpoint after the backfill, for example `/instruments/<symbol>/ownership-rollup` for ownership changes.

If any step fails after merge, open a follow-up ticket citing the merge SHA.

## Review: match the depth to the risk

This table is the single source for how much review a change gets. Climbing higher than the change warrants is a defect, not diligence.

| change | review, and nothing more |
|---|---|
| Narrow and mechanical (1–2 files, no data semantics) | Self-review, pre-push hook, review bot |
| Behavioural with data semantics (services, endpoints, scoring inputs) | The above, the domain skill, Codex checkpoint 2 before first push |
| Corpus change | The above, plus the full-population A/B and the evidence list above |
| Judgement artefact (spec, plan, causal claim, priority order, acceptance criteria, skill) | Codex checkpoint 1 on the framing |
| High-stakes cross-domain plan | Committee review (`committee-review` skill), capped at 2–3 lenses unless the operator asks for more |
| Rebuttal-only merge round | Codex checkpoint 3 on the rebutted claims only |

- **Ask every reviewer for everything it finds** and classify severity yourself.
- **Never run a second agent to re-check your own work.** Deterministic gates are not that.
- **Fix real findings before the next step.**
- **No Codex review is needed** for follow-up pushes that address bot comments, or for routine edits after the checkpoints have run.

**Codex checkpoints.**
- **Checkpoint 1** runs twice: on the spec before the operator signs off, and on the implementation plan before the first task is dispatched. Its prompt asks Codex to flag any treatment inferred from first principles where a documented source rule exists, and any safety claim resting on a sample.
- **Checkpoint 2** runs `codex exec --profile review review --base origin/main` (profile in `~/.codex/review.config.toml`). Run `git add -A` first, because untracked files are invisible to it.
- **Checkpoint 3:** Codex must independently agree that each rebuttal is sound. If Codex and the author agree the remaining bot findings are unfounded, record that on the PR. The merge gate still needs a bot APPROVE: push the evidence as a code comment or test so the next bot round can accept it. If Codex sides with the bot, or finds a new issue, fix it and restart the loop.
- **Invocation:** always `codex exec`, always with `< /dev/null`, output redirected to a file. Bare `codex review` needs a target. Codex usage limits are checked by invocation (`codex exec "Reply with exactly: AVAILABLE" < /dev/null`), never from a note.

## Branch, PR and merge

1. **Branch before touching code, and commit only on the branch:**
   - `feature/<issue>-…`
   - `fix/<issue>-…`
   - `docs/…` or `chore/…`, which need no issue.
2. **Push, open the PR, then poll** `gh pr view <n> --comments` and `gh pr checks <n>`. Read the bot's latest comment, which `github-actions` posts with the "Claude Code Review" marker. Do not push again, even to fix CI alone, until that review has posted, CI results are visible and every comment is read.
3. **Reply to every finding with what changed**, ending in exactly one of:
   - `FIXED <sha>`;
   - `DEFERRED #<issue>`;
   - `REBUTTED <concrete reason>`.

   PREVENTION items end as `EXTRACTED <file>`, `ALREADY_COVERED <file>` or `REBUTTED <reason>`. Details are in `engineering/review-resolution.md`. On a follow-up round the bot checks each earlier finding against the full diff.
4. **Before each follow-up push**, re-run the gates and check the branch scope with `git diff --stat origin/main...HEAD`. This catches a silent revert of merged work.
5. **Merge** with `bash $AUTONOMY_ENGINE_HOME/bin/safe_merge.sh <n>`, run from the repo root. The script lives in the separate `Luke-Bradford/autonomy-engine` repo (`engine/bin/safe_merge.sh`), not in eBull; `AUTONOMY_ENGINE_HOME` is `~/Dev/autonomy-engine/engine`, and the eBull side is configured in `.autonomy/config.yaml`.
   - **For a code PR**, the gate needs the latest marker-bearing bot comment to be posted after the head commit, with an APPROVE verdict and no `[BLOCKING]` section, plus green CI.
   - **For a PR whose every file is doc-only**, it needs no blocking bot comment and green CI. The bot posts a skip notice there, and that is a terminal state.
   - **After an engine merge**, fast-forward `~/Dev/autonomy-engine` with `git fetch && git merge --ff-only origin/main`. Nothing else updates it.
6. **After merge:**
   - delete the local and remote branch;
   - close only the issues the PR resolves (`Closes`/`Fixes`, never `Refs` or umbrella parents);
   - re-detach `~/Dev/eBull` at `origin/main`;
   - if `app/` changed, request a jobs reload with `touch ~/Dev/eBull/app/__init__.py`. The reloader waits for a running job to finish, sometimes for hours, so confirm the deploy by the jobs child PID changing. An unchanged PID while a job is live means wait.
7. **Re-check live state with `gh`/`git` before any close-out action**: an issue close, a merge, a re-detach or a final comment. A sibling may already have done it.

**A ticket is finished only when** it is merged with CI green on the merged commit and the branch deleted, or when a handoff comment on the PR gives:
- the exact remaining commands;
- the acceptance queries with their expected values;
- every prerequisite (a migration to apply, a job that must be running).

If time runs out mid-task, spend it on the handoff rather than another poll.

## Local gates

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright
uv run pytest -m "not db"          # fast tier
uv run pytest -n 0 tests/smoke     # app boots against the dev DB (pyproject defaults to 4 workers)
```

**The pre-push hook** (`.githooks/pre-push`; wire it once per clone with `git config core.hooksPath .githooks`) runs these, the full DB tier (`-m db`) and the chokepoint lints.
- It serialises the DB tier and smoke runs across worktrees with `lockf`.
- A manual run does not take that lock.
- CI does not run pytest, so the hook is the test gate. Use `--no-verify` only in a genuine emergency, and record why.

**DB tests.**
- The `db` marker is applied automatically to tests that use a real-DB fixture, and it is module-scoped: one DB test in a file moves the whole file into the DB tier. Exit code 5 means nothing was collected.
- To run DB tests for touched modules manually: start the test database (`docker compose --profile test up -d postgres-test`), then run `uv run pytest -m db tests/test_<touched>.py`. Do not run it while another push may be testing.

**The smoke test** exercises the real FastAPI lifespan, bootstrap and migrations against the dev DB. If it fails, fix the root cause; never skip it.

**Frontend changes** also run `pnpm --dir frontend typecheck` and `pnpm --dir frontend test:unit`.
- `test:unit` excludes `SetupPage.test.tsx`. CI runs the full suite; use `pnpm --dir frontend test` to debug integration tests.
- Read the `frontend/*` skills. `design-system.md` and `information-architecture.md` hold the standing layout and navigation decisions.

**Test style.** Prefer pure-function tests. Add one DB integration test per genuinely new SQL mechanism, and verify operator-visible behaviour on the running dev stack.

## Running work

- **Worktrees.** Long-running or experimental work goes in a `git worktree`: full-population runs, backfills, anything a sibling session might race. The dev stack serves `~/Dev/eBull`, so dev-verification happens there and the checkout is re-detached afterwards.
- **Background jobs.** Run long jobs with the tool's background mode. A `nohup … &` inside a tool call can be killed with the call.
- **Parallel sessions.**
  - Stake a ticket with a one-line issue comment before coding. A ticket with a fresh stake from another session belongs to it, so pick something else.
  - Before starting, check `git worktree list` and `git branch --list` for the ticket number; `gh pr list` is not enough.
  - Re-read an issue's latest comments immediately before posting any long-lived comment (evidence, verdict, close-out).
- **Subagents gather; you conclude.** Delegate measurement and search freely: running queries, finding callers, listing files, fetching endpoints. Keep causal claims, severity, priority and fixes in the main thread, where the settled decisions are; this follows the global rule. When a delegated investigation must end in a judgement, pass the relevant settled decisions and skill sections into its prompt. Do not delegate what you can finish in a few tool calls.
- **A wait is never a status.** If something blocks on a scheduled job, name the dispatcher and check it, then work on the next queue item meanwhile.
- **A job that reports success can still have done nothing.** Measure its output, not its status.

## Skills

The skills under `.claude/skills/` carry the domain knowledge. Read the ones the review rung calls for.

**Engineering:**
- `pre-flight-review`;
- `pr-authoring`;
- `review-resolution`;
- `python-hygiene`;
- `sql-correctness`;
- `test-quality`;
- `pre-push-checklist`;
- `full-population-ab` (mandatory before claiming a parser, ETL or scoring change is safe);
- `bash-script-hygiene`, for any `scripts/*.sh` (shellcheck at `-S warning`; `set -e` does not propagate inside `$(…)`);
- `pre-pr-fresh-agent-review`, an optional lens library for large or unfamiliar filings, schema, identity or observations diffs.

**Quant:**
- start at `quant/desk.md`;
- read `quant/strategy-menu.md` and `quant/research-process.md` before proposing or defending any strategy, and check turnover first, since one stored column screens faster than any backtest;
- `quant/strategy-evidence.md` is the evidence archive.

**Data:**
- `data-engineer`, for schema and write-through invariants;
- `metrics-analyst`, for source-to-chart lineage of every operator-visible metric;
- `data-sources/*`.

**Frontend:** `frontend/*` (see Local gates).

**Maintaining the skills.**
- Fix a skill that is wrong or missing something in the same PR that exposed it.
- New skills live at `.claude/skills/<area>/<name>.md` with a `## When to use` heading. YAML `name`/`description` frontmatter is only for skills discoverable via the Skill tool.
- `.claude/**` is write-refused inside loop worktrees. A loop session posts the exact intended edit (file and anchor) on #2403, and the next supervised session applies it.
- Every new prevention lesson goes into the relevant skill and into `docs/review-prevention-log.md` in the same PR.

**Audits and incidental findings.**
- After each epic, or every ~10 merged PRs, run one audit pass: is anything to contest, any process inadequate, any spec intent orphaned? File a ticket for each finding at once.
- File an incidental defect immediately when it is high-risk, cheap to write up or would be lost with the context. Otherwise record its evidence and file it by the end of the task. Do not let it widen the current ticket.
