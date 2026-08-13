You are working on the eBull **ownership / filings correctness tail**, headless
and unattended. One task per run. You have the full repo, the local dev
Postgres, and all project skills.

⚠ A SECOND loop is running concurrently in a different worktree on the TA epic
(#2240). Stay off its surface entirely: do not take any #2240 issue, do not
touch `/Users/lukebradford/Dev/.ebull-autonomy`, and do not edit
`app/services/indicator_*`, `app/services/strategy_*`, `app/services/signal_*`
or `app/services/price_quarantine_store.py`. If your task genuinely needs one
of those files, stop and say so on the issue.

## Read first — do not skip

1. `.claude/CLAUDE.md` — engineering discipline. Non-negotiable and hard-won.
2. `.claude/skills/data-sources/sec-edgar.md` — **before** you cite a reg,
   choose a column, or report a figure. Reciting an SEC rule from memory is not
   source-rule compliance; the skill carries the per-form as-of columns, the
   structured-data mandate floors and the unit traps that the reg does not.
3. `.claude/skills/data-engineer/SKILL.md` — schema invariants, the two-layer
   ownership model, the write-through pattern.
4. `docs/settled-decisions.md` — grep it for the EFFECT you are about to
   explain, not just the ticket you are working. A surprising symptom is as
   often a decision nobody re-read as it is a bug.
5. `docs/review-prevention-log.md` — skim for anything touching what you change.

## Pick ONE task

Check `gh issue view <n> --comments` for a recent ownership stake before
starting — a ticket someone else staked today belongs to them. Then take the
first item below that is not done:

1. **#2304** — a per-item OpenFIGI `{"error": ...}` is tombstoned as
   `openfigi_unknown`, a terminal verdict. `_entry_to_mapping` in
   `app/services/openfigi_resolver.py` returns bare `None` for no-match, for
   per-item error and for unrecognised shapes alike, and
   `app/services/cusip_resolver.py` writes the tombstone from it. The ticket
   asks for a discriminated return **and** for the counters that make the
   existing 60,011-row bucket measurable — that measurement cannot be taken
   retrospectively, so it is the first deliverable, not the last.
2. **#2213** — OpenFIGI CUSIP resolution dark since 2026-06-18. ⚠ Re-falsify
   the premise before building: the 44,429-row backlog it cites is a
   DIFFERENT job's queue (`source IS NULL`), which a prior session established
   on 2026-08-05. Measure first, then decide what the ticket actually is.
3. **#2214** — ETF filer-type classification starved, the `etfs` ownership
   slice empty. ⚠ Established 2026-08-05: this is a SEEDING ticket (6 active
   filer seeds), not a classifier bug.
4. **#2234** — FINRA short-interest history never backfilled.
5. The **def14a Item 403 tail** — #2169, #2175, #2176 (parser correctness),
   #2171 (a seq-scan on every rewash). ⚠ These have been patched per-case four
   times (#2088/#2094/#2097 and again in #2169's lineage). A recurring per-case
   patch is the trigger to question the MODEL, not to write a fifth patch —
   and to run the standard-filing reuse check (does edgartools already do this,
   tested on OUR failing case? is there a structured/XBRL source?).
6. **#2226 / #2230 / #2231 / #2232** — the ownership arithmetic cluster
   (public float zero, double-attributed Section 16 + 13D/G shares, no split
   adjustment, implausible denominators). Bigger; take one, not the cluster.

⚠ **ONE task. Do it completely.** A half-finished second task is worse than an
idle iteration.

## You HAVE a database — use it. You do NOT have the dev stack.

- **Verify on the FULL population, never a sample.** This repo has repeatedly
  been bitten by a favourable sample: a 22-cluster ownership sample said
  "false positives ≈ 0" and the 210-cluster scan falsified it.
- ⚠ **Never state a number you did not compute in this run.** Do not copy a
  figure out of an issue body and present it as measured — several are stale,
  and at least three handoff premises in this area have been falsified.
- ⚠ **A descriptive claim is a measurement too.** If a sentence you write
  contains "most", "usually", "every", "always" or "rarely" about source data,
  run the query or delete the quantifier. State the query and its numbers, not
  a percentage whose subject the reader has to infer.
- Migrations: apply with `PYTHONPATH=. uv run python scripts/migrate.py`.
- ⚠ You are in a WORKTREE. The dev API (`:8000`) and vite (`:5173`) serve
  `~/Dev/eBull`, NOT here. Database access works fine; **live-endpoint
  verification does not.** See the handoff rule below — do not fake it, and do
  not skip the ticket because of it.

## Definition of done for THIS surface

Any change to filings ETL, parsers, ingest or ownership/observations schema
carries clauses 8-12 of `.claude/CLAUDE.md`. You can complete 8, 9 and 12
yourself; 10 and 11 need the operator.

- **Clause 8 — smoke-test 3-5 known instruments** in the dev DB. Default panel:
  `AAPL`, `GME`, `MSFT`, `JPM`, `HD`. Record the operator-visible figure.
- **Clause 9 — cross-source verify at least one fixture** against an
  independent reputable source (gurufocus, marketbeat, SEC EDGAR direct).
  Record the source and the compared figure.
- **Clauses 10 + 11 — backfill and the live figure are the OPERATOR's.** Finish
  by posting an `## OPERATOR VERIFY` block on the issue containing the exact
  commands to run, in order, with the expected result of each: the
  `POST /jobs/sec_rebuild/run` body with its scope, the endpoint to hit
  afterwards, and the acceptance query with the number it should return.
  Written so it can be run without re-deriving anything.
- **Clause 12** — the PR description records each of the above with its SHA.

## Workflow

- `git fetch origin` then `git checkout -b fix/<issue>-<short> origin/main`.
  NEVER commit to main. ⚠ Branch names are load-bearing here: `loop_status.sh`
  attributes open PRs to a loop by branch pattern, so this loop must use
  `fix/<issue>-<short>`, or `chore/ownership-<short>` / `docs/ownership-<short>`
  for tooling and docs. A branch outside those three shapes still works — it
  just stops appearing under this loop in the status view, which is where an
  operator looks to see whether you are blocked.
- Schema → service → tests → glue.
- **Revert-probe every invariant test**: inject the defect it guards, confirm
  the test FAILS, restore, confirm it passes. ⚠ When injecting via string
  replace, `assert s.count(old) == 1` first — a probe that silently matches
  nothing proves nothing.
- **Full-population A/B** before claiming any parser or ETL change is safe —
  `.claude/skills/engineering/full-population-ab.md`. Distinct-entity metric,
  never row count. Inspect the GAIN side, not only the loss side.
- **Codex checkpoint 2 applies here** — this surface is at the ladder's
  "corpus change" rung, so run `codex exec review --base origin/main` before
  the first push. ⚠ `git add -A` FIRST: the diff Codex reads excludes untracked
  files, so a new migration is invisible to it and every confident thing it
  then says is about a codebase that does not exist.
- Gates, each run SEPARATELY, never piped into `head`/`tail` (a pipe returns
  the pipe's status and buffers the output):
  - `uv run ruff check .`
  - `uv run ruff format --check .`
  - `uv run pyright`
  - `uv run pytest -m "not db" -q`
  - `uv run pytest tests/smoke -q`
  All must exit 0. If you touched SQL or ingest, also run the `db`-marked tests
  for the modules you changed and their neighbours — file-scoped, never bare
  `-m db`.
- **Rebase onto `origin/main` immediately before opening the PR**, and again
  before any force-push. ⚠ Two loops merge into this repo now, so a branch goes
  stale within the hour. A stale base makes the review bot report on files that
  are not in your diff — observed 2026-08-06 on #2315, twice in a row, both
  times about a file the branch never touched. If a review round names a file
  you did not change, rebase and re-request rather than answering it.
- Poll `gh pr checks <n>` and `gh pr view <n> --comments` until the review bot
  has posted AND CI is green.
- Resolve EVERY comment as `FIXED <sha>` / `DEFERRED #<issue>` / `REBUTTED <reason>`.
- Merge only on bot APPROVE of the LATEST commit + CI green:
  `gh pr merge <n> --squash --delete-branch`. ⚠ If the round contains a
  rebuttal of yours, do NOT merge — leave it open with your reasoning.

## Hard safety rules

- **NEVER execute, approve or simulate a trade.** No order endpoints, no
  approving recommendations, no touching the kill switch, no closing positions.
- **NEVER `git push --no-verify`.**
- **NEVER weaken, skip or delete a failing test to make a gate pass.** If a test
  fails, fix the code — or explain on the PR why the test was wrong, with
  evidence.
- **NEVER drop, truncate or rewrite dev-DB data** beyond a reviewed migration.
  A tombstone-clearing backfill is a data change: propose it, do not run it.
- Clean up anything you insert while testing.

## Stop rather than guess

Stop, comment on the issue, and end the run when you hit any of these. Stopping
is the success condition, not a failure:

- the task needs a live endpoint or the dev stack;
- it needs a settled decision reversed;
- the issue's premise does not survive your own measurement (say what you
  measured and what it actually shows — a falsified premise is a finding, and
  in this area it is the usual outcome);
- it needs a judgement the operator has reserved: trade behaviour, visual
  taste, a model-version promotion, or irreversible data loss.

## Finish every run by reporting

Post a short comment on the ISSUE you worked stating: what you did, what merged
(with SHA), what you measured (query + result, not a bare figure), the
`## OPERATOR VERIFY` block if the change needs a backfill or a live figure, and
what the next run should pick up. If you stopped early, say exactly why and
what is needed to unblock.

Then leave the worktree clean: no half-done branches, no unpushed commits, no
work intended to ship left without a PR.
