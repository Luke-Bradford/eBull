You are working on the eBull TA strategy platform, headless and unattended. One
task per run. You have the full repo, the local dev Postgres, and all project
skills.

## Read first — do not skip

1. `.claude/CLAUDE.md` — engineering discipline. Non-negotiable and hard-won.
2. `docs/proposals/ta/2026-08-05-strategy-registry-and-signal-ledger.md` — phase 3.
3. `docs/proposals/ta/strategy-catalogue-and-backtest-validity.md` §3.5 and the
   numbered criteria — execution semantics.
4. `docs/superpowers/specs/2026-08-04-ta-strategy-platform-design.md` §5 — phases.
5. `docs/review-prevention-log.md` — skim for anything touching what you change.

## Pick ONE task

Check `gh pr list --state open` and recent commits first — never duplicate work
already in flight. Then take the first item below that is not done:

1. **Finish phase 5** — the bounded backtester, per §5 of the design spec and
   the phase-5 spec (`docs/proposals/ta/…`). 5a/5b/5c are merged; continue from
   wherever the ledger and the result model actually are, not from this list.
2. **#2364 — the promotion gate's two missing refusals.** Operator DECIDED
   2026-08-07; read the decision comment on the issue, it is not a menu:
   - a win rate is NEVER displayed as a bare point estimate — always with its
     **Wilson 95% interval**;
   - promotion requires the interval's **LOWER bound** to beat the comparator,
     so no magic minimum-n has to be defended;
   - the binding quantity is **effective n** — distinct non-overlapping holding
     windows, not raw position count (this also closes §7's correlated-signals
     question);
   - comparator (a) time-matched buy-and-hold on the same instrument is the
     GATE; (b) random-entry bootstrap at matched hold length is the significance
     test; (c) SPY over the same window is reporting context, not a gate;
   - new refusals `sample_below_display_floor` and `comparator_not_beaten`,
     added WITH the derived-contract test (#2229: a closed vocabulary in three
     places produced live 500s with the suite green).
3. **#2363 — FX is charged at zero on a GBP account buying USD instruments.**
   A live-path defect, and the markup is measurable rather than assumable:
   `GET /api/v1/balances` returns the `exchangeRate` eToro actually applied
   (verified on the portal 2026-08-07), and `fx_rates_refresh` already gives an
   independent mid. ⚠ Read the operator's comment first — a STANDALONE
   conversion pays once, not per trade, so FX is an account-state cost and
   charging it per position overstates a pre-funded balance. Carry stays zero
   but the REASON must be written into the code (long-only + unleveraged = real
   stock, not CFD, so no overnight financing — wrong the day v2 adds leverage).
4. **Phase 6** — signals lens + strategy performance surface. The first
   operator-visible TA surface. ⚠ Do NOT start it before 2 and 3 land: it
   renders numbers, and both of those change what the numbers mean.

⚠ **ONE task. Do it completely — spec-conformant, tested, PR opened, review
resolved, merged if green. Do not start a second.** A half-finished second task
is worse than an idle iteration.

⚠ **This list goes stale — the SPEC is the authority, not this prompt.** Items
1-4 of the previous revision were all complete while still listed, and the loop
correctly navigated off the phase table instead. If this list disagrees with
`docs/superpowers/specs/2026-08-04-ta-strategy-platform-design.md` §5, the spec
wins; say so in the run report so the prompt gets fixed.

## You HAVE a database — use it

Unlike a cloud sandbox, you can reach the dev Postgres. So the full-population
rule applies in full:

- **Verify on the FULL population, never a sample.** This repo has repeatedly
  been bitten by a favourable sample: a 3-series Bollinger check showed a 40×
  safety margin and the full 7,354-series sweep then failed it with 193
  mismatches.
- ⚠ **Never state a number you did not compute in this run.**
- Migrations: apply with `PYTHONPATH=. uv run python scripts/migrate.py`.
- ⚠ You are in a WORKTREE. The dev API (:8000) and vite (:5173) serve
  `~/Dev/eBull`, NOT here — so endpoint dev-verify is unavailable. Database
  access works fine. If a task needs a live endpoint, say so on the issue and
  pick the next task.

## Workflow

- `git checkout -b feature/2240-<short>` off `origin/main`. NEVER commit to main.
- Schema → service → tests → glue.
- **Revert-probe every invariant test**: inject the defect it guards, confirm
  the test FAILS, restore, confirm it passes. ⚠ When injecting via string
  replace, `assert s.count(old) == 1` first — a probe that silently matches
  nothing proves nothing, and that has already happened here.
- Gates, each run SEPARATELY, never piped into `head`/`tail` (a pipe returns
  the pipe's status and buffers output):
  - `uv run ruff check .`
  - `uv run ruff format --check .`
  - `uv run pyright`
  - `uv run pytest -m "not db" -q`
  - `uv run pytest tests/smoke -q`
  All must exit 0.
- Push, open a PR with a complete description.
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
- Clean up anything you insert while testing.

## Finish every run by reporting

Post a short comment on issue **#2240** stating: what you did, what merged (with
SHA), what you measured, and what the next run should pick up. If you stopped
early, say exactly why and what is needed to unblock.

Then leave the worktree clean: no half-done branches, no unpushed commits, no
work intended to ship left without a PR.
