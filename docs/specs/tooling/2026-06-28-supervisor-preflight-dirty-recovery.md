# Supervisor preflight dirty-tree auto-recovery (#1801)

## Symptom

A headless session that ends mid-edit (context exhausted) before committing leaves a
dirty working tree. `supervisor.sh` preflight correctly refuses to `git checkout` over
uncommitted work (`return 2`) — but the loop then just `sleep 300s` and re-checks
**forever**. No auto-recovery → the board halts indefinitely until a human commits/clears
the WIP. Single point of failure for walk-away-for-days mode.

## Root cause (verified — supervisor.sh:61, 142)

`run_session()` line 61: `[ -z "$(git status --porcelain)" ] || { log ...; return 2; }`.
The `while` loop case `2)` (line 142) sleeps `ERR_BACKOFF_START` (300s) and re-enters with
no state — there is no consecutive-skip counter and no recovery path. A persistently dirty
tree wedges every future iteration.

## Fix — bounded auto-recovery in preflight

Extract the preflight into a sourceable `preflight()` function (so the recovery is unit-
testable against a throwaway repo — see Verification), guarded by a
`[ "${BASH_SOURCE[0]}" = "${0}" ]` main block so sourcing for tests does not start the loop.
A supervisor-process-global counter `dirty_skips` (reset to 0 on any clean-tree observation
or after a successful recovery):

1. **Reorder the guards.** Check an in-progress git operation **first**, using
   `git rev-parse --git-dir` for the path and covering rebase / cherry-pick / merge /
   **revert** / **bisect**. A `git stash` cannot resolve those and could mask half-done
   work; they always keep `return 2` (skip), never auto-recover. Only a *plain* dirty tree
   is recoverable.
2. **Dirty tree:** increment `dirty_skips`. While `< PREFLIGHT_RECOVERY_AFTER` (=2), keep
   `return 2` (grace: the prior session may still be finishing, or the monitor may commit
   the WIP). On the K-th consecutive dirty skip, `git stash push -u -m "autonomy-preflight-
   recovery <utc-ts>"` to preserve the WIP (tracked + untracked), then **re-check
   `git status --porcelain`** (don't trust the stash exit alone) before falling through to
   the normal `checkout main` / `pull --ff-only` path. The stash is **discoverable** via
   `git stash list` and the message is `log()`-ed — WIP is never silently dropped.
3. If the stash fails OR the tree is still dirty after it, log loudly and `return 2`.
   The counter stays ≥ K, so the next iteration retries the stash. A *deterministically*
   failing stash (disk full, repo corruption) is a genuinely unrecoverable state that no
   automated action can safely clear — repeated loud-logged skip is the correct unattended
   behaviour (a human must intervene); we explicitly accept the retry rather than do
   anything destructive or invent a disable-marker for a near-impossible state.
4. **Counter reset** happens on any clean-tree observation (placed *before* the
   fetch/checkout/pull so a one-off fetch failure never carries a stale dirty count) and on
   the board-empty idle branch (which bypasses `preflight()`).

## Why stash, not commit-to-recovery-branch

Stash is branch-independent (works even if HEAD is detached or unexpectedly on `main` from
a crashed mid-checkout), atomic, and trivially reversible (`git stash pop`). A
commit-to-branch path risks committing onto `main` if the crash left HEAD there. The issue
offered either; stash is the safer default.

## Scope

- Recovery applies ONLY to a plain dirty tree. In-progress rebase/cherry-pick/merge/revert/
  bisect still skips (destructive to auto-abort; rare; genuinely needs a human). Documented,
  not a regression of #1801's core (mid-edit dirty tree is the reported failure).
- Repo has **no submodules** (`.gitmodules` absent), so the superproject-stash-misses-
  submodule-WIP edge case does not apply; revisit if submodules are ever added.
- K=2 → ~one 300s grace cycle before recovery (≈5 min), matching the issue's suggestion.
- `supervisor.sh` is the live launchd-managed loop: the RUNNING supervisor must be
  restarted (operator/monitor, like #1775) to load the change. The PR is mergeable normally
  once CI is green; the restart is an operator follow-up noted in the PR.

## Verification

`bash -n` syntax check + a throwaway-repo scenario test (`scripts/autonomy/
test_preflight_recovery.sh`): a dirty tree skips K-1 times then stashes and proceeds; an
in-progress rebase always skips; a clean tree resets the counter.
