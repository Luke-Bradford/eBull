# scripts/autonomy/ — SEC jobs daemon

The autonomy **loop** scripts that used to live here were extracted into the
standalone repo-agnostic engine at
[github.com/Luke-Bradford/autonomy-engine](https://github.com/Luke-Bradford/autonomy-engine)
(#1884). eBull now drives the loop via its `.autonomy/` pack (loop_prompt.md,
hard_rules.md, config.yaml) which that engine reads.

This directory now holds only **`com.ebull.jobs-daemon.plist`** — unrelated to
the AI loop. Its install/run docs (previously in the deleted `setup.md`) live
here so the preserved plist stays documented.

## Data daemon — keep ETL fresh with the loop OFF (#1865)

The jobs daemon (`python -m app.jobs`: SEC manifest worker, per-CIK poll,
orchestrator sync, fundamentals, portfolio sync) is **independent of the AI
loop**. Run it under launchd so ingestion survives reboot/crash and data stays
current while the loop is paused. Config is read from the repo `.env` via the
plist's `WorkingDirectory`; only PATH (for `uv`) + HOME are injected.

```bash
mkdir -p var/autonomy-logs
# stop any manual `nohup … python -m app.jobs` first (avoid a duplicate; the
# daemon's PG advisory lock would otherwise idle the second one).
sed "s#__REPO__#$(pwd)#g; s#__HOME__#$HOME#g" scripts/autonomy/com.ebull.jobs-daemon.plist \
  > ~/Library/LaunchAgents/com.ebull.jobs-daemon.plist
launchctl load ~/Library/LaunchAgents/com.ebull.jobs-daemon.plist
launchctl list | grep jobs-daemon                       # confirm loaded
tail -f var/autonomy-logs/launchd.jobs-daemon.err.log   # scheduler ticks (stderr)
```

Restart (after a jobs/parser merge, or any time you want a clean boot):

```bash
launchctl kickstart -k "gui/$(id -u)/com.ebull.jobs-daemon"
```

Stop:

```bash
launchctl unload ~/Library/LaunchAgents/com.ebull.jobs-daemon.plist
```

## Exactly one launcher (#2187)

This agent is the only thing that may start the jobs daemon. The VS Code
`stack: jobs` task no longer launches its own supervisor — it runs the
`kickstart` above and tails the agent's log, so closing that panel does not
stop the daemon.

Why it matters: when both launched a daemon, the task `kill -9`-ed both process
patterns and started `app.jobs.dev_reload`, `KeepAlive` respawned this agent
inside `ThrottleInterval`, and whichever lost the PG advisory-lock race stayed
lost — `dev_reload`'s supervisor only respawns its child on a `*.py` change, not
on a tick, so it parked at ~19 MB forever. Both processes looked healthy while
the auto-reload-on-merge from #2144 was silently dead for weeks.

Safe to run this daemon WITHOUT the AI loop and with the kill switch ON — the
kill switch gates only the trade jobs (`morning_candidate_review`,
`retry_deferred`), not data ingestion.

## `ta_loop.sh` — the headless loop driver (TWO loops run on it)

```bash
scripts/autonomy/loop_status.sh              # every loop, one screen
scripts/autonomy/loop_status.sh ownership    # just one
touch  <worktree>/var/autonomy/PAUSE         # graceful stop after this iteration
tail -f <worktree>/var/autonomy/loop.log
```

| loop | worktree | launchd label | canonical prompt (tracked) | installed as |
| --- | --- | --- | --- | --- |
| TA (#2240) | `~/Dev/.ebull-autonomy` | `com.ebull.ta-loop` | `.autonomy/loop_prompt.md` | `var/autonomy/bin/ta_loop_prompt.md` |
| ownership / filings | `~/Dev/.ebull-ownership` | `com.ebull.ownership-loop` | `scripts/autonomy/ownership_loop_prompt.md` | `var/autonomy/bin/ownership_loop_prompt.md` |

⚠ **The installed prompt is DERIVED, never copied by hand (#2658).** Every
iteration the driver runs `git show $TA_LOOP_PROMPT_REF:$TA_LOOP_PROMPT_SOURCE`
(defaults: `origin/main` and the canonical column above, chosen from the
installed prompt's filename) and replaces the installed copy when the hashes
differ, logging the outcome into `loop.log` and `status.md` either way. Before
this there were two tracked prompt files and the loop read the one nobody
maintained: #2604 re-aimed `.autonomy/loop_prompt.md`, the driver read
`var/autonomy/bin/ta_loop_prompt.md` sourced from a now-deleted
`scripts/autonomy/ta_loop_prompt.md`, and the loop ran a week of completed
marching orders reporting `OK` every iteration.

It reads the git object store rather than the checked-out file on purpose: the
worktree changes branch every iteration, so the tracked path holds whatever that
branch holds — `origin/main:` is branch-independent and is reviewed, merged text.

**One driver, two configurations.** `ta_loop.sh` is loop-agnostic: worktree,
state directory and prompt are env-driven and the single-instance lock lives at
`$TA_LOOP_STATE/loop.lock`, so the two agents cannot collide. Adding a third is
a worktree, a prompt and a plist.

⚠ **A new plist MUST set `TA_LOOP_WORKTREE`.** `WorkingDirectory` does not imply
it — the driver defaults to the TA worktree, and the state directory (hence the
lock) is derived from it. Standing up the ownership loop with the prompt set but
not the worktree pointed it at the TA loop's checkout: three
`ABORT another ta_loop.sh holds …/.ebull-autonomy/var/autonomy/loop.lock` in 21
seconds, one per `KeepAlive` respawn. The lock did its job. Nothing *said* what
was wrong, so the driver now refuses outright when its installed path
(`<worktree>/var/autonomy/bin`) disagrees with `TA_LOOP_WORKTREE`.

⚠ **The tracked script is the source; `var/autonomy/bin/` is what runs.** The
driver drives a worktree whose branch changes every iteration, so a driver read
from a tracked path is deleted by its own `git checkout` the first time the loop
branches off an older commit. `/var/*` is gitignored. Re-copy after any change:

```bash
cp scripts/autonomy/ta_loop.sh <worktree>/var/autonomy/bin/
```

The **driver** is the one thing still copied by hand, and the reason it is not
auto-replaced like the prompt is that the running process is already executing
the old bytes: swapping the file would change what a reader sees without
changing what ran. So the driver instead **checks itself** at startup against
`origin/main:scripts/autonomy/ta_loop.sh` and logs `WARN driver STALE …` with
both hashes when the copy is behind — the check that would have caught #2658's
sibling before anyone went looking.

⚠ **Deliberately dumb, and that is the design.** The previous eBull loop ran
through the autonomy-engine supervisor and died on 2026-07-23 spinning on
`WARN cannot determine kind for in-flight 'coder' (state unreadable)` — it
wedged on its own state machine and nobody noticed for two weeks. This one has
no state machine: one iteration is one `claude -p` call, and a dead iteration
costs exactly one iteration.

Three things it inherits from real incidents:

- **Runs in its own git worktree** (`/Users/lukebradford/Dev/.ebull-autonomy`).
  Two Claude instances on `~/Dev/eBull` clobber each other's uncommitted work —
  prevention-log entry, 2026-07-16.
- **PAUSE is announced in `status.md`, not just the log.** The engine's loop sat
  paused for a MONTH printing its pause line into a log nobody opened.
- **Never pipes a command it needs the exit code or progress from.** A pipe
  returns the pipe's status and buffers output; that cost 7 minutes of
  misdiagnosis on 2026-08-05.

It stops itself after 3 consecutive failed iterations rather than hammering —
three in a row means something structural that another immediate attempt will
not fix.

⚠ It has the dev **database** but NOT the dev **stack**: `:8000` and `:5173`
serve `~/Dev/eBull`, not the worktree. Full-population verification works;
live-endpoint dev-verify does not.
