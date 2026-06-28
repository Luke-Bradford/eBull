# Autonomy loop — setup (refreshing unattended sessions)

Drains the engineering board on a schedule, each firing a **fresh** headless
`claude` session, until the board is clear. Runs on THIS Mac (the loop needs the
local dev stack + browser; cloud routines can't reach localhost).

## Pieces
- `loop_prompt.md` — the standing task (drain board, full workflow per ticket, safety rails).
- `run_loop.sh` — one headless session per run (lockfile = one at a time; logs to `var/autonomy-logs/`).
- `com.ebull.autonomy.plist` — launchd agent, fires `run_loop.sh` hourly.

## Try one run by hand first (recommended)
```bash
bash scripts/autonomy/run_loop.sh        # blocks for the session; tail the log it prints
```
Watch it pick a ticket, open a PR, wait for the bot, merge. Ctrl-C to stop; the
lock auto-clears on exit.

## Walk-away-for-days mode (RECOMMENDED): the supervisor
Runs sessions back-to-back forever with **usage-limit backoff** — hits a limit →
backs off to the reset window → retries when capacity returns; board empty →
idle-polls; kept alive across crashes/reboots by launchd `KeepAlive`. Start it
once and leave for days.
```bash
mkdir -p var/autonomy-logs
cp scripts/autonomy/com.ebull.autonomy.supervisor.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.ebull.autonomy.supervisor.plist
launchctl list | grep ebull          # confirm loaded
tail -f var/autonomy-logs/supervisor.log
```
Stop:
```bash
launchctl unload ~/Library/LaunchAgents/com.ebull.autonomy.supervisor.plist
```

## Simpler alternative: hourly run_loop (no continuous supervisor)
One fresh session per hour (lock = no overlap). Less tight than the supervisor
and no smart limit-backoff (a limited hour just retries next hour), but minimal.
Use this OR the supervisor, **not both**.
```bash
cp scripts/autonomy/com.ebull.autonomy.plist ~/Library/LaunchAgents/
launchctl load   ~/Library/LaunchAgents/com.ebull.autonomy.plist
```

### Stop / remove
```bash
launchctl unload ~/Library/LaunchAgents/com.ebull.autonomy.plist        # or .supervisor.plist
rm ~/Library/LaunchAgents/com.ebull.autonomy*.plist
```

### Watch it
```bash
tail -f var/autonomy-logs/loop-*.log        # latest session transcript (stream-json)
gh pr list ; gh issue list --state open      # the board draining
```

## What it will and won't do
- **Will:** triage open issues, fix them through the full workflow (spec → Codex →
  gates → PR → bot review → resolve every comment → merge), restart the jobs
  daemon when needed, run `dq_audit.py` + site review to file new verified
  tickets, update memory.
- **Won't (hard rails in the prompt + appended system prompt):** execute/approve
  any trade, touch the kill-switch, close a position, merge around the Claude
  review bot, or `--no-verify`. Trade execution stays human-gated; the review bot
  + execution-guard remain the safety gates.

## REQUIRED before going unattended (mechanical safety gates — Codex ckpt-2)
A prompt rule is not a control under `--dangerously-skip-permissions`. Two
server/credential-level gates make the loop safe regardless of session behaviour:

1. **Server-side merge gate — required status checks on `main`.** `main` already
   requires a review; also require the bot + CI checks so GitHub itself blocks a
   merge until they pass (the loop physically cannot merge a red/un-reviewed PR):
   ```bash
   gh api -X PUT repos/Luke-Bradford/eBull/branches/main/protection/required_status_checks \
     -f strict=true -f 'contexts[]=review' -f 'contexts[]=lint' -f 'contexts[]=build'
   ```
   Decide separately whether to keep the human PR-approval requirement: keep it →
   the loop does everything and leaves each PR for your one-click merge
   (recommended for a trading repo); drop it → the loop self-merges once checks
   are green (zero clicks, less oversight). `safe_merge.sh` enforces the
   bot-APPROVE+green check locally either way (defence-in-depth).

2. **No broker credentials for the loop.** Run with NO eToro creds configured so
   the order client fails closed — an unattended session then cannot place even a
   demo order. Verify `GET /broker/credentials` is empty (or creds absent from
   the loop's env) before loading the agent.

## Notes / caveats
- `run_loop.sh` uses `--dangerously-skip-permissions` so the unattended session
  isn't blocked on edit/commit prompts. That's the trade-off for hands-off; the
  safety rails above are what keep it bounded.
- Site-review steps need vite (`:5173`) + API (`:8000`) up (the VS Code tasks).
  If they're down the session still does backend/PR work and skips site review.
- Spend is real and continuous while loaded — `launchctl unload` to pause.
- Each run is a fresh session (fresh context). State lives in git + the board +
  memory, so a new session resumes cleanly.
