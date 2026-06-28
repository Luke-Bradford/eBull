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

## Install the scheduler (refreshing sessions, unattended)
```bash
cp scripts/autonomy/com.ebull.autonomy.plist ~/Library/LaunchAgents/
launchctl load   ~/Library/LaunchAgents/com.ebull.autonomy.plist   # start
launchctl list | grep ebull                                        # confirm
```
It now fires hourly. While a session is mid-drain the next firing no-ops (lock),
so effectively a new session starts within ~1h of the previous finishing.

### Stop / remove
```bash
launchctl unload ~/Library/LaunchAgents/com.ebull.autonomy.plist
rm ~/Library/LaunchAgents/com.ebull.autonomy.plist
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

## Notes / caveats
- `run_loop.sh` uses `--dangerously-skip-permissions` so the unattended session
  isn't blocked on edit/commit prompts. That's the trade-off for hands-off; the
  safety rails above are what keep it bounded.
- Site-review steps need vite (`:5173`) + API (`:8000`) up (the VS Code tasks).
  If they're down the session still does backend/PR work and skips site review.
- Spend is real and continuous while loaded — `launchctl unload` to pause.
- Each run is a fresh session (fresh context). State lives in git + the board +
  memory, so a new session resumes cleanly.
