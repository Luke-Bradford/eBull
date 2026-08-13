# Runbook — dev Postgres crash-loop detector (D1, #1449)

## Why
On 2026-06-03 the dev Postgres OOM-killed during WAL recovery and `restart: unless-stopped` looped it **silently for ~18h** (RestartCount 19). Nothing noticed because the app can't detect a wedged PG — its lifespan blocks on PG, so it never even binds its port. RCA: [`docs/proposals/etl/2026-06-03-pg-recovery-oom-rca.md`](../../proposals/etl/2026-06-03-pg-recovery-oom-rca.md).

Defenses, layered:
- **D2** (#1447) — `restart: on-failure:5` + healthcheck stops the *infinite* loop (it now exits after 5 and shows `unhealthy`/`exited`).
- **D1** (this) — `scripts/pg_crash_loop_detector.py` watches from *outside* the app+PG (via the Docker daemon) and **alerts proactively** in minutes, whatever the cause.

## What it alerts on
- **Crash-loop:** `docker inspect` RestartCount rises ≥3 within 15 min.
- **Stuck recovery:** `pg_controldata` shows `in crash recovery` with the REDO location **frozen** for >10 min (the exact OOM-loop signature).

Alert = macOS notification + a JSON status file (`~/.cache/ebull/pg_crash_loop_status.json`) + a loud stderr line.

## Quick manual probe
```bash
uv run python -m scripts.pg_crash_loop_detector --once          # single check, exit 2 if alerting
uv run python -m scripts.pg_crash_loop_detector                 # foreground loop (Ctrl-C to stop)
```
Note: `--once` cannot detect a *stall* (that needs history across time); use the loop, or launchd below.

## Install (launchd, wire once)
```bash
REPO="$(pwd)"                                   # from the eBull checkout root
sed "s#__REPO__#${REPO}#g" scripts/com.ebull.pg-crash-loop-detector.plist \
  > ~/Library/LaunchAgents/com.ebull.pg-crash-loop-detector.plist
launchctl load ~/Library/LaunchAgents/com.ebull.pg-crash-loop-detector.plist
```
Logs: `var/log/pg-crash-loop-detector.log`. Stop: `launchctl unload ~/Library/LaunchAgents/com.ebull.pg-crash-loop-detector.plist`.

## When it fires
1. `docker ps` / `docker inspect ebull-postgres --format '{{.RestartCount}} {{.State.Status}}'`.
2. `docker logs ebull-postgres --tail 40` → look for `terminated by signal 9: Killed` (OOM during recovery).
3. `docker exec ebull-postgres pg_controldata -D /var/lib/postgresql/data` → frozen REDO across restarts confirms the wedge.
4. Follow the RCA's decisive call (§ VERDICT): a relation-count-driven recovery OOM does not recover under the 6 g cap → **wipe + re-bootstrap**. See SKILL §14.1.

## Tuning
`--restart-threshold` / `--restart-window-s` / `--recovery-stall-s` / `--interval` / `--renotify-s`. Defaults: 3 restarts / 15 min, 10 min stall, 60 s poll, 15 min re-alert.
