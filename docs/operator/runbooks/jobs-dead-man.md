# Runbook — jobs dead-man and push channel (#3614 item 3)

## Why
On 2026-09-27 the jobs child was SIGKILLed and stayed dark for ~14 h. launchd still showed the supervisor as `running`, and every in-process check died with the child. `scripts/jobs_dead_man.py` watches `max(job_runs.started_at)` from outside the jobs process and pages through ntfy when no job has started for 30 min. The docstring carries the query that reproduces the threshold check.

## What it alerts on
- **Dark daemon:** the newest `job_runs.started_at` is older than 30 min (`--stale-after-s`).
- **Unreadable `job_runs`** (Postgres down, broken config): the newest start seen on an earlier run is the evidence, so it pages on the same 30-min clock and names the error class.
- Re-alerts every 2 h while still dark (`--renotify-s`), and sends one "recovered" notice.

Each alert goes to ntfy (priority 5, when `EBULL_NTFY_TOPIC` is set), a macOS notification, `~/.cache/ebull/jobs_dead_man_status.json` and `var/log/jobs-dead-man.log`.

## Refusal surfaces (same LaunchAgent pass)
Each pass also runs `scripts/operator_alert_watch.py`, unless `job_runs` was unreadable. It pushes one notice per:
- **Kill-switch change** (`runtime_config_audit`, `field = 'kill_switch'`): on at priority 5, off at priority 4.
- **Execution block held for 10 min** (`strategy_execution_blocks`): `drawdown`, `order_reconciliation` or `broker_availability`, plus one notice when the block clears. `quote_freshness` and `scan_freshness` block every night by design and never page.
- **Entry refusals at a limit** (rejected `strategy_entry_preflights`): `sandbox_exceeded` and the mandate loss limits. One run's refusals arrive as one notice.

Its de-duplication state is `~/.cache/ebull/operator_alert_watch_status.json`. A notice is recorded only once it is delivered, and events are read back 1 h. Preview with `uv run python -m scripts.operator_alert_watch --dry-run --status-file /tmp/oaw.json`. A failure of the watch is logged to the dead-man log and never changes the dead-man's own verdict.

## Push channel
`app/system/push_channel.py` publishes to one ntfy topic ([publish API](https://docs.ntfy.sh/publish/)). On the public `ntfy.sh` server the topic name is the only thing that grants read access: use a long random one and never commit it. Optional: `EBULL_NTFY_SERVER` (self-hosted server), `EBULL_NTFY_TOKEN` (bearer token for a protected topic).

Subscribe: install the ntfy app (iOS/Android), tap **+**, enter the topic.

## Install (launchd, wire once)
```bash
REPO="$HOME/Dev/eBull"
TOPIC="ebull-$(python3 -c 'import secrets; print(secrets.token_urlsafe(18))')"
mkdir -p "$REPO/var/log"
sed -e "s#__REPO__#${REPO}#g" -e "s#__NTFY_TOPIC__#${TOPIC}#g" \
  "$REPO/scripts/com.ebull.jobs-dead-man.plist" > ~/Library/LaunchAgents/com.ebull.jobs-dead-man.plist
launchctl load ~/Library/LaunchAgents/com.ebull.jobs-dead-man.plist
(cd "$REPO" && EBULL_NTFY_TOPIC="$TOPIC" .venv/bin/python -m scripts.jobs_dead_man --test-push)   # exit 0 = delivered
```
Read the topic back later with `plutil -extract EnvironmentVariables.EBULL_NTFY_TOPIC raw ~/Library/LaunchAgents/com.ebull.jobs-dead-man.plist`.

## Manual probe
```bash
uv run python -m scripts.jobs_dead_man --status-file /tmp/dm.json   # exit 0 healthy, 2 alerting, 3 alerting but undelivered
```

## When it fires
1. `SELECT job_name, max(started_at) FROM job_runs GROUP BY 1 ORDER BY 2 DESC LIMIT 10;` confirms the gap.
2. `pgrep -fl 'app.jobs'` — the supervisor can be alive while its child is gone.
3. `launchctl kickstart -k "gui/$(id -u)/com.ebull.jobs-daemon"` restarts it (install notes: `scripts/autonomy/README.md`), then check that a new `job_runs` row appears within 5 min.
