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

Stop:

```bash
launchctl unload ~/Library/LaunchAgents/com.ebull.jobs-daemon.plist
```

Safe to run this daemon WITHOUT the AI loop and with the kill switch ON — the
kill switch gates only the trade jobs (`morning_candidate_review`,
`retry_deferred`), not data ingestion.
