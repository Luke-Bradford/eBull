# Runbook — data freshness

## Quick check

Hit the freshness dashboard:

```bash
curl http://localhost:8000/admin/freshness | jq '.'
```

Or per-category:

```bash
curl "http://localhost:8000/admin/freshness?category=ownership"
```

## What "fresh" means

Each source has an expected refresh window declared in
`app/services/data_freshness.py`:

| Source | Window |
|---|---|
| `sec_13f_hr` | 120 days |
| `sec_form4` | 5 days |
| `sec_n_port` | 90 days |
| `sec_13d_13g` | 14 days |
| `sec_def14a` | 400 days |
| `finra_si` | 30 days (planned #915) |

A row is fresh if its `last_obs_at` is within the window for its
source. Beyond the window: stale. With no observation at all:
missing.

## Actioning stale data

Stale data usually means one of:

1. **Job is failing.** Check the jobs heartbeat:
   ```bash
   curl http://localhost:8000/jobs/health
   ```
   If a specific job is failing repeatedly, see
   [`runbook-job-failures.md`](runbook-job-failures.md).
2. **Upstream changed format.** SEC has periodically tightened the
   13F-HR XSD. A parser that worked yesterday may fail today.
   Symptom: ingest log shows `partial` status with `error` rows.
3. **Manifest worker backlog.** Check pending count:
   ```bash
   curl http://localhost:8000/jobs/sec_manifest_worker/status
   ```
   First-install drain can take an hour. Subsequent drains are
   minutes unless a parser change triggered a rebuild.

## When to trust the figure on the operator card

If a slice's coverage banner shows `stale` or `missing`, the figure
on the rollup card is **not** trustworthy for trade-decision use.
The card UI greys out stale slices. Do not rely on a stale slice
for a recommendation.

When the freshness banner says `fresh`, the figure is current per
the source's published cadence (e.g. fresh 13F-HR is at most 45 days
old by SEC publication rules).
