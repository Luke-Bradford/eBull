# Runbook — diagnosing a failed job

## Step 1 — find the failure

```bash
curl http://localhost:8000/admin/jobs/recent-failures | jq '.'
```

Or for a specific job:

```bash
curl "http://localhost:8000/admin/jobs/<job_name>/runs?status=failed&limit=10"
```

Each failure carries: `started_at`, `finished_at`, `status`,
`error`, `error_traceback` (if structured).

## Step 2 — categorise

| Symptom | Likely cause | Action |
|---|---|---|
| `partial` status with one accession's `error` | Single bad upstream payload | Tombstone the accession via the rebuild job; let the next run skip it |
| `failed` status with `psycopg` exception | DB schema drift or migration not applied | Run `psql -d ebull_dev -f sql/<missing>.sql` |
| `failed` status with `httpx.ConnectError` | Upstream down (SEC fair-use throttling, FINRA CDN outage) | Wait + retry; the manifest worker is retry-safe |
| Job not running at all | Jobs process not booted, or advisory lock held by another instance | Check `python -m app.jobs` is the singleton process |
| `failed` with a `RewashParseError` | Parser-version bump rejected an old payload | Open a follow-up ticket; the manifest worker will drain on next run |

## Step 3 — re-run

```bash
curl -X POST http://localhost:8000/jobs/<job_name>/run
```

The trigger is durable: a row in `pending_job_requests` plus a
`pg_notify` wakeup hint. Even if the jobs process is restarting,
the trigger is replayed on boot.

## Step 4 — verify

After the re-run completes:

```bash
curl "http://localhost:8000/admin/jobs/<job_name>/runs?limit=1"
```

Expect `status: ok` with `finished_at` set.

## Common errors

### `master_key.bootstrap` failure on startup

The lifespan smoke gate (see `tests/smoke/test_app_boots.py`)
catches this. If it surfaces in production, the API process will
not boot. Fix: check the master-key state in the operator-secrets
table — see ADR `docs/adr/0001-operator-auth-and-broker-secrets.md`.

### `Singleton instance already running`

The jobs process uses a Postgres advisory lock to enforce singleton.
Two simultaneous `python -m app.jobs` invocations cause the second
to FATAL exit. Fix: kill the older process first, or wait for it to
finish.

### `kill switch active`

The kill switch is a DB-backed flag separate from deployment config.
It blocks all `BUY` / `ADD` execution until disabled. Check via:

```bash
curl http://localhost:8000/admin/kill-switch/status
```

To disable, the operator must explicitly toggle it via the admin UI.
This is intentional friction.
