# Architecture

High-level operator-facing overview. For implementation detail, see
the spec docs at `docs/superpowers/specs/`.

## Two processes, one database

eBull runs as **two separate Python processes** that share a single
Postgres instance.

- **API process** (`app.main`) — FastAPI. HTTP only. No
  schedulers, no executors, no reapers. Operator-facing endpoints:
  ownership rollup, instrument detail, jobs admin, etc.
- **Jobs process** (`python -m app.jobs`) — APScheduler + sync
  orchestrator + manual-trigger executor + reaper + queue
  dispatcher + boot freshness sweep + heartbeat writer.

Both processes use a hardened connection pool (`app/db/pool.py`).

**Why split?** The API process must remain responsive to operator
requests; long-running ingestion + ML calls would block FastAPI's
event loop. The split is settled (#719) — see
`docs/settled-decisions.md` "Process topology".

## Inter-process communication

**Postgres only.** No HTTP between API and jobs, no Redis pub/sub,
no shared memory.

- Durable rows in `pending_job_requests`.
- `pg_notify('ebull_job_request', ...)` as a wakeup hint.
- A trigger sent while jobs is restarting is replayed on boot — the
  durable row survives.

Singleton enforcement: a session-scoped Postgres advisory lock on a
dedicated long-lived connection guarantees one jobs process at a
time. Starting a second is a hard FATAL exit.

## Data plane

```
┌──────────┐   ┌──────────┐   ┌──────────────┐
│ eToro API│   │ SEC EDGAR│   │ FINRA / etc. │
└────┬─────┘   └────┬─────┘   └──────┬───────┘
     │ quotes/      │ filings        │ short-interest /
     │ orders       │ NPORT / 13F    │ regsho
     ▼              ▼                ▼
┌──────────────────────────────────────────┐
│ Provider adapters (app/providers)        │
│ — thin HTTP wrappers, no DB, no domain   │
└────────────────┬─────────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────────┐
│ Service layer (app/services)             │
│ — DB-aware, owns identity resolution +   │
│   raw-payload persistence + parsing +    │
│   write-through to canonical tables.     │
└────────────────┬─────────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────────┐
│ Postgres                                 │
│ — raw payload tables, observation        │
│   tables (append-only), current tables   │
│   (write-through), reference tables.     │
└──────────────────────────────────────────┘
```

**Provider rule.** Providers do not own DB lookups. They are HTTP
shims. Any identifier resolution belongs in the service layer. See
`docs/settled-decisions.md` "Provider strategy".

## Three-tier ownership data

For ownership data specifically:

1. **Raw documents.** SEC payload bytes land in
   `filing_raw_documents` / `cik_raw_documents` /
   `sec_reference_documents` BEFORE any parser runs. This is a
   PREVENTION rule (see `docs/review-prevention-log.md`).
2. **Observations.** Append-only event-log tables:
   `ownership_insider_observations`, `ownership_institution_observations`,
   `ownership_blockholder_observations`, `ownership_funds_observations`,
   `ownership_treasury_def14a_observations`. Every parser run writes
   here without overwriting prior rows.
3. **Current.** Materialised "what's true now" snapshots:
   `ownership_*_current`. Refreshed by `refresh_*_current` writers
   that apply source-priority dedupe + filed_at tie-break.

The rollup endpoint reads only from `*_current` (see
`app/services/ownership_rollup.py`).

## Broker boundary

eToro is the source of truth for:
- tradable universe
- quotes + candles in v1
- portfolio + account data
- execution

When official filings (SEC, Companies House) and eToro's normalized
data conflict, **prefer the official filing**. See
`docs/settled-decisions.md` "Conflict rule".

## Execution guard

Every executable trade goes through a guard layer that re-checks
critical constraints against current state. The guard has its own
table (`decision_audit`) that records every invocation with per-rule
results — see the spec at
`docs/superpowers/specs/2026-04-18-cascade-advisory-lock-design.md`.

Guard rules differ by action:
- `BUY` / `ADD`: kill switch, config flags, fresh thesis, Tier 1
  coverage, spread / cash / concentration.
- `EXIT`: do not block on stale thesis or wide spread (the position
  must be closeable even if research is stale).

The kill switch is a DB-backed runtime flag, separate from
deployment config flags.
