# Getting started

## What eBull is

A long-horizon AI-assisted investment engine for **eToro**. Research
side is AI-heavy; execution side is deterministic and hard-rule
constrained. Every trade path is auditable.

Posture: demo-first, small-capital live later, **long only in v1**,
no leverage, no shorting, no silent bypass of failed checks.

## Prerequisites

- Python 3.14+ (see `pyproject.toml`).
- PostgreSQL (the dev database is `ebull_dev`; tests use `ebull_test`).
- Node 20+ + pnpm (frontend).
- An eToro account with API access (operator credentials are stored
  encrypted via the broker-secrets system — see ADR
  `docs/adr/0001-operator-auth-and-broker-secrets.md`).

## First-run sequence

1. **Clone + install.**
   ```bash
   uv sync
   pnpm --dir frontend install
   ```

2. **Configure environment.** Copy `.env.example` to `.env` and fill
   in DB connection + SEC user-agent. The `EBULL_SEC_USER_AGENT`
   header is required by SEC EDGAR's fair-use policy (10 req/s,
   identifying the requester).

3. **Provision the database.** Run all migrations under `sql/`. The
   ingest stack uses `_dev` for live operator work; `_test` is for
   the test suite.

4. **Seed reference data.** First-install drain hits SEC for the
   universe of CIK + CUSIP mappings. Expect 5-10 minutes on first
   start. Subsequent boots use the cached `sec_filing_manifest` +
   `external_identifiers` tables.

5. **Boot the dev stack.** Two processes:
   - `app.main` (FastAPI HTTP only).
   - `python -m app.jobs` (APScheduler + sync orchestrator + reaper).

   Both run via VS Code tasks (preferred) or honcho-equivalent
   process managers. **Do not run them via raw `uvicorn`** without
   the jobs sibling — boot freshness sweep + heartbeat will go
   missing.

6. **Verify the stack.** Hit `http://localhost:8000/health` (HTTP
   readiness) and `http://localhost:8000/jobs/health` (jobs process
   heartbeat). Both should return green.

## Where to go next

- For the data plane overview: [`architecture.md`](architecture.md).
- For data source contracts (eToro / SEC / FINRA): [`data-sources.md`](data-sources.md).
- For "I changed the parser, now what?": [`runbooks/runbook-after-parser-change.md`](runbooks/runbook-after-parser-change.md).
