# trader-os

Long-horizon AI-assisted investment engine for eToro.

This starter repo is intentionally opinionated:
- Python backend
- PostgreSQL as the system of record
- Claude Code skills / agents / hooks for research and execution discipline
- SQL-first schema for auditability
- Demo-first, live-small-capital later

## What this skeleton contains

- `docs/` design and policy docs
- `.claude/` project guidance, skills, agents, and hook notes
- `sql/001_init.sql` initial Postgres schema
- `app/` minimal Python service skeleton
- `docker-compose.yml` for local Postgres

## Suggested build order

1. Create the database with `sql/001_init.sql`
2. Read:
   - `docs/architecture.md`
   - `docs/scoring-model.md`
   - `docs/trading-policy.md`
   - `docs/tax-engine.md`
3. Open `.claude/CLAUDE.md` in Claude Code
4. Implement services in this order:
   - universe sync
   - market data
   - filings
   - thesis engine
   - ranking engine
   - portfolio manager
   - execution guard
   - tax ledger
5. Use Demo keys before anything live

## Local setup

```bash
cp .env.example .env
docker compose up -d
```

This repo is a scaffold, not a production-ready trading system.
You still need to implement:
- eToro API client
- filings/news/macro providers
- job scheduling
- actual order placement
- reconciliation
- proper tests
