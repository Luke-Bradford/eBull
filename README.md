# eBull

Long-horizon AI-assisted investment engine for eToro.

- Python backend with FastAPI
- PostgreSQL as the system of record
- Claude Code skills / agents / hooks for research and execution discipline
- SQL-first schema for auditability
- Demo-first, live-small-capital later

## Repo structure

- `app/` — services, providers, workers, and API
- `sql/` — Postgres migrations (001–010)
- `docs/` — architecture, scoring model, trading policy, tax engine
- `.claude/` — project guidance, skills, agents, and hooks
- `tests/` — pytest suite
- `docker-compose.yml` — local Postgres

## Current state

Backend services implemented:
- Universe sync
- Market data (OHLCV, quotes, features)
- Filings and fundamentals (SEC EDGAR, Companies House, FMP)
- News and sentiment
- Scoring and ranking engine
- Portfolio manager
- Execution guard

Remaining backend:
- Thesis engine (#6)
- eToro order client (#10)
- Tax ledger (#11)
- Coverage tier management (#12)

Not yet started:
- API layer (REST endpoints for frontend)
- Frontend / dashboard
- Ops monitoring and admin controls

## Local setup

```bash
cp .env.example .env
docker compose up -d
```

## Build order

See `.claude/CLAUDE.md` and `docs/architecture.md` for detailed guidance.
