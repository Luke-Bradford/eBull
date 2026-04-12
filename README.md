# eBull

Long-horizon AI-assisted investment engine for eToro.

- Python backend with FastAPI
- PostgreSQL as the system of record
- Claude Code skills / agents / hooks for research and execution discipline
- SQL-first schema for auditability
- Demo-first, live-small-capital later

## Repo structure

- `app/` — services, providers, workers, and API
- `sql/` — Postgres migrations (001–021 and counting)
- `docs/` — architecture, scoring model, trading policy, tax engine
- `.claude/` — project guidance, skills, agents, and hooks
- `frontend/` — React + Vite operator dashboard (pnpm)
- `tests/` — pytest suite
- `docker-compose.yml` — local Postgres

## Current state

Backend services implemented:
- Universe sync
- Market data (OHLCV, quotes, features)
- Filings and fundamentals (SEC EDGAR, Companies House, FMP)
- News and sentiment
- Thesis engine (#6)
- Scoring and ranking engine
- Portfolio manager
- Execution guard
- eToro order client (#10)
- Tax ledger (#11)
- Coverage tier management (#12)
- REST API layer (FastAPI) and operator dashboard
- Ops monitoring (job runs, runtime config, admin page)
- Broker credential management with durable audit log

Currently in flight:

- Copy-trading ingestion (#183 Track 1a), AUM correction (#187
  Track 1b), browsing UX (#188 Track 1.5), discovery (#189 Track 2)

## Prerequisites

| Tool | Minimum version | Notes |
|------|----------------|-------|
| Python | 3.14 | `python --version` |
| uv | 0.11 | Python package manager — `pip install uv` |
| Node.js | 22 LTS | `node --version` |
| pnpm | 10 | `npm install -g pnpm` |
| Docker | 28 | Runs PostgreSQL 17 via `docker-compose.yml` |
| Git | 2.40+ | |

## Local setup

```bash
cp .env.example .env
docker compose up -d
uv sync --group dev
uv run uvicorn app.main:app --reload
pnpm --dir frontend install && pnpm --dir frontend dev
```

Open <http://localhost:5173>. On a fresh database the app drops into
**first-run setup**: pick a username and a password (≥ 12 characters)
on the `/setup` form and you are signed in. After that the standard
`/login` flow takes over.

### Non-loopback bind

The default bind is `127.0.0.1` (loopback only) so the first-run setup
form needs no token. If you change `EBULL_HOST` to a non-loopback
address, the setup form will refuse the request unless one of the
following is true:

- you set `EBULL_BOOTSTRAP_TOKEN` in `.env` to a high-entropy string
  and paste that value into the **Setup token** field, **or**
- you let the server generate one on first start: with no env token,
  an empty `operators` table, and a non-loopback bind, the server
  prints a one-shot token to its log on the first request and accepts
  it exactly once on `/setup`.

This is the only path that lets a brand-new instance be set up over
the LAN. There is no IP allow-list — anything reachable on the bind
address can hit the form, so the token is the trust boundary.

### Recovery / break-glass CLI

Normal onboarding is the browser flow above. The CLI in `app/cli.py`
exists for cases where the browser path is unavailable:

```bash
# Forgot your password
uv run python -m app.cli set-password    alice

# Operators table got wiped and the browser flow refuses to help
uv run python -m app.cli create-operator alice
```

Both prompt for the password interactively (via `getpass`) so it never
appears in shell history. `create-operator` refuses to overwrite an
existing row without `--force`.

## Build order

See `.claude/CLAUDE.md` and `docs/architecture.md` for detailed guidance.
