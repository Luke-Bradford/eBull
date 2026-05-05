# eBull

Long-horizon AI-assisted investment engine for eToro.

- Python 3.14 backend on FastAPI; jobs run in a separate APScheduler process.
- PostgreSQL 17 as the system of record (125+ migrations, partitioned ownership tables).
- React + Vite + TypeScript operator dashboard with Tailwind.
- Claude Code skills / agents / hooks drive research, review, and execution discipline.
- SQL-first schema for auditability — every recommendation, decision, and order ties back to a structured row.
- Demo-first; small-capital live later. Long-only v1, no leverage, no shorting.

## Process topology

The runtime is split (#719):

- **API process** (`uvicorn app.main:app`) — HTTP only. No scheduler, no orchestrator, no reaper.
- **Jobs process** (`python -m app.jobs`) — APScheduler, manual-trigger executor, sync orchestrator, queue dispatcher, reaper, heartbeat. Singleton via Postgres advisory lock.
- **Frontend** — Vite dev server (`pnpm --dir frontend dev`).

IPC is Postgres-only: durable `pending_job_requests` rows + `pg_notify` wakeups. No HTTP between processes.

## Repo structure

- `app/` — FastAPI services, providers, jobs runtime, security, CLI.
  - `app/api/` — HTTP route handlers.
  - `app/services/` — domain logic (filings, ownership, fundamentals, news, ranking, portfolio, execution guard, ledger).
  - `app/providers/` — thin adapters over SEC EDGAR, FINRA, eToro, Companies House, Anthropic.
  - `app/workers/scheduler.py` — declared `ScheduledJob` registry (~26 jobs).
  - `app/jobs/` — runtime, locks, listener, supervisor, manifest worker, ingest workers.
- `sql/` — Postgres migrations (`001` … `125+`). Numeric prefix; never edited in place.
- `frontend/` — React + Vite operator dashboard (pnpm).
- `tests/` — pytest suite (integration tests against `ebull_test` DB).
- `.claude/` — project guidance, skills, agents, hooks, commands.
- `.githooks/pre-push` — runs ruff + format + pyright on every push.
- `docs/` — architecture, scoring model, trading policy, tax engine, settled-decisions, review-prevention log, ADRs, super-power specs.
- `docker-compose.yml` — local Postgres 17.
- `THIRD_PARTY_NOTICES.md` — open-source dependency licenses.

## Current state

Backend services implemented:

- Universe sync (eToro instruments, exchange + sector lookups).
- Market data (OHLCV, intraday candles, FX rates).
- Filings ingestion: SEC EDGAR (Form 3/4/5, 13D/G, 13F-HR, DEF 14A, 8-K, NPORT-P, 10-K/10-Q, XBRL company-facts) and Companies House.
- News + sentiment with Anthropic-classified scores.
- Fundamentals + business-summary ingest from SEC XBRL.
- **Ownership card** (#788 redesign) — two-layer observations + materialised current snapshots, partitioned by `period_end`. Categories: insiders, institutions (13F-HR), blockholders (13D/G), DEF 14A bene, treasury, **funds (NPORT-P, #917)**.
- Thesis engine + critic, scoring + ranking, portfolio manager, execution guard, eToro order client.
- Tax ledger + reconciliation.
- Coverage tier management, ops monitoring, runtime config, broker-credential audit.
- Operator dashboard with chart drilldowns, ownership card, thesis drawer, settings.

In flight:

- Phase 3 of #788 — N-CSR (#918), rollup funds slice + ESOP (#919).
- Short-interest overlay (#915 FINRA bimonthly + #916 RegSHO daily).
- DEF 14A consolidated (#843), DRS / restricted disclosure (#844).
- Chart UI polish: hatching, click-through, coverage-banner v2, history pane (#920–#923).
- EdgarTools as 13F drop-in parser (#925).

## Prerequisites

| Tool | Minimum version | Notes |
|------|----------------|-------|
| Python | 3.14 | |
| uv | 0.5.21 | Python package manager — `pip install uv==0.5.21`. Pinned to match CI. |
| Node.js | 22 LTS | |
| pnpm | 10 | `npm install -g pnpm` |
| Docker | 28 | Local Postgres 17 via `docker-compose.yml`. |
| Git | 2.40+ | |

## Local setup

```bash
cp .env.example .env
docker compose up -d
uv sync --group dev
pnpm --dir frontend install

# Wire the pre-push hook (one-time per clone).
git config core.hooksPath .githooks
```

Then run the three processes side by side (a VS Code task pre-bakes
this):

```bash
uv run uvicorn app.main:app --reload --reload-dir app
uv run python -m app.jobs
pnpm --dir frontend dev
```

Open <http://localhost:5173>. On a fresh database the app drops into
**first-run setup**: pick a username and a password (≥ 12 characters)
on the `/setup` form and you are signed in. After that the standard
`/login` flow takes over.

### Non-loopback bind

The default bind is `127.0.0.1` (loopback only) so the first-run setup
form needs no token. If you change `EBULL_HOST` to a non-loopback
address, the setup form refuses the request unless one of the
following is true:

- you set `EBULL_BOOTSTRAP_TOKEN` in `.env` to a high-entropy string
  and paste that value into the **Setup token** field, **or**
- you let the server generate one on first start: with no env token,
  an empty `operators` table, and a non-loopback bind, the server
  prints a one-shot token to its log on the first request and accepts
  it exactly once on `/setup`.

This is the only path that lets a brand-new instance be set up over
the LAN. There is no IP allow-list — anything reachable on the bind
address can hit the form, so the token is the trust boundary. See
[`docs/adr/0002-first-run-setup.md`](docs/adr/0002-first-run-setup.md).

### Recovery / break-glass CLI

Normal onboarding is the browser flow above. The CLI in
[`app/cli.py`](app/cli.py) exists for cases where the browser path is
unavailable:

```bash
# Forgot your password
uv run python -m app.cli set-password    alice

# Operators table got wiped and the browser flow refuses to help
uv run python -m app.cli create-operator alice
```

Both prompt for the password interactively (via `getpass`) so the
password never appears in shell history. `create-operator` refuses to
overwrite an existing row without `--force`.

## Pre-push checklist

The committed pre-push hook at [`.githooks/pre-push`](.githooks/pre-push)
runs these on every push:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

Pytest is the developer's responsibility before push (CI runs lint +
supply-chain only). Run locally:

```bash
uv run pytest
# Frontend
pnpm --dir frontend typecheck && pnpm --dir frontend test:unit
```

`uv run pytest` includes `tests/smoke/test_app_boots.py`, which drives
the FastAPI lifespan through `TestClient` against the real dev DB. If
that test fails, the running server is broken — fix the root cause,
do not skip it.

## CI

`.github/workflows/ci.yml` runs on every pull request:

- **lint** — ruff check + ruff format + pyright + pre-push hook mode (100755 enforced).
- **supply-chain** — pnpm audit (frontend) + pip-audit (backend lockfile).

Pytest is no longer a CI gate (operator decision 2026-05-05; pre-push
hook is the test gate).

`.github/workflows/claude-review.yml` posts an automated review on
every PR push using Claude.

## Settled decisions

See [`docs/settled-decisions.md`](docs/settled-decisions.md) for live
repo-level decisions: provider strategy, identifier strategy, filing
storage, news/sentiment, thesis semantics, scoring, portfolio manager,
execution guard, process topology, broker-secret encryption.

Per-feature design specs live under
[`docs/superpowers/specs/`](docs/superpowers/specs/) and
[`docs/superpowers/plans/`](docs/superpowers/plans/).

## Third-party software

eBull bundles open-source dependencies under permissive licenses (MIT,
BSD, Apache-2.0) plus LGPL'd psycopg as a runtime link. Full inventory
and per-package notices in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

Public data sources (SEC EDGAR, FINRA, Companies House, eToro API) are
listed there with their terms.

## License

eBull's own source is currently unlicensed (proprietary). Distribution
or modification of eBull source requires explicit operator consent.
The bundled open-source dependencies retain their own licenses
irrespective of eBull's status.
