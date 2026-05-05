# eBull

A self-hosted, AI-assisted long-horizon investment engine for eToro.

eBull aggregates regulatory filings, market data, news, and ownership
disclosures across thousands of US and UK issuers, runs them through
an explicit scoring model and an Anthropic-backed thesis writer, and
surfaces the result as a structured operator dashboard. Every
recommendation, ranking, and order ties back to a row you can audit.

> **Status — actively developed pre-release.** The data plane and
> operator dashboard are functional on a single-instance demo. Live
> trading is gated behind a kill switch and explicit operator opt-in.
> Schemas, endpoints, and frontend surfaces are subject to change
> without notice.

## What it does

- Ingests filings from **SEC EDGAR** (10-K / 10-Q / 8-K / 13F-HR /
  13D / 13G / NPORT-P / N-CSR / DEF 14A / Form 3-4-5) and
  **Companies House** (UK).
- Pulls market data and execution from **eToro**.
- Adds **FINRA short-interest** + RegSHO daily volume and
  **Anthropic-classified** news sentiment.
- Resolves identifiers (CIK, CUSIP, ISIN, LEI) into a single
  instrument graph.
- Materialises an "ownership card" per instrument: institutional /
  insider / blockholder / mutual-fund / treasury / ESOP slices, each
  with a freshness state and a click-through to the underlying
  filings.
- Generates per-instrument theses and a critic pass via the
  Anthropic API; scores and ranks under an explicit, audit-friendly
  v1 model (no ML, no cohort normalisation, no hidden weights).
- Drives a portfolio-manager + execution-guard pipeline that turns
  recommendations into eToro orders — under hard rule constraints
  (long-only, no leverage, no shorting, kill-switch enforced).

## Design philosophy

- **Free regulated-source-only.** SEC, FINRA, Companies House,
  eToro. No paid fundamentals (S&P, FactSet, Bloomberg). No scraped
  feeds. No unofficial API wrappers.
- **Audit before automation.** Every trade path is reviewable.
  Every recommendation cites its inputs. Raw payloads are persisted
  before parsing so a parser bug can be re-washed without re-fetching
  the upstream.
- **Deterministic execution, AI-heavy research.** Thesis writer and
  critic are LLM-driven. The scoring model, portfolio manager, and
  execution guard are explicit code with hard rules.
- **Operator in the loop.** Long-only v1. Kill switch is a runtime
  flag separate from deployment config. Live trading requires
  explicit opt-in and small-capital posture before scaling.
- **Postgres-first.** No Redis pub-sub for control plane, no shared
  memory between processes, no message queues. The system of record
  is one Postgres instance with structured rows you can query
  directly.

## Direction of travel

eBull is being built out in phases:

1. **Tradable universe** — eToro instrument sync, exchange + sector
   metadata, identifier reconciliation. ✓
2. **Market data** — quotes, candles, FX, intraday + EOD. ✓
3. **Filings + news ingestion** — SEC + Companies House + Anthropic
   sentiment. ✓
4. **Ownership card** — multi-source decomposition with freshness
   gates. (Active.)
5. **Thesis + critic engine** — LLM-driven research with structured
   citations. ✓
6. **Ranking engine** — heuristic v1 scoring with audit columns. ✓
7. **Portfolio manager + execution guard** — recommendation →
   guarded order pipeline. ✓
8. **Tax + reconciliation ledger** — basis tracking + reporting. (In flight.)

Active focus areas: closing out ownership decomposition (mutual
funds via N-PORT / N-CSR; short-interest overlay via FINRA),
chart polish on the operator dashboard, and full coverage banner
state machine.

## Stack

- **Backend** — Python 3.14, FastAPI, APScheduler, psycopg 3, uv.
- **Database** — PostgreSQL 17 with partitioned ownership tables and
  125+ migrations.
- **Frontend** — React + Vite + TypeScript + Tailwind.
- **AI** — Anthropic Claude (research + critic + narrative).
- **Process model** — split API ↔ jobs processes, both on the same
  Postgres. Singleton enforced via advisory lock. (#719 settled.)

Full third-party inventory: [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

## Documentation

| Audience | Start here |
|---|---|
| Operator running eBull | [`docs/wiki/`](docs/wiki/) |
| Contributor — workflow | [`.claude/CLAUDE.md`](.claude/CLAUDE.md) |
| Contributor — design decisions | [`docs/settled-decisions.md`](docs/settled-decisions.md) |
| Contributor — recurring mistakes | [`docs/review-prevention-log.md`](docs/review-prevention-log.md) |
| Contributor — epic specs | [`docs/superpowers/specs/README.md`](docs/superpowers/specs/README.md) |

The `docs/wiki/` directory is the source of truth for the operator
wiki. The GitHub Wiki tab can be populated from this directory once
the operator creates the first wiki page via the web UI (GitHub does
not provision the wiki repo until then). See
[`docs/wiki/HOW-TO-PUBLISH.md`](docs/wiki/HOW-TO-PUBLISH.md).

## Local setup

Prerequisites:

| Tool | Minimum |
|---|---|
| Python | 3.14 |
| uv | 0.5.21 |
| Node.js | 22 LTS |
| pnpm | 10 |
| Docker | 28 (for Postgres 17 via `docker-compose.yml`) |
| Git | 2.40+ |

Bootstrap:

```bash
cp .env.example .env       # fill in DATABASE_URL + SEC_USER_AGENT
docker compose up -d
uv sync --group dev
pnpm --dir frontend install
git config core.hooksPath .githooks   # one-time per clone
```

Three processes side by side (a VS Code task pre-bakes this):

```bash
uv run uvicorn app.main:app --reload --reload-dir app
uv run python -m app.jobs
pnpm --dir frontend dev
```

Open <http://localhost:5173>. On a fresh database the app drops into
**first-run setup**: pick a username and a password (≥ 12 characters)
on `/setup` and you are signed in.

### Non-loopback bind

The default bind is `127.0.0.1` (loopback only) so the first-run setup
form needs no token. If you change `EBULL_HOST` to a non-loopback
address, the setup form refuses the request unless one of the
following is true:

- `EBULL_BOOTSTRAP_TOKEN` is set to a high-entropy string in `.env`
  and pasted into the **Setup token** field, **or**
- the server generates one on first start (no env token + empty
  `operators` table + non-loopback bind) and prints a one-shot token
  to its log on the first request.

The login surface is open to anyone reachable on the bind address —
there is no IP allow-list, the token is the trust boundary. See
[`docs/adr/0002-local-browser-bootstrap-and-multi-operator.md`](docs/adr/0002-local-browser-bootstrap-and-multi-operator.md).

### Recovery / break-glass CLI

Normal onboarding is the browser flow above. The CLI in
[`app/cli.py`](app/cli.py) exists for cases where the browser path is
unavailable:

```bash
uv run python -m app.cli set-password    alice    # forgot password
uv run python -m app.cli create-operator alice    # operators table wiped
```

Both prompt interactively via `getpass`; passwords never appear in
shell history. `create-operator` refuses to overwrite an existing
row without `--force`.

## Pre-push checklist

The committed pre-push hook at [`.githooks/pre-push`](.githooks/pre-push)
runs on every push:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

Pytest is the developer's responsibility before push — CI runs lint +
supply-chain only. Run locally:

```bash
uv run pytest
pnpm --dir frontend typecheck && pnpm --dir frontend test:unit
```

`uv run pytest` includes `tests/smoke/test_app_boots.py`, which drives
the FastAPI lifespan against the real dev DB — that test failing
means the running server is broken, not that the test is flaky.

## CI

`.github/workflows/ci.yml` on every pull request:

- **lint** — ruff check + format + pyright + pre-push hook mode-bit check.
- **supply-chain** — pnpm audit (frontend) + pip-audit (backend).

`.github/workflows/claude-review.yml` posts an automated review on
every PR push.

## Contributing

eBull is currently developed by a single operator. Contributions are
accepted under the project's source license (currently proprietary).
Read [`.claude/CLAUDE.md`](.claude/CLAUDE.md) end-to-end before
opening a PR — the workflow rules (branch + PR sequence, Codex
checkpoints, review resolution contract, ETL definition-of-done
clauses) are non-negotiable.

## License

eBull's own source is currently unlicensed (proprietary).
Distribution or modification of eBull source requires explicit
operator consent. Open-source dependencies retain their own licenses
irrespective of eBull's status.
