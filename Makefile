.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Dev environment
# ---------------------------------------------------------------------------

# Per-OS dispatch for the stack scripts. Windows uses the .ps1 path
# (PowerShell-only `netstat -ano` ghost-socket handling); macOS / Linux
# use the .sh path.
ifeq ($(OS),Windows_NT)
    STACK_PREPARE := pwsh -File ./stack.ps1
    STACK_STOP    := pwsh -File ./stack-stop.ps1
else
    STACK_PREPARE := ./stack.sh
    STACK_STOP    := ./stack-stop.sh
endif

.PHONY: help up migrate dev stop logs stack stack-stop frontend-install frontend-dev frontend-build frontend-typecheck

help:
	@echo "Usage:"
	@echo "  make stack              — prepare the dev stack (postgres + migrations); backend/frontend launch via VS Code tasks"
	@echo "  make stack-stop         — stop backend, frontend, jobs process, and postgres"
	@echo "  make dev                — start postgres (if not running), apply migrations, start server"
	@echo "  make up                 — start postgres container only"
	@echo "  make migrate            — apply pending SQL migrations"
	@echo "  make stop               — stop postgres container"
	@echo "  make logs               — tail postgres container logs"
	@echo "  make frontend-install   — install frontend dependencies (pnpm)"
	@echo "  make frontend-dev       — start the Vite dev server (proxies /api -> :8000)"
	@echo "  make frontend-build     — production build of the frontend"
	@echo "  make frontend-typecheck — typecheck the frontend"

stack:
	$(STACK_PREPARE)

stack-stop:
	$(STACK_STOP)

frontend-install:
	cd frontend && pnpm install

frontend-dev:
	cd frontend && pnpm dev

frontend-build:
	cd frontend && pnpm build

frontend-typecheck:
	cd frontend && pnpm typecheck

up:
	docker compose up -d
	@echo "Waiting for postgres to be ready..."
	@timeout 60 bash -c 'until docker exec ebull-postgres pg_isready -U postgres -d ebull > /dev/null 2>&1; do sleep 1; done' \
		|| (echo "Postgres did not become ready in 60s -- check: docker logs ebull-postgres" && exit 1)
	@echo "Postgres is ready."

migrate: up
	uv run python scripts/migrate.py

dev: migrate
	uv run uvicorn app.main:app --reload --reload-dir app --host 0.0.0.0 --port 8000

jobs:
	uv run python -m app.jobs

stop:
	docker compose stop

logs:
	docker compose logs -f postgres
