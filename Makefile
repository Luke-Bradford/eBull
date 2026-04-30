.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Dev environment
# ---------------------------------------------------------------------------

.PHONY: help up migrate dev stop logs stack stack-stop frontend-install frontend-dev frontend-build frontend-typecheck

help:
	@echo "Usage:"
	@echo "  make stack              — start the full stack (postgres + backend + frontend) in new windows"
	@echo "  make stack-stop         — stop the full stack"
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
	pwsh -File ./stack.ps1

stack-stop:
	pwsh -File ./stack-stop.ps1

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
