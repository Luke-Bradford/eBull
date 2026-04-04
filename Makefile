.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Dev environment
# ---------------------------------------------------------------------------

.PHONY: help up migrate dev stop logs

help:
	@echo "Usage:"
	@echo "  make dev      — start postgres (if not running), apply migrations, start server"
	@echo "  make up       — start postgres container only"
	@echo "  make migrate  — apply pending SQL migrations"
	@echo "  make stop     — stop postgres container"
	@echo "  make logs     — tail postgres container logs"

up:
	docker compose up -d
	@echo "Waiting for postgres to be ready..."
	@timeout 60 bash -c 'until docker exec trader-os-postgres pg_isready -U postgres -d trader_os > /dev/null 2>&1; do sleep 1; done' \
		|| (echo "Postgres did not become ready in 60s -- check: docker logs trader-os-postgres" && exit 1)
	@echo "Postgres is ready."

migrate: up
	uv run python scripts/migrate.py

dev: migrate
	uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

stop:
	docker compose stop

logs:
	docker compose logs -f postgres
