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
	@until docker exec trader-os-postgres pg_isready -U postgres -d trader_os > /dev/null 2>&1; do \
		sleep 1; \
	done
	@echo "Postgres is ready."

migrate: up
	uv run python -c "from app.db.migrations import run_migrations; applied = run_migrations(); print(f'Applied: {applied}' if applied else 'No pending migrations.')"

dev: migrate
	uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

stop:
	docker compose stop

logs:
	docker compose logs -f postgres
