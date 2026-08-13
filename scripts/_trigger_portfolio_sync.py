from __future__ import annotations

import psycopg

import app.services.manifest_parsers  # noqa: F401
from app.config import settings
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn

with psycopg.connect(settings.database_url) as conn:
    with conn.transaction():
        rid = publish_manual_job_request_with_conn(conn, "daily_portfolio_sync", requested_by="p6-portfolio-sync")
print(f"daily_portfolio_sync queued request_id={rid}")
