"""P6 clean-bootstrap drive — programmatic trigger (no HTTP auth).

Replicates the exact body of POST /system/bootstrap/run (app/api/bootstrap.py):
start_run + publish_manual_job_request_with_conn in one transaction. The
running jobs worker's listener picks up the queued manual_job and dispatches
the bootstrap_orchestrator. Operator-driven trigger for #1423 / P6.
"""

from __future__ import annotations

import psycopg

# Load-bearing: the jobs worker imports this at entry to break a circular
# import in the manifest-parser chain (see app/jobs/__main__.py). Replicate
# that ordering before importing the orchestrator.
import app.services.manifest_parsers  # noqa: F401, E402
from app.api.bootstrap import BootstrapRunRequest
from app.config import settings
from app.services.bootstrap_orchestrator import (
    JOB_BOOTSTRAP_ORCHESTRATOR,
    get_bootstrap_stage_specs,
)
from app.services.bootstrap_state import start_run
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn

with psycopg.connect(settings.database_url) as conn:
    with conn.transaction():
        run_id = start_run(
            conn,
            operator_id=None,
            stage_specs=get_bootstrap_stage_specs(),
            params=BootstrapRunRequest().model_dump(),
        )
        request_id = publish_manual_job_request_with_conn(
            conn,
            JOB_BOOTSTRAP_ORCHESTRATOR,
            requested_by="p6-clean-bootstrap-drive",
        )

print(f"run_id={run_id} request_id={request_id}")
