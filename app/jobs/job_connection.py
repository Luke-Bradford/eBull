"""Bounded DB connections for scheduled-job bodies (#1690).

A scheduled job whose body query wedges on a SQL-level wait (a lock wait,
a runaway scan) would otherwise leave its ``job_runs`` row ``status='running'``
forever — Python cannot force-kill the hung thread, so ``_tracked_job`` never
reaches ``record_job_finish`` and only the next restart's boot reaper clears
it (#1689 Decision 4).

The fix: bound the *query* so the wedged backend cancels itself. The bound is
a per-job ``statement_timeout`` carried on a ContextVar that
``app.workers.scheduler._tracked_job`` sets for the duration of the job body,
and read here by ``connect_job`` when a body opens a connection.

Why ``options=`` and not ``SET statement_timeout`` (verified empirically on
dev PG17, 2026-06-20):
  * ``options='-c statement_timeout=N'`` is a libpq *startup* parameter — it
    is applied outside any transaction, so it is immune to ``ROLLBACK`` and
    opens no implicit transaction. Most job bodies are non-autocommit and roll
    back on close.
  * a plain ``SET statement_timeout`` (even without ``LOCAL``) IS reverted on
    ``ROLLBACK`` — it would be silently undone mid-job.

Scope: statement_timeout caps a *single statement* (including time spent
waiting on a lock), NOT whole-job wall-clock. A long sweep made of many short
statements is safe under the cap. See
``docs/specs/infra/job-statement-timeout.md``. NEVER a process-global
``PGOPTIONS`` (would kill legitimate long ETL) — the bound is scoped
per-connection only, per
``docs/proposals/infra/2026-06-04-db-connection-discipline.md`` GAP-A/GAP-B.
"""

from __future__ import annotations

import contextvars
from typing import Any

import psycopg

from app.config import settings

# Per-job statement_timeout (ms) for the active job body. ``None`` (the
# default, and the value outside a tracked job) means "no bound" — so
# non-job callers, manual-trigger jobs not in ``SCHEDULED_JOBS``, and any
# unmigrated site behave exactly like a raw ``psycopg.connect``. Set/reset
# by ``_tracked_job`` with token semantics so the orchestrator's inner-adapter
# re-entry nests correctly. Lives only for the duration of one invoker call on
# its worker thread (same model as ``_prelude_run_id`` / ``_HELD_SOURCES``).
job_statement_timeout_ms: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "job_statement_timeout_ms", default=None
)


def connect_job(*, autocommit: bool = False, **kwargs: Any) -> psycopg.Connection[Any]:
    """Drop-in for ``psycopg.connect(settings.database_url, ...)`` inside a
    scheduled-job body.

    When the active job has a ``statement_timeout`` (set by ``_tracked_job``
    via the ContextVar) the bound is applied as a libpq startup option so the
    body's queries self-abort on a SQL-level wedge. Outside a tracked job the
    var is ``None`` and this is identical to a raw connect.

    Returns a ``psycopg.Connection`` (a context manager) — usable identically
    in ``with connect_job() as conn:`` and ``with (Provider(...), connect_job()
    as conn):`` tuple forms.
    """
    ms = job_statement_timeout_ms.get()
    if ms is not None:
        opt = f"-c statement_timeout={ms}"
        existing = kwargs.get("options")
        kwargs["options"] = f"{existing} {opt}" if existing else opt
    return psycopg.connect(settings.database_url, autocommit=autocommit, **kwargs)
