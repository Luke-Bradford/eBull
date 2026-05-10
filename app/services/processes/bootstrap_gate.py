"""Universal ``bootstrap_state.status='complete'`` gate.

PR1b-2 of #1064 admin-control-hub follow-up sequence.
Plan: docs/internal/plans/pr1-job-registry-refactor.md (Step 7).

## Why this exists

Pre-PR1b-2 the bootstrap-completion gate was implicit: each
SEC/fundamentals job declared a ``ScheduledJob.prerequisite``
(``_bootstrap_complete``) that returned ``(False, reason)`` while
``bootstrap_state.status`` was anything other than ``'complete'``.
The scheduled-fire path honoured the prereq; PR1b extended that to
the manual-queue path.

The gate is ORTHOGONAL to per-job prerequisites:

* **Per-job prereq** (e.g. ``_bootstrap_complete``, ``_has_filings``)
  is the data-availability check — "do we have the upstream rows
  this job needs?" Each job declares its own.
* **Bootstrap gate** (this module) is the install-state check —
  "is the first-install bootstrap complete, partial-error, or
  cancelled?" Identical for every gated job.

Order: gate first, then prereq. If both fail, gate wins — the
operator's actionable signal is "bootstrap not complete", not "no
coverage rows". The gate is what the operator can fix
(retry/iterate the bootstrap); a downstream prereq failure during a
half-installed bootstrap is noise.

## Override semantics

Manual triggers may override the gate via the ``{control:
{override_bootstrap_gate: true}}`` envelope. When the override
fires:

1. A ``decision_audit`` row is written with ``stage='bootstrap_gate_override'``,
   ``pass_fail='OVERRIDE'``, and an explanation including the
   ``invocation_path`` + ``operator_id``.
2. The gate returns ``(True, '')`` — the run proceeds.

Codex round-2 WARNING addressed: the audit row fires ONLY when the
override actually bypasses the gate. A happy-path ``status='complete'``
run never writes an audit row (otherwise every successful run
generates noise that drowns the actual override events).

Scheduled fires CANNOT override — there is no operator at the
keyboard for a cron tick. ``invocation_path='scheduled'`` ignores
``override_present`` entirely.

## Why a dedicated module

The gate has three distinct callers (scheduled-fire wrapper,
queue-dispatch wrapper, catch-up loop) and one decision_audit
write. Keeping the logic in one place prevents drift between paths
— each caller passes its own ``invocation_path`` literal, and the
gate handles the rest identically.

The module reads ``bootstrap_state``; it does NOT mutate. The
read-only contract means callers can supply any psycopg connection
(autocommit or transactional) without changing semantics.
"""

from __future__ import annotations

import logging
from typing import Any, Literal
from uuid import UUID

import psycopg

from app.services.bootstrap_state import read_state

logger = logging.getLogger(__name__)


GateInvocationPath = Literal["scheduled", "manual_queue"]
"""Which dispatch path is consulting the gate.

* ``scheduled`` — APScheduler fire or boot-time catch-up. Override
  is meaningless here; ``override_present`` is ignored.
* ``manual_queue`` — durable queue-consumer dispatch
  (``app.jobs.listener``). Override-aware.
"""


_GATE_REASON_BOOTSTRAP_NOT_COMPLETE: Literal["bootstrap_not_complete"] = "bootstrap_not_complete"
"""Reason key surfaced to operator UI via ``processStatus.ts::REASON_TOOLTIP``."""


def check_bootstrap_state_gate(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
    invocation_path: GateInvocationPath,
    override_present: bool,
    operator_id: UUID | str | None = None,
) -> tuple[bool, str]:
    """Decide whether ``job_name`` may run given ``bootstrap_state.status``.

    Returns ``(allowed, reason)``:

    * ``(True, "")`` — bootstrap is complete OR a manual override
      bypassed the gate. On override, a ``decision_audit`` row is
      written before returning.
    * ``(False, "bootstrap_not_complete")`` — gate blocks the run.
      Caller's responsibility to surface this through the
      appropriate skip path:

      - scheduled fire / catch-up → ``record_job_skip(conn, job_name, reason)``
        BEFORE entering ``_tracked_job`` (prevention-log L791).
      - manual queue dispatch → ``mark_request_rejected(conn,
        request_id, error_msg=reason)`` (prevention-log L1202;
        data-engineer skill §6.5.7 step 8 — NEVER
        ``mark_request_completed`` for skipped runs).

    The connection is consulted in two ways:

    1. ``read_state(conn)`` — single SELECT on the singleton.
    2. ``INSERT INTO decision_audit`` (only when the override
       actually bypasses).

    Connections passed by scheduled-fire callers run in autocommit
    mode (matching ``record_job_skip``'s contract); the override
    INSERT relies on autocommit semantics so the audit row is
    visible immediately. Manual-queue callers should also use
    autocommit so a rejection-write race cannot strand the audit
    row inside an uncommitted transaction.
    """
    state = read_state(conn)
    if state.status == "complete":
        return (True, "")

    # status is one of {'pending', 'running', 'partial_error', 'cancelled'}
    # — every non-complete value blocks scheduled fires unconditionally
    # and blocks manual-queue dispatches unless the override flag is set.
    if invocation_path == "manual_queue" and override_present:
        _write_override_audit(
            conn,
            job_name=job_name,
            current_status=state.status,
            operator_id=operator_id,
        )
        logger.info(
            "bootstrap_gate: manual override granted for %r (status=%s, operator=%s)",
            job_name,
            state.status,
            operator_id,
        )
        return (True, "")

    return (False, _GATE_REASON_BOOTSTRAP_NOT_COMPLETE)


def _write_override_audit(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
    current_status: str,
    operator_id: UUID | str | None,
) -> None:
    """Record the bypass in ``decision_audit`` for the operator audit trail.

    Single INSERT, no return value. The connection's commit posture
    (autocommit vs. transactional) is the caller's choice; the
    contract is "row is visible by the time the gate returns
    ``(True, '')``" — autocommit satisfies that automatically.

    A failure here does NOT raise: operator-action audit is desired
    but not load-bearing for the run itself, and we have already
    decided to allow the run. The log line surfaces the failure for
    ops_monitor.
    """
    explanation = (
        f"manual override of bootstrap_state gate for job {job_name!r}: "
        f"bootstrap_state.status was {current_status!r}; "
        f"operator override bypassed the gate"
    )
    operator_label = str(operator_id) if operator_id is not None else "<unknown>"
    try:
        conn.execute(
            """
            INSERT INTO decision_audit
                (decision_time, stage, pass_fail, explanation, evidence_json)
            VALUES
                (NOW(), 'bootstrap_gate_override', 'OVERRIDE', %(expl)s, %(evidence)s::jsonb)
            """,
            {
                "expl": explanation,
                "evidence": _encode_evidence(
                    {
                        "job_name": job_name,
                        "bootstrap_status": current_status,
                        "operator_id": operator_label,
                    }
                ),
            },
        )
    except Exception:
        logger.exception(
            "bootstrap_gate: failed to write override audit row for %r — proceeding anyway",
            job_name,
        )


def _encode_evidence(payload: dict[str, str]) -> str:
    """Hand-roll the JSON payload for the evidence_json column.

    ``json.dumps`` is enough — the dict is shallow and the values
    are all strings. Avoiding ``Jsonb()`` keeps this module's
    psycopg dependency minimal (helpful for unit tests that mock
    the connection).
    """
    import json

    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


__all__ = [
    "GateInvocationPath",
    "check_bootstrap_state_gate",
]
