"""Ingest-sweep → ProcessRow adapter — STUB for PR3.

Issue #1071 (umbrella #1064).
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §Adapter map / ingest_sweep + §PR3 / §PR6.

PR3 ships an empty stub so the snapshot handler can call every adapter
unconditionally. PR6 fills it from ``sec_filing_manifest`` aggregates +
``data_freshness_index`` aggregates + the per-source ingest logs
(``institutional_holdings_ingest_log``, ``n_port_ingest_log`` …).

The ``sync_layer`` mechanism is explicitly purged from v1
(Codex round 2 R2-W4). Sync orchestrator surfaces as the
``orchestrator_full_sync`` scheduled_job row; per-layer drill-in is
PR6.
"""

from __future__ import annotations

from typing import Any

import psycopg

from app.services.processes import ErrorClassSummary, ProcessRow, ProcessRunSummary


def list_rows(conn: psycopg.Connection[Any]) -> list[ProcessRow]:
    """STUB — PR6 fills this from sec_filing_manifest + data_freshness_index."""
    return []


def get_row(conn: psycopg.Connection[Any], *, process_id: str) -> ProcessRow | None:
    """STUB — PR6 wires per-source rows."""
    return None


def list_runs(conn: psycopg.Connection[Any], *, process_id: str, days: int) -> list[ProcessRunSummary]:
    """STUB — PR6 wires per-source manifest run history."""
    return []


def list_run_errors(conn: psycopg.Connection[Any], *, process_id: str, run_id: int) -> tuple[ErrorClassSummary, ...]:
    """STUB — PR6 wires per-source manifest error grouping."""
    return ()


__all__ = ["get_row", "list_run_errors", "list_rows", "list_runs"]
