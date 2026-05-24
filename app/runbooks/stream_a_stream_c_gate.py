"""Stream-C correctness gate runbook (#1233 Stream A spec §1.8).

Runs the 7-check post-Run-#8 acceptance gate (C1-C7) against a
completed bootstrap_run, emits a structured JSON envelope, and persists
``bootstrap_runs.stream_c_gate_status`` so the operator-attestation
state is durably recorded.

Operator usage::

    EBULL_ENV=dev python -m app.runbooks.stream_a_stream_c_gate \\
        --bootstrap-run-id <int> [--strict] [--json-out PATH]

Spec: ``docs/proposals/etl/stream-a-run-8-fixes.md`` v2.4 §1.8 + §17.

IMPORTANT — module-load parser-registry import
----------------------------------------------
``app.services.manifest_parsers`` is imported at module load (BEFORE
``argparse``-time anything else fires) so the side-effect
``register_all_parsers()`` populates the worker's parser registry.
``registered_parser_sources()`` (used by C4) returns the empty
frozenset until that import runs — without this, C4 false-passes
against an empty source set and silently misses every parser drain
gap. PR-D Codex 1 IMPORTANT 10 fold.
"""

from __future__ import annotations

import argparse  # noqa: E402
import json  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402
from datetime import UTC, datetime  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any  # noqa: E402

import psycopg  # noqa: E402
from psycopg import sql  # noqa: E402

# CRITICAL — side-effect-import populates registered_parser_sources().
import app.services.manifest_parsers  # noqa: F401, E402
from app.config import settings  # noqa: E402
from app.jobs.ownership_observations_repair import _CATEGORIES  # noqa: E402
from app.jobs.sec_manifest_worker import registered_parser_sources  # noqa: E402
from app.runbooks.safety import (  # noqa: E402
    RunbookRefused,
    assert_dev_db,
    assert_dev_db_name_in_url,
    assert_dev_env,
)
from app.runbooks.stream_a_stream_c_gate_schema import validate_envelope  # noqa: E402
from app.services.capability_manifest_mapping import (  # noqa: E402
    CATEGORY_TO_MANIFEST_SOURCES,
)

JSON_SCHEMA_VERSION: int = 1
LOG_DIR: Path = Path("var/runbooks")


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _check_record(check_id: str, status: str, count: int, detail: str) -> dict[str, Any]:
    return {"id": check_id, "status": status, "count": count, "detail": detail}


def _get_completed_at(conn: psycopg.Connection[Any], run_id: int) -> datetime:
    """Fetch ``bootstrap_runs.completed_at``; raise if NULL/missing."""
    row = conn.execute("SELECT completed_at FROM bootstrap_runs WHERE id = %s", (run_id,)).fetchone()
    if row is None:
        raise RunbookRefused(f"bootstrap_runs id={run_id} does not exist.")
    if row[0] is None:
        raise RunbookRefused(
            f"bootstrap_runs id={run_id} has NULL completed_at — gate requires "
            f"a completed run (status='complete' OR 'partial_error')."
        )
    return row[0]


def _check_layer_job_fired(
    conn: psycopg.Connection[Any], *, job_name: str, completed_at: datetime
) -> tuple[str, int, str]:
    """C1/C2/C3 shared shape: ≥ 1 successful run started after Run-#8 completed."""
    row = conn.execute(
        "SELECT COUNT(*) FROM job_runs WHERE job_name = %s AND status = 'success' AND started_at > %s",
        (job_name, completed_at),
    ).fetchone()
    count = int(row[0]) if row else 0
    status = "passed" if count >= 1 else "failed"
    detail = (
        f"job_runs WHERE job_name={job_name!r} AND status='success' AND "
        f"started_at>{completed_at.isoformat()} count={count}"
    )
    return status, count, detail


def _check_c4_manifest_drained(conn: psycopg.Connection[Any], *, completed_at: datetime) -> tuple[str, int, str]:
    """C4: every registered parser source has ≥ 1 manifest row drained post-Run-#8."""
    registered = registered_parser_sources()
    if not registered:
        return (
            "failed",
            0,
            "registered_parser_sources() returned empty frozenset — "
            "app.services.manifest_parsers import did not populate the registry.",
        )
    rows = conn.execute(
        "SELECT source, COUNT(*) FROM sec_filing_manifest "
        "WHERE updated_at > %s AND ingest_status IN ('parsed', 'tombstoned') "
        "GROUP BY source",
        (completed_at,),
    ).fetchall()
    drained = {str(row[0]): int(row[1]) for row in rows}
    missing = sorted(registered - drained.keys())
    total_count = sum(drained.values())
    if missing:
        return (
            "failed",
            total_count,
            f"missing drain for sources: {missing} (registered={sorted(registered)}, drained={drained})",
        )
    return (
        "passed",
        total_count,
        f"all {len(registered)} registered sources drained ≥ 1 row (total={total_count})",
    )


def _check_c5_freshness_index_current(conn: psycopg.Connection[Any], *, completed_at: datetime) -> tuple[str, int, str]:
    """C5: data_freshness_index has ≥ 1 row with state='current' updated post-Run-#8.

    Uses ``updated_at`` (real column per sql/120). Spec v2.3 cited
    ``last_seen_at`` — phantom — corrected in v2.4 fold (PR-D Codex 1
    BLOCKING 1).
    """
    row = conn.execute(
        "SELECT COUNT(*) FROM data_freshness_index WHERE updated_at > %s AND state = 'current'",
        (completed_at,),
    ).fetchone()
    count = int(row[0]) if row else 0
    status = "passed" if count >= 1 else "failed"
    detail = f"data_freshness_index WHERE updated_at>{completed_at.isoformat()} AND state='current' count={count}"
    return status, count, detail


def _check_c6_category(
    conn: psycopg.Connection[Any],
    *,
    category: str,
    observations_table: str,
    completed_at: datetime,
) -> tuple[str, int, str]:
    """C6 per-category: ≥ 1 new observation OR fallback quiescence warning.

    7 categories per ``_CATEGORIES`` (insiders, institutions, blockholders,
    treasury, def14a, funds, esop). Treasury maps to ``sec_xbrl_facts``
    (NOT def14a — xbrl_dei source via fundamentals_sync).
    """
    obs_row = conn.execute(
        sql.SQL("SELECT COUNT(*) FROM {table} WHERE ingested_at > %s").format(table=sql.Identifier(observations_table)),
        (completed_at,),
    ).fetchone()
    obs_count = int(obs_row[0]) if obs_row else 0
    if obs_count >= 1:
        return ("passed", obs_count, f"{observations_table} new rows={obs_count}")

    sources = CATEGORY_TO_MANIFEST_SOURCES.get(category, frozenset())
    if not sources:
        return (
            "failed",
            0,
            f"category {category!r} has no CATEGORY_TO_MANIFEST_SOURCES mapping",
        )
    manifest_row = conn.execute(
        "SELECT COUNT(*) FROM sec_filing_manifest WHERE source = ANY(%s) AND filed_at > %s - INTERVAL '24 hours'",
        (list(sources), completed_at),
    ).fetchone()
    manifest_count = int(manifest_row[0]) if manifest_row else 0
    if manifest_count == 0:
        return (
            f"warning_category_quiescent_{category}",
            0,
            f"{observations_table} new rows=0 AND no manifest rows for "
            f"source IN {sorted(sources)} in last 24h "
            f"(per spec §1.8 C6 — DEF 14A / treasury legitimately quiet)",
        )
    return (
        "failed",
        manifest_count,
        f"{observations_table} new rows=0 BUT {manifest_count} manifest rows "
        f"for source IN {sorted(sources)} in last 24h — observations missing",
    )


def _check_c7_sidecar_populated(conn: psycopg.Connection[Any], *, run_id: int) -> tuple[str, int, str]:
    """C7: sidecar populated for every in-universe CIK (sentinel-aware).

    Numerator: distinct CIKs in ``sec_cik_submissions_files_index`` carrying
    ``bootstrap_run_id=run_id``. Sentinel rows COUNT toward populated.
    Denominator: distinct in-universe CIKs minus intersection with
    ``KNOWN_FILING_AGENT_CIKS`` (spec v2.3 §1.8 C7 fold; agent CIKs are
    excluded at the writer layer).
    """
    from app.providers.implementations.sec_edgar import KNOWN_FILING_AGENT_CIKS

    # Numerator: in-universe + tradable CIKs that carry sidecar rows
    # for this run. Restricting via the JOIN (NOT a raw COUNT against
    # sec_cik_submissions_files_index) hardens C7 against any writer
    # that populates the sidecar for out-of-universe CIKs — the
    # production S8 path and the repair runbook both filter via
    # _load_cik_to_instrument, but defence-in-depth here prevents a
    # future writer bug from false-passing the gate. Codex 2 IMPORTANT
    # 1 fold (PR-D pre-push).
    num_row = conn.execute(
        "SELECT COUNT(DISTINCT s.cik) "
        "FROM sec_cik_submissions_files_index s "
        "JOIN external_identifiers ei ON ei.identifier_value = s.cik "
        "JOIN instruments i ON i.instrument_id = ei.instrument_id "
        "WHERE s.bootstrap_run_id = %s "
        "  AND ei.provider = 'sec' "
        "  AND ei.identifier_type = 'cik' "
        "  AND i.is_tradable = TRUE",
        (run_id,),
    ).fetchone()
    populated = int(num_row[0]) if num_row else 0

    universe_row = conn.execute(
        "SELECT COUNT(DISTINCT ei.identifier_value) "
        "FROM external_identifiers ei "
        "JOIN instruments i ON i.instrument_id = ei.instrument_id "
        "WHERE ei.provider = 'sec' "
        "  AND ei.identifier_type = 'cik' "
        "  AND i.is_tradable = TRUE"
    ).fetchone()
    universe = int(universe_row[0]) if universe_row else 0

    if universe == 0:
        return (
            "failed",
            0,
            "in-universe CIK count is 0 (external_identifiers empty?)",
        )

    intersection_row = conn.execute(
        "SELECT COUNT(DISTINCT ei.identifier_value) "
        "FROM external_identifiers ei "
        "JOIN instruments i ON i.instrument_id = ei.instrument_id "
        "WHERE ei.provider = 'sec' "
        "  AND ei.identifier_type = 'cik' "
        "  AND i.is_tradable = TRUE "
        "  AND ei.identifier_value = ANY(%s)",
        (list(KNOWN_FILING_AGENT_CIKS),),
    ).fetchone()
    agent_overlap = int(intersection_row[0]) if intersection_row else 0
    expected = universe - agent_overlap

    if populated >= expected:
        return (
            "passed",
            populated,
            f"sidecar populated for {populated} CIKs (in-universe={universe}, "
            f"agent-overlap={agent_overlap}, expected≥{expected})",
        )
    return (
        "failed",
        populated,
        f"sidecar populated for only {populated} CIKs but expected≥{expected} "
        f"(in-universe={universe}, agent-overlap={agent_overlap})",
    )


def _run_gate(conn: psycopg.Connection[Any], *, run_id: int, started_at_iso: str) -> dict[str, Any]:
    """Execute C1..C7 against ``run_id`` and return the JSON envelope."""
    completed_at = _get_completed_at(conn, run_id)

    checks: list[dict[str, Any]] = []
    accepted = True
    first_failed: str | None = None

    for check_id, job_name in (
        ("c1", "sec_atom_fast_lane"),
        ("c2", "sec_daily_index_reconcile"),
        ("c3", "sec_per_cik_poll"),
    ):
        status, count, detail = _check_layer_job_fired(conn, job_name=job_name, completed_at=completed_at)
        checks.append(_check_record(check_id, status, count, detail))
        if status == "failed":
            accepted = False
            if first_failed is None:
                first_failed = check_id

    status, count, detail = _check_c4_manifest_drained(conn, completed_at=completed_at)
    checks.append(_check_record("c4", status, count, detail))
    if status == "failed":
        accepted = False
        if first_failed is None:
            first_failed = "c4"

    status, count, detail = _check_c5_freshness_index_current(conn, completed_at=completed_at)
    checks.append(_check_record("c5", status, count, detail))
    if status == "failed":
        accepted = False
        if first_failed is None:
            first_failed = "c5"

    for current_table, observations_table, category, _refresh_fn in _CATEGORIES:
        del current_table, _refresh_fn  # only category + observations_table used
        status, count, detail = _check_c6_category(
            conn,
            category=category,
            observations_table=observations_table,
            completed_at=completed_at,
        )
        checks.append(_check_record(f"c6_{category}", status, count, detail))
        if status == "failed":
            accepted = False
            if first_failed is None:
                first_failed = f"c6_{category}"

    status, count, detail = _check_c7_sidecar_populated(conn, run_id=run_id)
    checks.append(_check_record("c7", status, count, detail))
    if status == "failed":
        accepted = False
        if first_failed is None:
            first_failed = "c7"

    payload: dict[str, Any] = {
        "schema_version": JSON_SCHEMA_VERSION,
        "runbook": "stream_a_stream_c_gate",
        "bootstrap_run_id": run_id,
        "started_at": started_at_iso,
        "ended_at": _now_iso(),
        "checks": checks,
        "accepted": accepted,
        "first_failed": first_failed,
    }
    # Validate the envelope shape BEFORE returning. Catches accidental
    # shape drift (new field added here without parallel schema update;
    # wrong type; key rename) at emit-time so a malformed envelope
    # cannot reach the operator's #1233 attestation comment. Pydantic
    # ValidationError propagates as ValueError; the runbook surfaces
    # with exit code 1. See app/runbooks/stream_a_stream_c_gate_schema.py.
    validate_envelope(payload)
    return payload


def _persist_status(conn: psycopg.Connection[Any], *, run_id: int, status_value: str) -> None:
    """UPSERT bootstrap_runs.stream_c_gate_status (CHECK-constrained)."""
    conn.execute(
        "UPDATE bootstrap_runs SET stream_c_gate_status = %s WHERE id = %s",
        (status_value, run_id),
    )


def _write_log_jsonl(envelope: dict[str, Any]) -> Path:
    """Append the envelope to ``var/runbooks/stream_a_stream_c_gate-<id>-<ts>.jsonl``."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    run_id = envelope.get("bootstrap_run_id", "unknown")
    path = LOG_DIR / f"stream_a_stream_c_gate-{run_id}-{ts}.jsonl"
    with path.open("a") as fh:
        fh.write(json.dumps(envelope) + "\n")
    return path


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns process exit code (0/1/2)."""
    parser = argparse.ArgumentParser(
        prog="stream_a_stream_c_gate",
        description=("Stream-C correctness gate (#1233 §1.8) — 7-check C1-C7 post-Run-#8 acceptance gate."),
    )
    parser.add_argument("--bootstrap-run-id", type=int, required=True)
    parser.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Fail-closed on any failed check (default True). "
            "Pass --no-strict to downgrade failures to warnings (exit 0)."
        ),
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="If set, write JSON envelope to PATH instead of stdout.",
    )
    args = parser.parse_args(argv)

    try:
        assert_dev_env()
        assert_dev_db_name_in_url()
    except RunbookRefused as exc:
        print(exc.msg, file=sys.stderr)
        return 2

    started = _now_iso()
    run_id = int(args.bootstrap_run_id)

    try:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            try:
                assert_dev_db(conn)
            except RunbookRefused as exc:
                print(exc.msg, file=sys.stderr)
                return 2

            # Stamp 'pending' at start so concurrent operator inspections
            # don't see stale NULL while the gate runs. PR-D R6 fold.
            _persist_status(conn, run_id=run_id, status_value="pending")

            try:
                envelope = _run_gate(conn, run_id=run_id, started_at_iso=started)
            except RunbookRefused as exc:
                print(exc.msg, file=sys.stderr)
                _persist_status(conn, run_id=run_id, status_value="failed_runbook_crashed")
                return 2
            except Exception:
                # Crash mid-gate — stamp distinct status so operator can
                # triage "the runbook itself broke" vs "a check failed".
                # PR-D O6 fold.
                _persist_status(conn, run_id=run_id, status_value="failed_runbook_crashed")
                raise

            if envelope["accepted"]:
                final_status = "passed"
                exit_code = 0
            else:
                final_status = f"failed_{envelope['first_failed']}"
                exit_code = 1 if args.strict else 0

            envelope["exit_code"] = exit_code
            _persist_status(conn, run_id=run_id, status_value=final_status)
    except psycopg.Error as exc:
        print(f"DB error: {exc}", file=sys.stderr)
        return 2

    rendered = json.dumps(envelope, indent=2, sort_keys=True, default=str)
    if args.json_out is not None:
        args.json_out.write_text(rendered + "\n")
    else:
        print(rendered)

    log_path = _write_log_jsonl(envelope)
    print(f"# JSONL log: {log_path}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
