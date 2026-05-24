"""Sidecar repair runbook (#1233 Stream A spec §17).

Rebuilds ``sec_cik_submissions_files_index`` rows from an on-disk
``submissions.zip`` archive WITHOUT re-fetching SEC. Re-uses
``refresh_cik_sidecar`` (the same writer S8 uses), so semantics match
exactly: agent CIKs filtered at the writer layer, sentinel row written
for zero-overflow CIKs, per-CIK DELETE + INSERT under a per-CIK
transaction.

Operator usage::

    EBULL_ENV=dev python -m app.runbooks.stream_a_t13_sidecar_repair \\
        --archive-path /path/to/submissions.zip \\
        [--cik 0000320193] \\
        [--bootstrap-run-id 17] \\
        --apply

Default mode is dry-run (planned actions printed; no DB writes). The
``--apply`` flag is REQUIRED to actually mutate rows. When
``--bootstrap-run-id`` is set, repaired rows carry that bootstrap-run
lineage + ``populate_origin='bootstrap'``; otherwise NULL run id +
``populate_origin='steady_state'`` (existing default; PR-D F8 fold).

Spec: ``docs/proposals/etl/stream-a-run-8-fixes.md`` v2.4 §17.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import zipfile
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.runbooks.safety import (
    RunbookRefused,
    assert_dev_db,
    assert_dev_db_name_in_url,
    assert_dev_env,
    assert_jobs_process_stopped,
)
from app.services.sec_submissions_ingest import repair_cik_sidecar_from_archive

LOG_DIR: Path = Path("var/runbooks")


def _count_archive_entries(archive_path: Path, *, cik: str | None) -> int:
    """Cheap dry-run preview: count matching ``CIK<10>.json`` entries."""
    if not archive_path.exists():
        raise RunbookRefused(f"--archive-path {archive_path!s} does not exist.")
    with zipfile.ZipFile(archive_path) as zf:
        names = zf.namelist()
    import re

    cik_re = re.compile(r"^CIK(\d{10})\.json$")
    matches = 0
    for name in names:
        m = cik_re.match(name)
        if m is None:
            continue
        if cik is not None and m.group(1) != cik:
            continue
        matches += 1
    return matches


def _write_log_jsonl(envelope: dict[str, Any]) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    path = LOG_DIR / f"stream_a_t13_sidecar_repair-{ts}.jsonl"
    with path.open("a") as fh:
        fh.write(json.dumps(envelope) + "\n")
    return path


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns process exit code (0/1/2)."""
    parser = argparse.ArgumentParser(
        prog="stream_a_t13_sidecar_repair",
        description=(
            "Rebuild sec_cik_submissions_files_index from a local "
            "submissions.zip archive without re-fetching SEC "
            "(#1233 Stream A spec §17)."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute repair. Without this flag, dry-run plan only.",
    )
    parser.add_argument(
        "--cik",
        type=str,
        default=None,
        help="Specific 10-digit padded CIK to repair; default = all entries.",
    )
    parser.add_argument(
        "--bootstrap-run-id",
        type=int,
        default=None,
        help=(
            "If set, stamp repaired rows with this bootstrap_run_id + "
            "populate_origin='bootstrap'. Else NULL + 'steady_state'."
        ),
    )
    parser.add_argument(
        "--archive-path",
        type=Path,
        default=None,
        help=(
            "Path to submissions.zip on disk. REQUIRED with --apply; "
            "optional in dry-run (will skip the entry-count preview if "
            "absent)."
        ),
    )
    args = parser.parse_args(argv)

    try:
        assert_dev_env()
        assert_dev_db_name_in_url()
    except RunbookRefused as exc:
        print(exc.msg, file=sys.stderr)
        return 2

    if not args.apply:
        plan: dict[str, Any] = {
            "mode": "dry-run",
            "cik": args.cik,
            "bootstrap_run_id": args.bootstrap_run_id,
            "archive_path": str(args.archive_path) if args.archive_path else None,
        }
        if args.archive_path is not None:
            try:
                plan["entries_would_process"] = _count_archive_entries(args.archive_path, cik=args.cik)
            except RunbookRefused as exc:
                print(exc.msg, file=sys.stderr)
                return 2
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0

    if args.archive_path is None:
        print(
            "REFUSE: --archive-path is required with --apply.",
            file=sys.stderr,
        )
        return 2
    if not args.archive_path.exists():
        print(
            f"REFUSE: --archive-path {args.archive_path!s} does not exist.",
            file=sys.stderr,
        )
        return 2

    try:
        with psycopg.connect(settings.database_url) as conn:
            try:
                assert_dev_db(conn)
                assert_jobs_process_stopped(settings.database_url)
            except RunbookRefused as exc:
                print(exc.msg, file=sys.stderr)
                return 2
            try:
                telemetry = repair_cik_sidecar_from_archive(
                    conn,
                    archive_path=args.archive_path,
                    cik=args.cik,
                    bootstrap_run_id=args.bootstrap_run_id,
                )
            except Exception as exc:
                print(
                    f"REPAIR FAILED: {exc}\n"
                    f"RECOVERY: re-run with the same arguments after "
                    f"investigating. The repair helper is idempotent "
                    f"per CIK (DELETE+INSERT). If the archive is "
                    f"corrupted, re-download submissions.zip via the "
                    f"S8 path.",
                    file=sys.stderr,
                )
                return 1
    except psycopg.Error as exc:
        print(f"DB error: {exc}", file=sys.stderr)
        return 2

    envelope: dict[str, Any] = {
        "schema_version": 1,
        "runbook": "stream_a_t13_sidecar_repair",
        "cik": args.cik,
        "bootstrap_run_id": args.bootstrap_run_id,
        "archive_path": str(args.archive_path),
        "telemetry": telemetry,
        "exit_code": 0,
    }
    rendered = json.dumps(envelope, indent=2, sort_keys=True)
    print(rendered)
    log_path = _write_log_jsonl(envelope)
    print(f"# JSONL log: {log_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
