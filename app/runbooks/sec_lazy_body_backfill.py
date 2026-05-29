"""Lazy-body force-drain runbook (#1343 spec §17).

Operator escape hatch: flip deferred ``sec_10k`` / ``sec_8k`` manifest
rows back to ``'pending'`` so the steady-state manifest worker eagerly
fetches their bodies — for when lazy-on-view isn't enough (warming a
demo, or backfilling a cohort ahead of a thesis run). #1343 defers
10-K Item 1 + 8-K item bodies out of bootstrap (S16 seeds the manifest
row ``'deferred'``; bodies fill on first user view); this runbook
re-queues a deferred cohort for the worker to fill eagerly.

Operator usage::

    EBULL_ENV=dev python -m app.runbooks.sec_lazy_body_backfill \\
        --source sec_10k [--instrument 12345] --apply

Default mode is dry-run (counts deferred rows; no writes). ``--apply``
flips them to ``'pending'``. Unlike most runbooks, the jobs service
should be RUNNING — the manifest worker is what then drains the
re-queued rows. A flip while ``bootstrap_state`` is not ``'complete'``
is a no-op (the worker is gated during bootstrap) — refused (exit 2).

Spec: ``docs/proposals/etl/1343-s18-s21-lazy-on-click.md`` §17.

Exit codes: ``0`` ok / ``1`` DB error / ``2`` refused precondition.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.runbooks.safety import (
    RunbookRefused,
    assert_dev_db,
    assert_dev_db_name_in_url,
    assert_dev_env,
)

LOG_DIR: Path = Path("var/runbooks")
_SOURCES: tuple[str, ...] = ("sec_10k", "sec_8k")


def _count_deferred(conn: psycopg.Connection[Any], *, source: str, instrument_id: int | None) -> int:
    sql = "SELECT COUNT(*) FROM sec_filing_manifest WHERE source = %(src)s AND ingest_status = 'deferred'"
    params: dict[str, Any] = {"src": source}
    if instrument_id is not None:
        sql += " AND instrument_id = %(iid)s"
        params["iid"] = instrument_id
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return int(row[0]) if row is not None else 0


def _bootstrap_complete(conn: psycopg.Connection[Any]) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM bootstrap_state WHERE id = 1")
        row = cur.fetchone()
    return row is not None and str(row[0]) == "complete"


def _flip_deferred_to_pending(conn: psycopg.Connection[Any], *, source: str, instrument_id: int | None) -> int:
    sql = (
        "UPDATE sec_filing_manifest SET ingest_status = 'pending' WHERE source = %(src)s AND ingest_status = 'deferred'"
    )
    params: dict[str, Any] = {"src": source}
    if instrument_id is not None:
        sql += " AND instrument_id = %(iid)s"
        params["iid"] = instrument_id
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


def _write_log_jsonl(envelope: dict[str, Any]) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    path = LOG_DIR / f"sec_lazy_body_backfill-{ts}.jsonl"
    with path.open("a") as fh:
        fh.write(json.dumps(envelope) + "\n")
    return path


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns process exit code (0/1/2)."""
    parser = argparse.ArgumentParser(
        prog="sec_lazy_body_backfill",
        description=(
            "Force-drain deferred 10-K/8-K bodies: flip deferred manifest "
            "rows to pending so the steady-state worker fetches them "
            "eagerly (#1343 spec §17)."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Flip deferred→pending. Without this flag, dry-run counts only.",
    )
    parser.add_argument(
        "--source",
        choices=_SOURCES,
        required=True,
        help="Which deferred source to force-drain (sec_10k or sec_8k).",
    )
    parser.add_argument(
        "--instrument",
        type=int,
        default=None,
        help="Scope to one instrument_id; default = all deferred rows for the source.",
    )
    args = parser.parse_args(argv)

    try:
        assert_dev_env()
        assert_dev_db_name_in_url()
    except RunbookRefused as exc:
        print(exc.msg, file=sys.stderr)
        return 2

    try:
        with psycopg.connect(settings.database_url) as conn:
            try:
                assert_dev_db(conn)
            except RunbookRefused as exc:
                print(exc.msg, file=sys.stderr)
                return 2

            deferred = _count_deferred(conn, source=args.source, instrument_id=args.instrument)

            if not args.apply:
                print(
                    json.dumps(
                        {
                            "mode": "dry-run",
                            "source": args.source,
                            "instrument": args.instrument,
                            "deferred_rows": deferred,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 0

            if not _bootstrap_complete(conn):
                print(
                    "REFUSE: bootstrap_state is not 'complete' — the manifest worker is "
                    "gated during bootstrap, so a flip would be a no-op. Re-run after "
                    "bootstrap completes.",
                    file=sys.stderr,
                )
                return 2

            flipped = _flip_deferred_to_pending(conn, source=args.source, instrument_id=args.instrument)
            conn.commit()
    except psycopg.Error as exc:
        print(f"DB error: {exc}", file=sys.stderr)
        return 1

    envelope: dict[str, Any] = {
        "schema_version": 1,
        "runbook": "sec_lazy_body_backfill",
        "source": args.source,
        "instrument": args.instrument,
        "deferred_before": deferred,
        "flipped_to_pending": flipped,
        "exit_code": 0,
    }
    print(json.dumps(envelope, indent=2, sort_keys=True))
    log_path = _write_log_jsonl(envelope)
    print(f"# JSONL log: {log_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
