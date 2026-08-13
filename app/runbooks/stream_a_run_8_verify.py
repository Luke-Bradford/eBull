"""Run #8 verification runbook (#1233 Stream A spec §17).

End-to-end orchestrator for re-bootstrapping the dev database and
capturing per-stage wall-clock timings. The DESTRUCTIVE path (under
``--apply``) drops + recreates ``ebull_dev``, re-runs migrations, posts
``/auth/setup`` with a freshly-generated random operator password,
dispatches ``/system/bootstrap/run``, waits for the operator to start
the jobs service, then polls ``/system/bootstrap-status`` until terminal
or the 90-min cap.

Operator usage::

    EBULL_ENV=dev python -m app.runbooks.stream_a_run_8_verify --apply

Default mode is dry-run (planned actions printed; no DB writes; no
HTTP). ``--apply`` is REQUIRED to mutate.

Spec: ``docs/proposals/etl/stream-a-run-8-fixes.md`` v2.4 §17.

DESTRUCTIVE SEQUENCE NOTE
-------------------------
Steps 4-7 (cancel, drop, create, migrate, setup, dispatch) run while
holding ``JOBS_PROCESS_LOCK_KEY`` on the application DB. PG advisory
locks are PER-DATABASE in PG 9.0+ (NOT cluster-wide — empirically
confirmed in
``tests/test_jobs_process_probe_fence.py::test_per_database_isolation_regression_gate``).
The fence connection DIES with ``DROP DATABASE ebull_dev``; the runbook
re-acquires the fence on the FRESH DB after migrations complete. The
TOCTOU window during drop-and-create is unavoidable at the lock layer
alone — operator MUST keep the jobs service stopped (e.g. ``systemctl
stop ebull-jobs``, not just SIGINT) for the duration. Three probe
checkpoints raise the alarm if the jobs process appears mid-run.

§6.3 PRE-WIPE PROCEDURE
-----------------------
If the current dev DB has been touched by ``pg_resetwal`` (recovery from
crash/data-corruption), its catalog may carry stale ``multixact``
state that the next DROP cannot reclaim. Symptoms:
``job_runtime_heartbeat`` + ``broker_credentials`` show wraparound-class
SQLSTATE errors mid-bootstrap. The §6.3 procedure in
``docs/specs/etl/retention-rubric.md`` covers reclaim ordering.

This runbook calls ``assert_no_multixact_wraparound(conn)`` immediately
after ``assert_dev_db(conn)`` as a pre-flight refusal — wraparound
proximity raises ``RunbookRefused`` with exit code 2 BEFORE any DROP.
Operator action on refusal: run §6.3, then re-invoke this runbook.

Exit codes
----------
* ``0`` — bootstrap reached terminal status (success path).
* ``1`` — gate-side failure (DROP retried twice + still 55006, etc.).
* ``2`` — invalid input / refused precondition (env guards, missing
  ``--apply``, drift detected mid-poll).
* ``3`` — CRITICAL: concurrent bootstrap detected (foreign ``run_id``
  observed in poll); data-corruption risk; foreign run NOT cancelled.
"""

from __future__ import annotations

import argparse
import json
import secrets
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx
import psycopg
from psycopg import sql

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, acquire_jobs_process_fence
from app.runbooks.safety import (
    RunbookRefused,
    assert_dev_db,
    assert_dev_db_name_in_url,
    assert_dev_env,
    assert_jobs_process_stopped,
    assert_no_multixact_wraparound,
    parse_db_name_from_url,
    prune_old_runbook_logs,
    wait_for_jobs_process_started,
)

DEFAULT_API_BASE: str = "http://127.0.0.1:8000"
DEFAULT_TIMEOUT_MIN: int = 90
DEFAULT_POLL_SEC: int = 30
DEFAULT_WAIT_FOR_JOBS_SEC: int = 1800
LOG_DIR: Path = Path("var/runbooks")


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _postgres_url() -> str:
    """Sibling URL with path ``/postgres`` for admin operations.

    DROP / CREATE DATABASE cannot run while connected to the target
    DB. We must connect to a sibling DB (conventionally ``postgres``)
    to issue them.
    """
    parsed = urlparse(settings.database_url)
    return urlunparse(parsed._replace(path="/postgres"))


def _generate_password() -> str:
    """Random 32-char password via ``secrets.token_urlsafe(24)``.

    Per operator decision in PR-D plan v3 review: NO env vars; runbook
    generates fresh credentials on every ``--apply`` and prints to
    stdout once with red banner. Never persists plaintext.
    """
    return secrets.token_urlsafe(24)


def _print_password_banner(password: str) -> None:
    bar = "=" * 78
    red = "\033[91m"
    reset = "\033[0m"
    print(f"\n{red}{bar}{reset}", flush=True)
    print(f"{red}  OPERATOR PASSWORD (record this — printed only once)  {reset}", flush=True)
    print(f"{red}    username: operator{reset}", flush=True)
    print(f"{red}    password: {password}{reset}", flush=True)
    print(f"{red}{bar}{reset}\n", flush=True)


def _drop_and_create_db(*, postgres_url: str, target_db: str, log: list[dict[str, Any]]) -> None:
    """Terminate other backends + DROP + CREATE. Retry once on 55006.

    Spec v2.4 §17 step 5: terminate other sessions on ``target_db``,
    sleep 2s, DROP. On 55006 (object in use), terminate again, sleep
    5s, DROP. On second 55006, raise RunbookRefused with
    ``pg_stat_activity`` rows for operator triage.
    """
    with psycopg.connect(postgres_url, autocommit=True) as admin:
        for attempt in (1, 2):
            admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                (target_db,),
            )
            sleep_sec = 2 if attempt == 1 else 5
            time.sleep(sleep_sec)
            try:
                admin.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(target_db)))
                log.append({"step": "drop_db", "attempt": attempt, "status": "ok"})
                break
            except psycopg.errors.ObjectInUse as exc:
                log.append(
                    {
                        "step": "drop_db",
                        "attempt": attempt,
                        "status": "55006_object_in_use",
                        "error": str(exc),
                    }
                )
                if attempt == 2:
                    rows = admin.execute(
                        "SELECT pid, application_name, client_addr, state, "
                        "query_start, query FROM pg_stat_activity "
                        "WHERE datname = %s",
                        (target_db,),
                    ).fetchall()
                    detail = "\n".join(repr(r) for r in rows)
                    raise RunbookRefused(
                        f"DROP DATABASE {target_db!r} blocked twice with 55006. "
                        f"RECOVERY: terminate the holders below + re-run.\n"
                        f"pg_stat_activity:\n{detail}"
                    ) from exc

        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db)))
        log.append({"step": "create_db", "status": "ok"})


def _run_migrations(log: list[dict[str, Any]]) -> None:
    """In-process ``run_migrations()`` (v2.4 fold of Codex re-pass IMP 4)."""
    from app.db.migrations import run_migrations

    applied = run_migrations()
    log.append(
        {
            "step": "migrations",
            "applied_count": len(applied),
            "applied": applied if len(applied) <= 50 else applied[:50] + ["..."],
        }
    )


def _post_auth_setup(
    *,
    client: httpx.Client,
    api_base: str,
    username: str,
    password: str,
    log: list[dict[str, Any]],
) -> None:
    """POST /auth/setup — sets session cookie on the client."""
    resp = client.post(
        f"{api_base}/auth/setup",
        json={"username": username, "password": password},
        timeout=30.0,
    )
    log.append(
        {
            "step": "auth_setup",
            "status_code": resp.status_code,
            "cookies_count": len(resp.cookies),
        }
    )
    if resp.status_code != 200:
        raise RunbookRefused(f"/auth/setup returned {resp.status_code}: {resp.text[:500]}")


def _post_bootstrap_cancel(
    *,
    client: httpx.Client,
    api_base: str,
    log: list[dict[str, Any]],
) -> None:
    """POST /system/bootstrap/cancel — FIRE-AND-FORGET (v2.4 fold).

    With jobs stopped (pre-flight gate), no orchestrator observes
    cancel_requested_at, so polling-until-idle is pointless. Any
    'running' row vanishes with the DROP DATABASE in the next step.
    """
    try:
        resp = client.post(f"{api_base}/system/bootstrap/cancel", timeout=10.0)
        log.append(
            {
                "step": "bootstrap_cancel_fire_and_forget",
                "status_code": resp.status_code,
            }
        )
    except httpx.HTTPError as exc:
        log.append(
            {
                "step": "bootstrap_cancel_fire_and_forget",
                "status_code": None,
                "error": str(exc),
            }
        )


def _post_bootstrap_run(
    *,
    client: httpx.Client,
    api_base: str,
    log: list[dict[str, Any]],
) -> int:
    """POST /system/bootstrap/run — returns the queued run_id."""
    resp = client.post(f"{api_base}/system/bootstrap/run", json={}, timeout=30.0)
    log.append(
        {
            "step": "bootstrap_run",
            "status_code": resp.status_code,
        }
    )
    if resp.status_code != 202:
        raise RunbookRefused(f"/system/bootstrap/run returned {resp.status_code}: {resp.text[:500]}")
    body = resp.json()
    run_id = int(body["run_id"])
    log.append({"step": "bootstrap_run_captured", "run_id": run_id, "body": body})
    return run_id


def _poll_with_retry(
    *,
    client: httpx.Client,
    api_base: str,
    captured_run_id: int,
    timeout_sec: int,
    poll_sec: int,
    log: list[dict[str, Any]],
) -> int:
    """Poll /system/bootstrap-status until terminal or timeout.

    Returns exit code (0 success / 2 timeout / 3 drift CRITICAL).
    """
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        body = _poll_once_with_retry(client=client, api_base=api_base)
        observed_run_id = body.get("current_run_id")
        if observed_run_id is None:
            log.append(
                {
                    "step": "poll",
                    "current_run_id": None,
                    "detail": "no current_run_id yet",
                }
            )
        elif int(observed_run_id) != captured_run_id:
            log.append(
                {
                    "step": "poll_drift",
                    "captured_run_id": captured_run_id,
                    "observed_run_id": int(observed_run_id),
                    "severity": "CRITICAL",
                }
            )
            print(
                f"CRITICAL: concurrent bootstrap detected. "
                f"captured run_id={captured_run_id} but observed "
                f"current_run_id={observed_run_id}. Likely data corruption "
                f"in bootstrap_runs. DO NOT start a third run; investigate "
                f"before proceeding. Foreign run NOT cancelled.",
                file=sys.stderr,
            )
            return 3
        else:
            state_status = body.get("state_status")
            log.append(
                {
                    "step": "poll",
                    "current_run_id": int(observed_run_id),
                    "state_status": state_status,
                }
            )
            if state_status in ("complete", "partial_error"):
                stages = body.get("stages") or []
                log.append(
                    {
                        "step": "terminal",
                        "state_status": state_status,
                        "stage_count": len(stages),
                    }
                )
                return 0
        time.sleep(poll_sec)

    log.append({"step": "timeout", "elapsed_sec": timeout_sec})
    print(
        f"TIMEOUT after {timeout_sec}s. bootstrap run_id={captured_run_id} "
        f"still in flight. To cancel: "
        f"curl -X POST {api_base}/system/bootstrap/cancel. "
        f"To check: curl {api_base}/system/bootstrap-status.",
        file=sys.stderr,
    )
    return 2


def _poll_once_with_retry(
    *,
    client: httpx.Client,
    api_base: str,
    retries: int = 3,
    backoff_sec: int = 5,
) -> dict[str, Any]:
    """Single /bootstrap-status GET with bounded retry on transient errors.

    PR-D Operator O7 fold: API restart blip should not cause exit 2.
    """
    last_exc: Exception | None = None
    for _attempt in range(retries):
        try:
            resp = client.get(f"{api_base}/system/bootstrap-status", timeout=10.0)
            if resp.status_code == 502:
                last_exc = httpx.HTTPStatusError(
                    "502 from /bootstrap-status",
                    request=resp.request,
                    response=resp,
                )
                time.sleep(backoff_sec)
                continue
            if resp.status_code != 200:
                # Surface non-502, non-200 as a hard fail.
                raise RunbookRefused(f"/system/bootstrap-status returned {resp.status_code}: {resp.text[:300]}")
            return resp.json()
        except (httpx.ConnectError, httpx.ReadTimeout) as exc:
            last_exc = exc
            time.sleep(backoff_sec)
    if last_exc is not None:
        raise RunbookRefused(f"/system/bootstrap-status: {retries} retries exhausted (last error: {last_exc})")
    raise RunbookRefused("/system/bootstrap-status: unreachable")


def _write_log_jsonl(envelope: dict[str, Any]) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    prune_old_runbook_logs(LOG_DIR)  # #1328 — bound dev-local log growth
    ts = int(time.time())
    run_id = envelope.get("captured_run_id") or "queued"
    path = LOG_DIR / f"stream_a_run_8_verify-{run_id}-{ts}.jsonl"
    with path.open("a") as fh:
        fh.write(json.dumps(envelope, default=str) + "\n")
    return path


def build_parser() -> argparse.ArgumentParser:
    """Construct the argparse parser. Extracted from main() so tests can
    inspect argparse defaults without invoking main() (#1327)."""
    parser = argparse.ArgumentParser(
        prog="stream_a_run_8_verify",
        description=(
            "Run #8 verification (#1233 Stream A §17): drop dev DB, "
            "re-run migrations, dispatch bootstrap, wait for operator "
            "to start jobs, poll until terminal."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute destructive Run #8 verify. Without this flag, dry-run plan only.",
    )
    parser.add_argument("--api-base", type=str, default=DEFAULT_API_BASE)
    parser.add_argument(
        "--timeout-min",
        type=int,
        default=DEFAULT_TIMEOUT_MIN,
        help=f"Total poll budget in minutes (default {DEFAULT_TIMEOUT_MIN}).",
    )
    parser.add_argument(
        "--poll-sec",
        type=int,
        default=DEFAULT_POLL_SEC,
        help=f"Seconds between /bootstrap-status polls (default {DEFAULT_POLL_SEC}).",
    )
    parser.add_argument(
        "--wait-for-jobs-sec",
        type=int,
        default=DEFAULT_WAIT_FOR_JOBS_SEC,
        help=(
            "After dispatch, seconds to wait for operator to start the "
            f"jobs process (default {DEFAULT_WAIT_FOR_JOBS_SEC})."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns process exit code (0/1/2/3)."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        assert_dev_env()
        assert_dev_db_name_in_url()
    except RunbookRefused as exc:
        print(exc.msg, file=sys.stderr)
        return 2

    started_at = _now_iso()
    log_entries: list[dict[str, Any]] = []
    captured_run_id: int | None = None
    exit_code = 0

    if not args.apply:
        plan = {
            "mode": "dry-run",
            "api_base": args.api_base,
            "timeout_min": args.timeout_min,
            "would_execute": [
                "assert_dev_env",
                "assert_dev_db",
                "assert_jobs_process_stopped",
                "POST /system/bootstrap/cancel (fire-and-forget)",
                "acquire_jobs_process_fence on application DB",
                "DROP + CREATE ebull_dev (via 'postgres' admin DB)",
                "app.db.migrations.run_migrations()",
                "re-acquire fence on fresh ebull_dev",
                "POST /auth/setup with random 32-char password (printed once with red banner)",
                "POST /system/bootstrap/run -> capture run_id",
                f"release fence; wait for jobs process to start (timeout {args.wait_for_jobs_sec}s)",
                f"poll /system/bootstrap-status every {args.poll_sec}s (total {args.timeout_min}min)",
            ],
        }
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0

    try:
        with psycopg.connect(settings.database_url) as conn:
            try:
                assert_dev_db(conn)
                assert_no_multixact_wraparound(conn)
            except RunbookRefused as exc:
                print(exc.msg, file=sys.stderr)
                return 2

        try:
            assert_jobs_process_stopped(settings.database_url)
        except RunbookRefused as exc:
            print(exc.msg, file=sys.stderr)
            return 2

        with httpx.Client(follow_redirects=True) as http:
            _post_bootstrap_cancel(client=http, api_base=args.api_base, log=log_entries)

            # N5 fold (post-final-committee polish): use shared helper
            # from safety.py instead of inlining urlparse(...).path.lstrip("/").
            # Keeps URL-parse logic in one place — Architect IMP-1 fold.
            target_db = parse_db_name_from_url(settings.database_url)
            postgres_url = _postgres_url()
            try:
                with acquire_jobs_process_fence(settings.database_url):
                    _drop_and_create_db(
                        postgres_url=postgres_url,
                        target_db=target_db,
                        log=log_entries,
                    )
                # The fence conn died with DROP DATABASE — exit the
                # context cleanly (psycopg unlock will no-op).
            except JobAlreadyRunning:
                print(
                    "REFUSE: could not acquire JOBS_PROCESS_LOCK_KEY fence "
                    "on application DB. The jobs process started between "
                    "the pre-flight probe and the fence acquire.",
                    file=sys.stderr,
                )
                return 2
            except RunbookRefused as exc:
                print(exc.msg, file=sys.stderr)
                return 1

            _run_migrations(log_entries)

            # Re-acquire fence on the FRESH application DB for the
            # remaining setup + dispatch window.
            try:
                with acquire_jobs_process_fence(settings.database_url):
                    password = _generate_password()
                    _post_auth_setup(
                        client=http,
                        api_base=args.api_base,
                        username="operator",
                        password=password,
                        log=log_entries,
                    )
                    _print_password_banner(password)

                    captured_run_id = _post_bootstrap_run(client=http, api_base=args.api_base, log=log_entries)
            except JobAlreadyRunning:
                print(
                    "REFUSE: could not re-acquire fence on fresh ebull_dev. "
                    "The jobs process started during the drop+create+migrate "
                    "window. RECOVERY: stop the jobs process, then re-run.",
                    file=sys.stderr,
                )
                return 2

            print(
                f"\n--- Bootstrap dispatched (run_id={captured_run_id}). Start the jobs process now ---",
                flush=True,
            )

            try:
                wait_for_jobs_process_started(
                    settings.database_url,
                    timeout_sec=args.wait_for_jobs_sec,
                )
            except RunbookRefused as exc:
                print(exc.msg, file=sys.stderr)
                return 2

            exit_code = _poll_with_retry(
                client=http,
                api_base=args.api_base,
                captured_run_id=captured_run_id,
                timeout_sec=args.timeout_min * 60,
                poll_sec=args.poll_sec,
                log=log_entries,
            )
    except psycopg.Error as exc:
        print(f"DB error: {exc}", file=sys.stderr)
        return 1

    envelope: dict[str, Any] = {
        "schema_version": 1,
        "runbook": "stream_a_run_8_verify",
        "captured_run_id": captured_run_id,
        "api_base": args.api_base,
        "started_at": started_at,
        "ended_at": _now_iso(),
        "log": log_entries,
        "exit_code": exit_code,
    }
    log_path = _write_log_jsonl(envelope)
    print(f"# JSONL log: {log_path}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
