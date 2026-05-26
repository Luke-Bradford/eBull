"""perf-bench harness: produce EXPLAIN + 3-trial JSON + manifest YAML.

Per-PR plan #1356. Spec: docs/proposals/etl/phase-0-instrumentation.md
§2.6 NEW-A.1 + master plan docs/proposals/etl/bootstrap-sub-1h-plan.md §4.

The shell entry point is scripts/perf_bench/run_explain.sh; this module
holds the logic.

Refusals:
    * EBULL_BENCH_DB_URL unset
    * scripts/perf_bench/<ticket_id>.yaml missing
    * psql not on PATH
    * dirty working tree (artifact SHA must be authoritative)
    * --check-floors-only + manifest row-count < floors.yaml
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Final, NoReturn

import yaml

from scripts.perf_bench._floors import load_floors

REPO_ROOT: Final[Path] = Path(__file__).parent.parent.parent
ARTIFACT_DIR: Final[Path] = REPO_ROOT / "var" / "perf_baselines"
EXPLAIN_FLAGS: Final[str] = "EXPLAIN (ANALYZE, BUFFERS, COSTS, FORMAT TEXT)"

# Bot review iter-3 WARNING fold: ``_row_count`` shells out to ``psql -c
# "SELECT COUNT(*) FROM {table}"`` so ``table`` is interpolated unquoted.
# Even though the input comes from an operator-authored per-ticket YAML
# (not an external network input), we validate it against the same
# identifier shape Postgres expects so a malformed value cannot reach
# ``_psql`` and execute arbitrary SQL against the bench DB.
TABLE_IDENT_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z_][a-z0-9_]*$")


def _err(msg: str) -> NoReturn:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(2)


def _require_env(var: str) -> str:
    val = os.environ.get(var)
    if not val:
        _err(f"{var} unset")
    return val


def _require_clean_tree_sha() -> str:
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        ).stdout.strip()
    except subprocess.CalledProcessError as exc:
        _err(f"git status failed: {exc}")
    if status:
        _err(
            "working tree dirty; commit or stash before running the harness "
            "(artifact filename must reference a committed SHA):\n" + status
        )
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    ).stdout.strip()
    return sha


def _require_psql() -> None:
    if not shutil.which("psql"):
        _err("psql not on PATH; install postgresql-client and retry")


def _load_ticket_config(ticket_id: str) -> dict[str, Any]:
    path = REPO_ROOT / "scripts" / "perf_bench" / f"{ticket_id}.yaml"
    if not path.exists():
        _err(
            f"missing per-ticket config: {path.relative_to(REPO_ROOT)}. "
            "Required keys: sql_file (string), fixture_label (string), "
            "target_table (string or null)."
        )
    cfg = yaml.safe_load(path.read_text())
    if not isinstance(cfg, dict):
        _err(f"{path.relative_to(REPO_ROOT)}: top-level must be a mapping")
    for key in ("sql_file", "fixture_label"):
        value = cfg.get(key)
        if not isinstance(value, str) or not value:
            _err(f"{path.relative_to(REPO_ROOT)}: missing required string '{key}'")
    if "target_table" not in cfg:
        _err(
            f"{path.relative_to(REPO_ROOT)}: missing key 'target_table' "
            "(set to null if the perf claim does not touch a floored table)"
        )
    target = cfg["target_table"]
    if target is not None and not isinstance(target, str):
        _err(f"{path.relative_to(REPO_ROOT)}: 'target_table' must be string or null")
    if isinstance(target, str):
        if not TABLE_IDENT_RE.fullmatch(target):
            _err(
                f"{path.relative_to(REPO_ROOT)}: 'target_table' {target!r} is not "
                "a valid lowercase Postgres identifier (matches "
                f"{TABLE_IDENT_RE.pattern}). A floored or sentinel-safe table "
                "name is expected; SQL fragments are forbidden."
            )
        floors = load_floors()
        if target not in floors:
            _err(
                f"{path.relative_to(REPO_ROOT)}: 'target_table' {target!r} not in "
                "floors.yaml. Add a floor entry or set target_table: null for a "
                "non-floor perf claim. Both code paths share this validation."
            )
    return cfg


def _psql(db_url: str, sql: str) -> str:
    res = subprocess.run(
        ["psql", db_url, "-X", "-A", "-t", "-c", sql],
        check=True,
        capture_output=True,
        text=True,
    )
    return res.stdout


def _strip_trailing_semicolon(text: str) -> str:
    return text.strip().rstrip(";").strip()


def _run_explain(db_url: str, sql_file: Path) -> str:
    body = _strip_trailing_semicolon(sql_file.read_text())
    return f"{EXPLAIN_FLAGS}\n" + _psql(db_url, f"{EXPLAIN_FLAGS} {body}")


def _time_trials(db_url: str, sql_file: Path, n: int = 3) -> list[float]:
    body = _strip_trailing_semicolon(sql_file.read_text())
    trials: list[float] = []
    for _ in range(n):
        start = time.perf_counter()
        _psql(db_url, body)
        trials.append((time.perf_counter() - start) * 1000.0)
    return trials


def _fingerprint(db_url: str) -> dict[str, str]:
    return {
        "pg_version": _psql(db_url, "SHOW server_version").strip(),
        "host": os.uname().nodename,
        "shared_buffers": _psql(db_url, "SHOW shared_buffers").strip(),
    }


def _row_count(db_url: str, table: str) -> int:
    # Defence-in-depth: reject anything that does not match a strict
    # Postgres identifier shape before the unquoted f-string lands in
    # the ``psql -c`` command line. ``_load_ticket_config`` already
    # validates the YAML-supplied value, but keeping the guard here
    # closes the door for any future caller that bypasses the loader.
    if not TABLE_IDENT_RE.fullmatch(table):
        _err(f"invalid table identifier passed to _row_count: {table!r}")
    out = _psql(db_url, f"SELECT COUNT(*) FROM {table}").strip()
    try:
        return int(out)
    except ValueError:
        _err(f"COUNT(*) on {table!r} returned non-integer: {out!r}")


def _check_floors_only(ticket_id: str, db_url: str) -> int:
    cfg = _load_ticket_config(ticket_id)
    table = cfg.get("target_table")
    if table is None:
        print(f"no floor configured for {ticket_id} (target_table: null)")
        return 0
    floors = load_floors()
    floor = floors.get(table)
    if floor is None:
        _err(
            f"target_table '{table}' not in floors.yaml; add a floor entry "
            "or set target_table to null if no floored table is touched"
        )
    actual = _row_count(db_url, table)
    if actual < floor:
        _err(f"floor unmet: {table} row_count={actual} < {floor}")
    print(f"floor met: {table} row_count={actual} >= {floor}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="run_explain",
        description="perf-bench harness: EXPLAIN + 3-trial JSON + manifest YAML",
    )
    parser.add_argument("ticket_id", help="ticket number, e.g. 1346")
    parser.add_argument(
        "--check-floors-only",
        action="store_true",
        help="check target_table row count against floors.yaml then exit",
    )
    args = parser.parse_args(argv)

    db_url = _require_env("EBULL_BENCH_DB_URL")
    _require_psql()

    if args.check_floors_only:
        return _check_floors_only(args.ticket_id, db_url)

    sha = _require_clean_tree_sha()
    cfg = _load_ticket_config(args.ticket_id)
    sql_file = REPO_ROOT / cfg["sql_file"]
    if not sql_file.exists():
        _err(f"sql_file missing: {sql_file.relative_to(REPO_ROOT)}")

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    base = ARTIFACT_DIR / f"{args.ticket_id}-{sha}"

    base.with_suffix(".txt").write_text(_run_explain(db_url, sql_file))

    trials = _time_trials(db_url, sql_file)
    json_doc = {
        "trials": [{"wall_ms": t} for t in trials],
        "median_ms": statistics.median(trials),
        "fingerprint": _fingerprint(db_url),
    }
    base.with_suffix(".json").write_text(json.dumps(json_doc, indent=2) + "\n")

    # Record ``target_table`` verbatim (string or null) so the CI lint
    # can enforce floor-proof for floored claims. Without this key an
    # empty ``row_counts: {}`` would silently satisfy the lint and
    # sidestep §4 (Codex 2 IMPORTANT-1).
    manifest: dict[str, Any] = {
        "fixture_label": cfg["fixture_label"],
        "target_table": cfg["target_table"],
        "row_counts": {},
    }
    table = cfg["target_table"]
    if isinstance(table, str):
        manifest["row_counts"][table] = _row_count(db_url, table)
    (ARTIFACT_DIR / f"{args.ticket_id}-{sha}.manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=True))

    print(f"wrote 3 artifacts under {base.relative_to(REPO_ROOT)}.*")
    return 0


if __name__ == "__main__":
    sys.exit(main())
