"""#3902 full-population A/B: the #3841 PIT build before and after spec Amendment 1, over one database snapshot.

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md`` ("Acceptance (Amendment 1)"). Read-only.

The driver exports a REPEATABLE READ snapshot (``pg_export_snapshot``) and holds it open while two arm processes
import ``iter_instrument_builds`` from their own checkout (arm A: a ``git worktree`` of the base ref; arm B: this
checkout), attach to the snapshot, read the same ``submissions.zip`` and write one JSON line per instrument. The
driver then compares every instrument's whole result and fails on any difference the amendment does not allow.

    PYTHONPATH=. uv run python scripts/ab_3902_pit_reader.py --base-checkout /tmp/ebull-3902-base

Arm mode (run by the driver): ``python scripts/ab_3902_pit_reader.py --arm OUT --snapshot ID --build-date D``.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import subprocess
import sys
import zipfile
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

ALLOWED_OLD = {"submissions_accession_conflict", "untimed_accession"}
_SYMBOLS_SQL = "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%s)"


def _git_head(path: Path) -> str:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    done = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"], capture_output=True, text=True, env=env)
    return done.stdout.strip()


def _plain(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: _plain(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, dict):
        return {str(_plain(k)): _plain(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(v) for v in value]
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, date | datetime | Decimal):
        return str(value)
    return value


def run_arm(out: Path, snapshot: str, build_date: date, archive_sha256: str) -> None:
    from app.security.master_key import resolve_data_dir
    from app.services import pit_market_cap_build as build

    print(f"arm imports {build.__file__}", file=sys.stderr)
    archive_path = resolve_data_dir() / "sec" / "bulk" / "submissions.zip"
    with archive_path.open("rb") as handle:
        # One handle hashes and reads: a refresh replaces the archive by rename, so the open file stays the hashed one.
        digest = build._sha256_file(handle)
        if digest != archive_sha256:
            raise SystemExit(f"submissions.zip changed: {digest} != {archive_sha256}")
        _run_arm(out, snapshot, build_date, zipfile.ZipFile(handle))


def _run_arm(out: Path, snapshot: str, build_date: date, archive: zipfile.ZipFile) -> None:
    import psycopg
    from psycopg import sql

    from app.config import settings
    from app.services import pit_market_cap_build as build

    new_reader = getattr(build, "instrument_submissions_reader", None)
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute(sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(snapshot)))
        index_of: Any  # the two arms' readers differ in arity: (cik) on the base, (cik, others) on the branch
        if new_reader is not None:
            index_of = new_reader(archive)
        else:
            from app.services.pit_fundamentals import submissions_reader

            index_of = submissions_reader(archive)
        others: dict[int, list[str]] = {}
        if new_reader is not None:
            for iid, cik in conn.execute(build._OTHER_CIKS_SQL, {"ids": [i for i, _ in build.load_scope(conn)]}):
                others.setdefault(iid, []).append(build._cik10(cik))
        scope = build.load_scope(conn)
        with out.open("w") as handle:
            for batch in build.iter_instrument_builds(conn, scope=scope, index_of=index_of, build_date=build_date):
                for result in batch.instruments:
                    row = _plain(result)
                    if new_reader is not None:
                        index = index_of(result.cik, sorted(others.get(result.instrument_id, ())))
                        predecessor = getattr(index, "predecessor", frozenset())
                        row["predecessor_selected"] = sum(s.selection.accession in predecessor for s in result.steps)
                    handle.write(json.dumps(row, sort_keys=True) + "\n")


def _load(path: Path) -> dict[int, dict[str, Any]]:
    return {row["instrument_id"]: row for row in map(json.loads, path.read_text().splitlines())}


def _publishes_at(row: dict[str, Any], session: str) -> bool:
    return any(s["refusal"] is None and s["valid_from"] <= session < s["valid_to"] for s in row["steps"])


def drive(base: Path) -> int:
    import psycopg

    from app.config import settings
    from app.services.pit_market_cap_build import (
        _FPI_ADR_SQL,
        _OTHER_CIKS_SQL,
        load_multiclass_ciks,
        load_scope,
        nyse_sessions,
    )

    here = Path(__file__).resolve()
    work = Path(os.environ.get("TMPDIR", "/tmp")) / "ab_3902"
    work.mkdir(exist_ok=True)
    from app.security.master_key import resolve_data_dir
    from app.services.pit_market_cap_build import _sha256_file

    with (resolve_data_dir() / "sec" / "bulk" / "submissions.zip").open("rb") as handle:
        archive_sha256 = _sha256_file(handle)
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        snapshot_row = conn.execute("SELECT pg_export_snapshot()").fetchone()
        assert snapshot_row is not None
        snapshot = snapshot_row[0]
        build_row = conn.execute(
            "SELECT build_id, build_date FROM market_cap_pit_builds WHERE status = 'complete' ORDER BY build_id DESC"
        ).fetchone()
        assert build_row is not None
        build_id, build_date = build_row
        ids = [iid for iid, _ in load_scope(conn)]
        fpi_adr = {int(r[0]) for r in conn.execute(_FPI_ADR_SQL, {"ids": ids})}
        multiclass = load_multiclass_ciks(conn)
        mc_other = {iid for iid, cik in conn.execute(_OTHER_CIKS_SQL, {"ids": ids}) if f"{int(cik):010d}" in multiclass}
        symbols = dict(conn.execute(_SYMBOLS_SQL, (ids,)))
        head = {name: _git_head(path) for name, path in (("A", base), ("B", here.parents[1]))}
        print(f"submissions.zip sha256 {archive_sha256} (each arm verifies it)")
        print(f"snapshot {snapshot}; build_date {build_date} (build {build_id}); arm A {head['A']}; arm B {head['B']}")
        procs = {}
        for name, root in (("A", base), ("B", here.parents[1])):
            env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")} | {"PYTHONPATH": str(root)}
            out = work / f"arm_{name}.jsonl"
            cmd = [sys.executable, str(here), "--arm", str(out), "--snapshot", snapshot]
            cmd += ["--build-date", str(build_date), "--archive-sha256", archive_sha256]
            procs[name] = (subprocess.Popen(cmd, env=env, cwd=str(root)), out)
        failed_arm = False
        for name, (proc, _) in procs.items():
            if proc.wait() != 0:
                print(f"arm {name} exited {proc.returncode}")
                failed_arm = True
        if failed_arm:
            return 2

    old, new = _load(procs["A"][1]), _load(procs["B"][1])
    session = str(nyse_sessions(build_date.replace(day=1), build_date)[-1])
    keys = ("refused", "facts", "steps", "anchor_status", "tallies", "stamp", "gate1", "gate2")
    transitions: Counter[str] = Counter()
    failures = 0
    recovered: list[int] = []
    assert set(old) == set(new), "the arms read different scopes"
    for iid in sorted(old):
        a, b = old[iid], new[iid]
        b_tallies = {k: v for k, v in b["tallies"].items() if k != "submissions"}
        same = all(a[k] == b[k] for k in keys if k != "tallies") and a["tallies"] == b_tallies
        if same:
            transitions["unchanged"] += 1
            continue
        label = f"{symbols.get(iid)} iid={iid}"
        if a["refused"] in ALLOWED_OLD:
            transitions[f"{a['refused']} -> {b['refused'] or 'built'}"] += 1
            recovered.append(iid)
        elif b["refused"] == "fpi_adr" and iid in fpi_adr:
            transitions[f"{a['refused'] or 'built'} -> fpi_adr"] += 1
        elif iid in mc_other:
            transitions["multiclass via non-primary CIK"] += 1
            print(f"[multiclass] {label}: {a['refused']} -> {b['refused']}")
        else:
            failures += 1
            diff = [k for k in keys if a[k] != b[k]]
            print(f"[FAIL] {label}: {a['refused']} -> {b['refused']}; differs in {diff}")
    predecessor_selected = sum(row.get("predecessor_selected", 0) for row in new.values())
    conflict_left = sum(row["refused"] == "submissions_accession_conflict" for row in new.values())
    print("\ntransitions:", dict(sorted(transitions.items())))
    print(f"arm B steps selecting a predecessor-only accession: {predecessor_selected}")
    print(f"arm B submissions_accession_conflict refusals: {conflict_left}")
    print(f"arm B cik_refused: {dict(Counter(r['refused'] for r in new.values() if r['refused']))}")
    print(f"\nrecovered ({len(recovered)}), latest session {session}:")
    publishing = 0
    for iid in recovered:
        b = new[iid]
        pub = _publishes_at(b, session)
        publishing += pub
        print(
            f"  {symbols.get(iid)} iid={iid}: {old[iid]['refused']} -> {b['refused'] or 'built'}; "
            f"facts {len(b['facts'])}; steps {len(b['steps'])} "
            f"(published {sum(s['refusal'] is None for s in b['steps'])}); publishes at session: {pub}; "
            f"submissions {b['tallies'].get('submissions')}"
        )
        for s in b["steps"]:
            sel = s["selection"]
            print(
                f"     {s['valid_from']}..{s['valid_to']} {sel['concept']} {sel['accession']} ctx {sel['context']} "
                f"shares {sel['shares']} factor {s['split_factor']} refusal {s['refusal']}"
            )
    timed = sum(new[i]["refused"] not in ALLOWED_OLD for i in recovered)
    built = sum(bool(new[i]["steps"]) for i in recovered)
    print(f"\nof {len(recovered)}: timed {timed}, built {built}, publishing at {session} {publishing}")
    by_symbol = {symbols.get(i): i for i in recovered}
    must = [s for s in ("AMZN", "MORN", "XPO") if not (s in by_symbol and _publishes_at(new[by_symbol[s]], session))]
    if must:
        print(f"[FAIL] not publishing at {session}: {must}")
    ok = failures == 0 and predecessor_selected == 0 and conflict_left == 0 and not must
    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-checkout", type=Path)
    parser.add_argument("--arm", type=Path)
    parser.add_argument("--snapshot")
    parser.add_argument("--build-date", type=date.fromisoformat)
    parser.add_argument("--archive-sha256")
    args = parser.parse_args()
    if args.arm:
        run_arm(args.arm, args.snapshot, args.build_date, args.archive_sha256)
        return 0
    return drive(args.base_checkout)


if __name__ == "__main__":
    raise SystemExit(main())
