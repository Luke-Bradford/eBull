"""#3908 full-population A/B: the #3841 PIT build reading 13F anchors from the legacy table (arm A) and from the
canonical observation layer (arm B), over one database snapshot.

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md`` ("Acceptance (Amendment 2)"). Read-only.

The arms run as in ``ab_3902_pit_reader.py``: the driver exports a REPEATABLE READ snapshot and holds it open while two
processes build every in-scope instrument from their own checkout (arm A: a ``git worktree`` of the base ref; arm B:
this checkout) and write one JSON line per instrument. The per-filer checks (fails 4 and 6) are too large for JSON, so
the driver runs both arms' ``_UNIT_VALUES_SQL`` itself, joined on (instrument, quarter, filer CIK) inside the same
snapshot; arm A's statement is read from the base checkout's source with ``ast``.

    git worktree add /tmp/ebull-3908-base origin/main
    PYTHONPATH=. uv run python scripts/ab_3908_pit_anchor_source.py --base-checkout /tmp/ebull-3908-base
"""

from __future__ import annotations

import argparse
import ast
import bisect
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from scripts.ab_3902_pit_reader import _git_head, _load

PANEL = ("AAPL", "GME", "MSFT", "JPM", "HD")
PARITY = 1.001
AGREEMENT = 1.05
SPLIT_BAND = 1.25
_SYMBOLS_SQL = "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%s)"
_CLOSES_SQL = (
    "SELECT DISTINCT ON (instrument_id) instrument_id, close FROM price_daily "
    "WHERE instrument_id = ANY(%s) AND price_date <= %s AND close IS NOT NULL "
    "ORDER BY instrument_id, price_date DESC"
)


def _sql_constant(path: Path, name: str) -> str:
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            assert isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
            return node.value.value
    raise SystemExit(f"{name} not found in {path}")


# Fails 4 and 6, per (instrument, quarter, filer CIK). ``a`` is arm A's reader output with its filer ids mapped to
# CIKs, ``b`` arm B's. ``picked`` is arm B's selected report for a filer arm A has and arm B lacks: its latest
# ``filed_at`` over the active economic 13F rows, and whether that report holds a positive EQUITY row.
_FILERS_SQL = """
    WITH a_raw (iid, q, filer, uv, accs, filed) AS ({a}),
    a AS (SELECT a_raw.iid, a_raw.q, f.cik, a_raw.uv, a_raw.accs, a_raw.filed
          FROM a_raw JOIN institutional_filers f ON f.filer_id = a_raw.filer),
    b (iid, q, cik, uv, accs, filed) AS ({b}),
    j AS (SELECT coalesce(a.iid, b.iid) AS iid, coalesce(a.q, b.q) AS q, coalesce(a.cik, b.cik) AS cik,
                 a.uv AS a_uv, b.uv AS b_uv, a.accs AS a_accs, b.accs AS b_accs, a.filed AS a_filed,
                 b.filed AS b_filed
          FROM a FULL JOIN b ON a.iid = b.iid AND a.q = b.q AND a.cik = b.cik),
    picked AS (
        SELECT j.iid, j.q, j.cik, max(o.filed_at) AS filed
        FROM j JOIN ownership_institutions_observations o
          ON o.instrument_id = j.iid AND o.period_end = j.q AND o.filer_cik = j.cik
        WHERE j.b_uv IS NULL AND o.source = '13f' AND o.ownership_nature = 'economic' AND o.known_to IS NULL
        GROUP BY 1, 2, 3
    )
    SELECT j.iid, j.q, j.cik, j.a_uv, j.b_uv, j.a_accs, j.b_accs, j.a_filed, j.b_filed, p.filed,
           EXISTS (SELECT 1 FROM ownership_institutions_observations o
                   WHERE o.instrument_id = j.iid AND o.period_end = j.q AND o.filer_cik = j.cik
                     AND o.source = '13f' AND o.ownership_nature = 'economic' AND o.known_to IS NULL
                     AND o.filed_at = p.filed AND o.exposure_kind = 'EQUITY'
                     AND o.shares > 0 AND o.market_value_usd > 0)
    FROM j LEFT JOIN picked p ON p.iid = j.iid AND p.q = j.q AND p.cik = j.cik
    WHERE j.a_uv IS NULL OR j.b_uv IS NULL OR j.a_accs IS DISTINCT FROM j.b_accs
       OR j.a_filed IS DISTINCT FROM j.b_filed OR greatest(j.a_uv / j.b_uv, j.b_uv / j.a_uv) > {parity}
"""


def _m(x: float) -> float:
    return max(x, 1 / x)


def _covering(steps: list[dict[str, Any]], starts: list[str], session: str) -> dict[str, Any] | None:
    i = bisect.bisect_right(starts, session) - 1
    return steps[i] if i >= 0 and session < steps[i]["valid_to"] else None


def _window(step: dict[str, Any]) -> str:
    return " ".join(
        f"{w['q']}:{w['status']}/{w['filers']}/{w['f_hat'] and round(w['f_hat'], 3)}"
        for w in step["evidence"].get("basis", {}).get("window", [])
    )


def _filers(conn: Any, a_sql: str, b_sql: str, ids: list[int], quarters: list[date]) -> tuple[int, Counter[str]]:
    sql = _FILERS_SQL.format(a=a_sql, b=b_sql, parity=PARITY)
    tally: Counter[str] = Counter()
    failures = 0
    for iid, q, cik, a_uv, b_uv, a_accs, b_accs, a_filed, b_filed, b_pick, b_pick_equity in conn.execute(
        sql, {"ids": ids, "quarters": quarters}
    ):
        where = f"iid={iid} q={q} filer={cik}"
        if a_uv is None:
            tally["added"] += 1
        elif b_uv is None:
            if b_pick is not None and b_pick > a_filed and not b_pick_equity:
                tally["dropped: later report without equity"] += 1
            else:
                failures += 1
                print(f"[FAIL 6] dropped {where}: A {a_accs} @{a_filed}; B report @{b_pick} equity={b_pick_equity}")
        elif sorted(a_accs) == sorted(b_accs):
            # The same accessions are the same report whatever each store's ``filed_at``: parity always applies.
            if _m(a_uv / b_uv) > PARITY:
                failures += 1
                print(f"[FAIL 4] parity {where}: {a_accs} A {a_uv} @{a_filed} B {b_uv} @{b_filed}")
            else:
                tally["same accessions, within parity" + ("" if a_filed == b_filed else ", filed_at differs")] += 1
        elif b_filed > a_filed:
            tally["reselected: later B report"] += 1
        elif b_filed == a_filed and set(a_accs) < set(b_accs):
            # Same-date tie (spec limit): the legacy table holds only some of the filer's same-day reports.
            tally["same-date tie: B holds more of the reports"] += 1
            print(f"[tie] {where}: A {a_accs} B {b_accs} @{a_filed}; values A {a_uv} B {b_uv}")
        else:
            failures += 1
            print(f"[FAIL 4] selection {where}: A {a_accs} @{a_filed}; B {b_accs} @{b_filed}")
    return failures, tally


def drive(base: Path) -> int:
    import psycopg

    from app.config import settings
    from app.security.master_key import resolve_data_dir
    from app.services.pit_market_cap import FIRST_QUARTER, quarter_ends
    from app.services.pit_market_cap_build import FIRST_SESSION, _sha256_file, load_scope, nyse_sessions

    here = Path(__file__).resolve()
    work = Path(os.environ.get("TMPDIR", "/tmp")) / "ab_3908"
    work.mkdir(exist_ok=True)
    with (resolve_data_dir() / "sec" / "bulk" / "submissions.zip").open("rb") as handle:
        archive_sha256 = _sha256_file(handle)
    a_sql = _sql_constant(base / "app/services/pit_market_cap_build.py", "_UNIT_VALUES_SQL")
    b_sql = _sql_constant(here.parents[1] / "app/services/pit_market_cap_build.py", "_UNIT_VALUES_SQL")
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
        symbols = dict(conn.execute(_SYMBOLS_SQL, (ids,)))
        head = {name: _git_head(path) for name, path in (("A", base), ("B", here.parents[1]))}
        print(f"snapshot {snapshot}; build_date {build_date} (build {build_id}); arm A {head['A']}; arm B {head['B']}")
        procs = {}
        for name, root in (("A", base), ("B", here.parents[1])):
            env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")} | {"PYTHONPATH": str(root)}
            out = work / f"arm_{name}.jsonl"
            cmd = [sys.executable, str(root / "scripts/ab_3902_pit_reader.py"), "--arm", str(out)]
            cmd += ["--snapshot", snapshot, "--build-date", str(build_date), "--archive-sha256", archive_sha256]
            procs[name] = (subprocess.Popen(cmd, env=env, cwd=str(root)), out)
        quarters = list(quarter_ends(FIRST_QUARTER, build_date))
        filer_failures, filer_tally = _filers(conn, a_sql, b_sql, ids, quarters)
        sessions = [str(s) for s in nyse_sessions(FIRST_SESSION, build_date)]
        latest = sessions[-1]
        closes = {iid: float(c) for iid, c in conn.execute(_CLOSES_SQL, (ids, latest)) if c is not None}
        if any(proc.wait() != 0 for proc, _ in procs.values()):
            print("an arm failed")
            return 2

    old, new = _load(procs["A"][1]), _load(procs["B"][1])
    failures = filer_failures
    traced: list[str] = []
    every: Counter[str] = Counter()
    at_latest: Counter[str] = Counter()
    anchors: Counter[str] = Counter()
    gained: list[tuple[float, int]] = []
    if set(old) != set(new):  # fail 1
        failures += 1
        print(f"[FAIL 1] instruments in one arm only: {sorted(set(old) ^ set(new))}")
    for iid in sorted(set(old) & set(new)):
        a, b = old[iid], new[iid]
        label = f"{symbols.get(iid)} iid={iid}"
        if sum(a["anchor_status"].values()) != sum(b["anchor_status"].values()):  # fail 3
            failures += 1
            print(f"[FAIL 3] {label}: anchor quarters {a['anchor_status']} vs {b['anchor_status']}")
        for key, x in (("A", a), ("B", b)):
            for status, n in x["anchor_status"].items():
                anchors[f"{key} {status}"] += n
        a_steps, b_steps = a["steps"], b["steps"]
        a_starts, b_starts = [s["valid_from"] for s in a_steps], [s["valid_from"] for s in b_steps]
        a_win = {w["q"]: w for s in a_steps for w in s["evidence"].get("basis", {}).get("window", [])}
        b_win = {w["q"]: w for s in b_steps for w in s["evidence"].get("basis", {}).get("window", [])}
        for q in sorted(set(a_win) & set(b_win)):  # fail 8, anchor half: a basis class flip
            wa, wb = a_win[q], b_win[q]
            if wa["status"] == wb["status"] == "ok":
                pa, pb = (1.0 if _m(w["f_hat"]) <= 1.10 else w["f_hat"] for w in (wa, wb))
                if (pa == 1.0) != (pb == 1.0) or _m(pa / pb) > AGREEMENT:
                    traced.append(f"[TRACE 8 anchor] {label} {q}: A {wa} B {wb}")
        reported: set[str] = set()
        for session in sessions:
            sa, sb = _covering(a_steps, a_starts, session), _covering(b_steps, b_starts, session)
            if (sa is None) != (sb is None):  # fail 2
                failures += 1
                print(f"[FAIL 2] {label} {session}: step in one arm only")
                break
            if sa is None or sb is None:
                continue
            pa, pb = sa["refusal"] is None, sb["refusal"] is None
            kind = (
                "published -> published"
                if pa and pb
                else f"refused -> published ({sa['refusal']})"
                if pb
                else f"published -> refused ({sb['refusal']})"
                if pa
                else "refused -> refused, changed"
                if sa["refusal"] != sb["refusal"]
                else "refused -> refused"
            )
            every[kind] += 1
            if session == latest:
                at_latest[kind] += 1
                if pb and not pa and iid in closes and sb["selection"]["shares"] is not None:
                    gained.append((closes[iid] * float(sb["selection"]["shares"]) * sb["split_factor"], iid))
                if symbols.get(iid) in PANEL and (sa["split_factor"], sa["selection"]) != (
                    sb["split_factor"],
                    sb["selection"],
                ):
                    traced.append(f"[TRACE 9 panel] {label}: A {sa['split_factor']} B {sb['split_factor']}")
            if pa and pb:
                if sa["selection"] != sb["selection"]:  # fail 5
                    failures += 1
                    print(f"[FAIL 5] {label} {session}: selection differs")
                    break
                if _m(sb["split_factor"] / sa["split_factor"]) > AGREEMENT and sa["valid_from"] not in reported:
                    reported.add(sa["valid_from"])
                    traced.append(
                        f"[TRACE 8 factor] {label} from {session}: A {sa['split_factor']} B {sb['split_factor']}"
                    )
            elif pa and sb["valid_from"] not in reported:  # traced 7: a lost publication, once per B step
                reported.add(sb["valid_from"])
                traced.append(
                    f"[TRACE 7 lost] {label} from {session} ({sb['refusal']}): A {_window(sa)} | B {_window(sb)}"
                )
    print("\nper-filer:", dict(sorted(filer_tally.items())))
    print("anchor statuses:", dict(sorted(anchors.items())))
    print("\ntransitions, all sessions:", json.dumps(dict(sorted(every.items())), indent=1))
    print(f"transitions at {latest}:", json.dumps(dict(sorted(at_latest.items())), indent=1))
    print(f"\ntraced cases ({len(traced)}):")
    for line in traced:
        print(" ", line)
    print(f"\nexternal spot-check candidates (refused -> published at {latest}, by arm-B cap):")
    for cap, iid in sorted(gained, reverse=True)[:15]:
        print(f"  {symbols.get(iid)} iid={iid} cap {cap / 1e9:,.2f}B")
    print(f"\nstructural + membership failures: {failures}; traced cases: {len(traced)}")
    print("RESULT:", "PASS (pending traces)" if failures == 0 else "FAIL")
    return 0 if failures == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-checkout", type=Path, required=True)
    return drive(parser.parse_args().base_checkout)


if __name__ == "__main__":
    raise SystemExit(main())
