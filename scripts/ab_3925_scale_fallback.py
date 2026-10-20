"""#3925 full-population A/B: the #3841 PIT build before (arm A) and after (arm B) spec Amendment 3, the scale
check's EPS-identity comparator, over one database snapshot.

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md`` ("Acceptance (Amendment 3)"). Read-only.

The arms run as in ``ab_3902_pit_reader.py``: the driver exports a REPEATABLE READ snapshot and holds it open while
two processes build every in-scope instrument from their own checkout (arm A: a ``git worktree`` of the base ref; arm
B: this checkout) and write one JSON line per instrument. The driver then reads, in the same snapshot, the facts the
oracle (``scripts/measure_3925_premise.py``'s ``evaluate``) needs for every arm-B step whose selection is eligible.

    git worktree add /tmp/ebull-3925-base origin/main
    PYTHONPATH=. uv run python scripts/ab_3925_scale_fallback.py --base-checkout /tmp/ebull-3925-base \\
        --reference docs/research/3925-companiesmarketcap.json > out.txt

``--reference`` holds companiesmarketcap weekly points per symbol (fail 6). A point stamped Saturday 00:00 UTC carries
the previous Friday's close, so it is compared on the last NYSE session before its stamp date.
"""

from __future__ import annotations

import argparse
import bisect
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from scripts.ab_3902_pit_reader import _git_head, _load
from scripts.measure_3925_premise import NEW_YORK, Fact, evaluate

PANEL = ("AAPL", "GME", "MSFT", "JPM", "HD")
TOLERANCE = 100.0
EXTERNAL_BAND = (0.95, 1.05)
BASIS_REFUSALS = {"no_left_anchor", "basis_unresolved"}
GATE_REFUSALS = {"anchor_contradicted", "float_bound"}
_SYMBOLS_SQL = "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%s)"
_CLOSES_SQL = (
    "SELECT instrument_id, price_date, close FROM price_daily WHERE instrument_id = ANY(%s) AND close IS NOT NULL"
)

# The oracle's inputs for the eligible (instrument, accession) keys, read inside the driver's snapshot.
_ORACLE_FACTS_SQL = """
SELECT DISTINCT f.instrument_id, f.accession_number, f.concept, f.period_start, f.period_end, f.val, f.fiscal_period
FROM financial_facts_raw f JOIN unnest(%(iids)s::bigint[], %(accs)s::text[]) AS k(iid, acc)
  ON f.instrument_id = k.iid AND f.accession_number = k.acc
WHERE f.taxonomy = 'us-gaap' AND f.val IS NOT NULL AND f.val <> 'NaN'::numeric
  AND f.period_start IS NOT NULL AND f.period_start <= f.period_end
  AND ((f.concept = 'EarningsPerShareBasic' AND f.unit = 'USD/shares')
    OR (f.concept IN ('NetIncomeLossAvailableToCommonStockholdersBasic', 'NetIncomeLoss') AND f.unit = 'USD')
    OR (f.concept = 'WeightedAverageNumberOfSharesOutstandingBasic' AND f.unit = 'shares'))
"""
_ORACLE_SHEET_SQL = """
WITH f AS (
  SELECT DISTINCT f.instrument_id, f.accession_number, f.period_end, f.val
  FROM financial_facts_raw f JOIN unnest(%(iids)s::bigint[], %(accs)s::text[]) AS k(iid, acc)
    ON f.instrument_id = k.iid AND f.accession_number = k.acc
  WHERE f.taxonomy = 'us-gaap' AND f.concept = 'CommonStockSharesOutstanding' AND f.unit = 'shares'
    AND f.val IS NOT NULL AND f.val <> 'NaN'::numeric AND f.period_end >= %(min_context)s
)
SELECT instrument_id, accession_number, array_agg(val ORDER BY val)
FROM f JOIN (SELECT instrument_id, accession_number, max(period_end) AS pe FROM f GROUP BY 1, 2) m
  USING (instrument_id, accession_number)
WHERE f.period_end = m.pe GROUP BY 1, 2
"""


def _covering(steps: list[dict[str, Any]], starts: list[str], session: str) -> dict[str, Any] | None:
    i = bisect.bisect_right(starts, session) - 1
    return steps[i] if i >= 0 and session < steps[i]["valid_to"] else None


def _within(ratio: float) -> bool:
    return 1 / TOLERANCE <= ratio <= TOLERANCE


def _positive(value: str | None) -> bool:
    try:
        return value is not None and 0 < float(value) < float("inf")
    except ValueError:
        return False


def _eligible(step: dict[str, Any]) -> bool:
    """Rule 1, read from the step: a valid cover count with no finite positive balance-sheet comparator."""
    scale = step["evidence"]["checks"]["scale"]
    return (
        step["selection"]["concept"] == "cover"
        and _positive(step["selection"]["shares"])
        and not _positive(scale.get("sheet"))
    )


def _expected_refusal(step: dict[str, Any]) -> str | None | bool:
    """Fail 8: the refusal the recorded checks imply, or True when the step's refusal is decided before them."""
    ev, refusal = step["evidence"], step["refusal"]
    checks = {k: v["outcome"] for k, v in ev["checks"].items()}
    if refusal in ("multiclass_unavailable", "shares_ambiguous", "invalid_input"):
        return True
    if ev["multiclass"]:
        return "multiclass_unavailable"
    if ev["basis"]["refusal"] is not None:
        return ev["basis"]["refusal"]
    for name, code in (
        ("scale", "shares_scale_conflict"),
        ("turnover", "shares_turnover_implausible"),
        ("discontinuity", "shares_discontinuity"),
    ):
        if checks[name] == "fail":
            return code
    return refusal if refusal in GATE_REFUSALS else None


def _discontinuity(steps: list[dict[str, Any]]) -> list[str]:
    """Fail 9: each step's discontinuity reference and outcome recomputed from the arm's own step sequence. A gate
    refusal is applied after the steps are built, so a gate-refused step was published when later steps saw it."""
    problems: list[str] = []
    for i, step in enumerate(steps):
        recorded = step["evidence"]["checks"]["discontinuity"]
        selection = step["selection"]
        reference = next(
            (
                s
                for s in reversed(steps[:i])
                if (s["refusal"] is None or s["refusal"] in GATE_REFUSALS)
                and s["selection"]["concept"] == selection["concept"]
                and s["selection"] != selection
            ),
            None,
        )
        factor = step["evidence"]["basis"]["factor"]
        current = float(selection["shares"]) * factor if selection["shares"] and factor is not None else None
        ref_basis = None
        if reference is not None and reference["selection"]["shares"] is not None:
            ref_factor = reference["evidence"]["basis"]["factor"]
            ref_basis = float(reference["selection"]["shares"]) * ref_factor if ref_factor is not None else None
        if current is None or not current > 0 or ref_basis is None or not ref_basis > 0:
            outcome = "untested"
        else:
            outcome = "pass" if _within(current / ref_basis) else "fail"
        accession = None if reference is None else reference["selection"]["accession"]
        recorded_accession = (recorded.get("reference") or {}).get("accession")
        if outcome != recorded["outcome"] or (outcome != "untested" and accession != recorded_accession):
            problems.append(
                f"from {step['valid_from']}: recorded {recorded['outcome']} vs {recorded_accession}, "
                f"recomputed {outcome} vs {accession}"
            )
    return problems


def drive(base: Path, reference_path: Path) -> int:
    import psycopg

    from app.config import settings
    from app.security.master_key import resolve_data_dir
    from app.services.factor_panel import MAX_SHARES_AGE_MONTHS, add_months
    from app.services.pit_market_cap_build import FIRST_SESSION, _sha256_file, load_scope, nyse_sessions

    here = Path(__file__).resolve()
    work = Path(os.environ.get("TMPDIR", "/tmp")) / "ab_3925"
    work.mkdir(exist_ok=True)
    min_context = add_months(FIRST_SESSION, -MAX_SHARES_AGE_MONTHS)
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
        if any(proc.wait() != 0 for proc, _ in procs.values()):
            print("an arm failed")
            return 2
        old, new = _load(procs["A"][1]), _load(procs["B"][1])
        keys = sorted(
            {(iid, s["selection"]["accession"]) for iid, row in new.items() for s in row["steps"] if _eligible(s)}
        )
        params = {"iids": [k[0] for k in keys], "accs": [k[1] for k in keys], "min_context": min_context}
        oracle_facts: dict[tuple[int, str], list[Fact]] = defaultdict(list)
        for iid, accession, concept, start, end, value, fiscal_period in conn.execute(_ORACLE_FACTS_SQL, params):
            oracle_facts[(iid, accession)].append(Fact(concept, start, end, value, fiscal_period))
        oracle_sheets = {(i, a): v for i, a, v in conn.execute(_ORACLE_SHEET_SQL, params)}
        reference = json.loads(reference_path.read_text())
        by_symbol = {symbol: iid for iid, symbol in symbols.items()}
        referenced = [by_symbol[s] for s in reference["symbols"] if s in by_symbol]
        closes: dict[tuple[int, str], float] = {
            (iid, str(day)): float(close) for iid, day, close in conn.execute(_CLOSES_SQL, (referenced,))
        }

    sessions = [str(s) for s in nyse_sessions(FIRST_SESSION, build_date)]
    latest = sessions[-1]
    failures: Counter[str] = Counter()
    traced: list[str] = []
    every: Counter[str] = Counter()
    at_latest: Counter[str] = Counter()
    scale_by: dict[str, Counter[tuple[str, str]]] = {"A": Counter(), "B": Counter()}
    newly: dict[tuple[int, str], list[str]] = defaultdict(list)
    no_reference = 0

    def fail(code: str, message: str) -> None:
        failures[code] += 1
        if failures[code] <= 25:
            print(f"[FAIL {code}] {message}")

    if set(old) != set(new):
        fail("1", f"instruments in one arm only: {sorted(set(old) ^ set(new))}")
    for iid in sorted(set(old) & set(new)):
        a, b = old[iid], new[iid]
        label = f"{symbols.get(iid)} iid={iid}"
        for name, row in (("A", a), ("B", b)):
            for s in row["steps"]:
                scale = s["evidence"]["checks"]["scale"]
                scale_by[name][(str(scale.get("comparator")), scale["outcome"])] += 1
            for problem in _discontinuity(row["steps"]):
                fail("9", f"{label} arm {name} {problem}")
            for s in row["steps"]:
                expected = _expected_refusal(s)
                if expected is not True and expected != s["refusal"]:
                    fail("8", f"{label} arm {name} from {s['valid_from']}: {s['refusal']} but checks give {expected}")
        for s in b["steps"]:  # fail 7: the oracle
            if not _eligible(s):
                continue
            key = (iid, s["selection"]["accession"])
            accepted_ny = datetime.fromisoformat(s["selection"]["accepted_at"]).astimezone(NEW_YORK).date()
            want = evaluate(Decimal(s["selection"]["shares"]), accepted_ny, oracle_sheets.get(key), oracle_facts[key])
            got = s["evidence"]["checks"]["scale"]
            want_comparator = "weighted_average_basic" if want.outcome in ("pass", "fail") else None
            want_statuses = [status for _, _, status, _, _ in want.periods]
            got_statuses = [p["status"] for p in got.get("periods", [])]
            if (got["outcome"], got.get("comparator"), got_statuses) != (want.outcome, want_comparator, want_statuses):
                fail(
                    "7",
                    f"{label} {key[1]} from {s['valid_from']}: B {got['outcome']}/{got.get('comparator')}/"
                    f"{got_statuses} oracle {want.outcome}/{want_comparator}/{want_statuses}",
                )
        a_steps, b_steps = a["steps"], b["steps"]
        a_starts, b_starts = [s["valid_from"] for s in a_steps], [s["valid_from"] for s in b_steps]
        reported: set[str] = set()
        for session in sessions:
            sa, sb = _covering(a_steps, a_starts, session), _covering(b_steps, b_starts, session)
            if (sa is None) != (sb is None):
                fail("2", f"{label} {session}: step in one arm only")
                break
            if sa is None or sb is None:
                continue
            ea, eb = sa["evidence"], sb["evidence"]
            same = (
                sa["selection"] == sb["selection"]
                and {k: ea["basis"][k] for k in ("factor", "refusal", "window")}
                == {k: eb["basis"][k] for k in ("factor", "refusal", "window")}
                and ea["checks"]["turnover"]["outcome"] == eb["checks"]["turnover"]["outcome"]
                and ea["multiclass"] == eb["multiclass"]
            )
            if not same:
                fail("3", f"{label} {session}: selection, basis, turnover or multiclass differs")
                break
            scale_b = eb["checks"]["scale"]
            if (scale_b.get("comparator") == "balance_sheet" or not _eligible(sb)) and (
                ea["checks"]["scale"]["outcome"] != scale_b["outcome"]
            ):
                fail("4", f"{label} {session}: scale outcome changed where rule 1 does not apply")
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
            if pa and pb and sa["split_factor"] != sb["split_factor"]:
                fail("5", f"{label} {session}: split factor {sa['split_factor']} vs {sb['split_factor']}")
            if pb and not pa:
                newly[(iid, sb["selection"]["accession"])].append(session)
            if pb and _eligible(sb) and scale_b["outcome"] == "untested":
                ref = eb["checks"]["discontinuity"]["reference"]
                no_reference += ref is None
            if pa and not pb and sb["valid_from"] not in reported:
                reported.add(sb["valid_from"])
                traced.append(
                    f"[TRACE 10] {label} from {session} ({sb['refusal']}): scale B {scale_b['outcome']}/"
                    f"{scale_b.get('comparator')}; discontinuity A {ea['checks']['discontinuity']} "
                    f"B {eb['checks']['discontinuity']}"
                )
            if (
                symbols.get(iid) in PANEL
                and session == latest
                and (sa["split_factor"], sa["refusal"])
                != (
                    sb["split_factor"],
                    sb["refusal"],
                )
            ):
                traced.append(f"[TRACE 11 panel] {label}: A {sa['refusal']} B {sb['refusal']}")

    print(f"\nfail 6, newly published counts against {reference['source']} (retrieved {reference['retrieved']}):")
    for (iid, accession), days in sorted(newly.items()):
        step = next(s for s in new[iid]["steps"] if s["selection"]["accession"] == accession)
        scale = step["evidence"]["checks"]["scale"]
        label = f"{symbols.get(iid)} iid={iid} {accession} shares {step['selection']['shares']}"
        span = f"{len(days)} sessions {days[0]}..{days[-1]}"
        print(f"  {label}: {span}; scale {scale['outcome']}/{scale.get('comparator')}")
        if scale.get("comparator") == "balance_sheet" and scale["outcome"] == "pass":
            continue
        compared = 0
        b_steps = new[iid]["steps"]
        for point in reference["symbols"].get(symbols.get(iid), []):
            stamp = date.fromisoformat(point["stamp"])
            session = next((d for d in reversed(sessions) if date.fromisoformat(d) < stamp), None)
            if session not in days:
                continue
            covering = _covering(b_steps, [s["valid_from"] for s in b_steps], session)
            close = closes.get((iid, session))
            if close is None:  # Amendment 2's rule: no stored close makes the point uncomparable, not a pass
                print(f"    {session} (point {point['stamp']}): uncomparable, no stored close")
                continue
            if covering is None or covering["split_factor"] is None:
                fail("6", f"{label}: no arm-B cap on published session {session}")
                continue
            cap = close * float(covering["selection"]["shares"]) * covering["split_factor"]
            ratio = cap / point["market_cap_usd"]
            compared += 1
            ok = EXTERNAL_BAND[0] <= ratio <= EXTERNAL_BAND[1]
            print(
                f"    {session} (point {point['stamp']}): B {cap / 1e9:,.4f}B vs "
                f"{point['market_cap_usd'] / 1e9:,.4f}B = {ratio:.4f} {'ok' if ok else 'MISS'}"
            )
            if not ok:
                fail("6", f"{label}: {session} ratio {ratio:.4f}")
        if compared == 0:
            fail("6", f"{label}: no balance-sheet pass and no external point in its newly published sessions")
    print("\nscale outcomes by (comparator, outcome), steps:")
    for name in ("A", "B"):
        print(f"  {name}: {dict(sorted(scale_by[name].items()))}")
    print("\ntransitions, all sessions:", json.dumps(dict(sorted(every.items())), indent=1))
    print(f"transitions at {latest}:", json.dumps(dict(sorted(at_latest.items())), indent=1))
    print(f"eligible untested sessions published in B with no effective discontinuity reference: {no_reference:,}")
    print(f"\ntraced cases ({len(traced)}):")
    for line in traced:
        print(" ", line)
    print(f"\nstructural failures: {dict(sorted(failures.items()))}")
    print("RESULT:", "PASS (pending traces)" if not failures else "FAIL")
    return 0 if not failures else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-checkout", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    args = parser.parse_args()
    return drive(args.base_checkout, args.reference)


if __name__ == "__main__":
    raise SystemExit(main())
