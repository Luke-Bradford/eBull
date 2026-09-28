"""#1822 route F: the ablation readout script.

Spec ``docs/proposals/ta/2026-09-28-1822-ranking-ablation.md`` (v8), slice 1b.

- ``--census`` prints the spec's premise table and the "Prices" figures from one REPEATABLE
  READ snapshot, always rolled back. It reads prices for verdicts, the control and entry
  availability, and NEVER computes a forward return.
- ``--terms`` writes a new terms sidecar: today's semantic terms plus the floor facts at
  ``--readout-date``, read from the calendar, scores and ``job_runs`` only.
- ``--freeze [--dry-run]`` freezes the declaration the selected sidecar pins, from a clean
  tree, and prints the record the readout-1 PR commits.
- ``--readout`` passes the gate (clean tree, one sidecar, the frozen row's digest equal to the
  rebuilt one, no declaration refusal), appends a start record to the look log under an
  exclusive lock, and only then reads prices and computes every evaluation.

Every query runs through ``ranking_ablation_reader.execute``'s registry.

    PYTHONPATH=. uv run python scripts/run_1822_ablation_readout.py --census [--readout-date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import subprocess
import sys
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

import psycopg
from psycopg import IsolationLevel

from app.config import settings
from app.services import ranking_ablation as ra
from app.services import ranking_ablation_reader as reader
from app.services import ranking_ablation_readout as readout_mod
from app.services import ranking_ablation_terms as terms
from app.services.hunt_evaluator import Grid, select_arm
from app.services.hunt_inference import MIN_ARM_FORMATIONS, SHORT_SAMPLE_LAGS, StatRefused, hunt_lag, sparse_arm_count
from app.services.prereg_contract import declaration_refusals
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.result_ledger import PreregDeclarationRefused, freeze_preregistration, load_preregistration
from app.services.series_termination import TerminationClass

#: The look log (spec "Trial register"): a start record per readout, appended before any return is read.
LOOKS_PATH: Final = terms.REPO_ROOT / "var" / "1822-route-f" / "looks.jsonl"
#: Confidence's neutral value (spec "Premise check"). A thesis can also score exactly this, so
#: the share of rows AT it is reported beside, never instead of, the writer's branch record.
_NEUTRAL_CONFIDENCE = 0.5


def _json_default(value: object) -> str:
    return value.isoformat() if isinstance(value, date | datetime) else str(value)


def _reconciliation(population: reader.Population) -> dict[str, Any]:
    """raw_total vs Σ w·s, total_score vs clip(raw_total − P + R), and branch usage, over every valid row."""
    weights = ra.weights()
    raw_gap = 0.0
    total_gap = 0.0
    clipped = 0
    neutral_confidence = 0
    value_thesis = 0
    confidence_thesis = 0
    rows = 0
    for run in population.runs.values():
        for stored in run.values():
            rows += 1
            rebuilt = ra.pre_clip_total(stored.row, weights) + stored.row.deductions - stored.row.additions
            raw_gap = max(raw_gap, abs(stored.raw_total - rebuilt))
            pre_clip = stored.raw_total - stored.row.deductions + stored.row.additions
            if not 0.0 <= pre_clip <= 1.0:
                clipped += 1
            if stored.total_score is not None:
                total_gap = max(total_gap, abs(stored.total_score - ra.rebuilt_key(stored.row, weights)))
            if stored.row.families["confidence"] == _NEUTRAL_CONFIDENCE:
                neutral_confidence += 1
            value_thesis += stored.value_from_thesis
            confidence_thesis += stored.confidence_from_thesis
    return {
        "rows": rows,
        "max_abs_raw_total_minus_weighted_sum": raw_gap,
        "max_abs_total_score_minus_key_full": total_gap,
        "clipped_rows": clipped,
        "confidence_at_neutral_share": neutral_confidence / rows if rows else None,
        "branch_rows_value_thesis_path": value_thesis,
        "branch_rows_confidence_from_thesis": confidence_thesis,
    }


def census(conn: psycopg.Connection[Any], readout_date: date) -> dict[str, Any]:
    out: dict[str, Any] = {"readout_date": readout_date, "model_version": ra.MODEL_VERSION}
    out["premise"] = {
        name.removeprefix("premise."): reader.execute(conn, name)[0][0]
        for name in reader.ROUTE_F_SQL
        if name.startswith("premise.")
    }

    population = reader.load_population(conn)
    out["population"] = {
        "runs": len(population.runs),
        "valid_rows": sum(len(run) for run in population.runs.values()),
        "names_valid": len({iid for run in population.runs.values() for iid in run}),
        "excluded_rows": population.excluded_rows,
        "excluded_names": population.excluded_names,
        "scored_per_run_min": min((len(run) for run in population.runs.values()), default=None),
        "scored_per_run_max": max((len(run) for run in population.runs.values()), default=None),
    }
    out["reconciliation"] = _reconciliation(population)

    witnessed = reader.witness_runs(population.runs, reader.load_job_windows(conn))
    if not witnessed.runs:
        out["refused"] = "no witnessed run"
        return out
    first_day = min(run.scored_at for run in witnessed.runs).astimezone(UTC).date() - timedelta(days=10)
    sessions = ra.nyse_sessions(first_day, readout_date)
    run_map = ra.map_runs(witnessed.runs, sessions)
    out["timeline"] = {
        "witnessed": len(witnessed.runs),
        "refused": Counter(witnessed.refused.values()),
        "max_known_minus_scored_seconds": max((r.known_at - r.scored_at).total_seconds() for r in witnessed.runs),
        "entry_sessions": len(run_map.entries),
        "superseded": [r.scored_at for r in run_map.superseded],
        "beyond_calendar": [r.scored_at for r in run_map.beyond_calendar],
    }

    if not run_map.entries:
        out["refused"] = "no witnessed run enters before the readout date"
        return out
    index = {day: ordinal for ordinal, day in enumerate(sessions)}
    first_entry = min(run_map.entries)
    first_formation = sessions[index[first_entry] - ra.LAG]
    cutoff = reader.cutoff_session(readout_date, sessions)
    if cutoff is None:
        out["refused"] = "no session before the provisional window"
        return out
    grid = ra.reporting_grid(sessions, first_formation=first_formation, cutoff=cutoff)
    grid_out: dict[str, Any] = {"first_formation": first_formation, "cutoff_c_k": cutoff}
    out["grid"] = grid_out
    if isinstance(grid, StatRefused):
        grid_out["refused"] = grid.reason
        return out
    observations = len(grid.sessions)
    lag = hunt_lag(observations, ra.H)
    grid_out.update(
        {
            "g_k": sessions[grid.last],
            "formations": len(grid.formations),
            "T": observations,
            "L": lag,
            "L_ge_T": lag >= observations,
            "2L_ge_T": 2 * lag >= observations,
            "short_sample_floor_T_over_L": observations / lag,
            "short_sample_floor_required": SHORT_SAMPLE_LAGS,
        }
    )

    names = sorted({iid for run in population.runs.values() for iid in run})
    series = reader.load_series(conn, names, first_formation=first_formation, cutoff=cutoff, as_of=readout_date)
    out["prices"] = _price_census(series, names, cutoff)

    links = reader.load_form25_links(conn, names, first_formation=first_formation, cutoff=cutoff)
    stopped = reader.stopped_series(series, cutoff)
    classes = reader.termination_classes(stopped, links=links, symbols=population.symbols)
    out["termination"] = {
        "stopped_before_c_k": len(stopped),
        "form25_links_in_window": len(links),
        "q_suffix": sorted(population.symbols[i] for i, c in classes.items() if c == TerminationClass.Q_SUFFIX_OTC),
        "terminating": sum(1 for c in classes.values() if c != TerminationClass.UNKNOWN),
        "classes": Counter(str(c) for c in classes.values()),
    }
    ((ex_dates, dividend_names),) = reader.execute(
        conn, "dividend_events", {"start": first_formation, "end": cutoff, "ids": names}
    )
    out["dividend_events_in_window"] = {"ex_dates": ex_dates, "names": dividend_names}

    out["formations"] = _formation_census(population, run_map, sessions, grid.formations, series)
    out["identity"] = {
        "price_input_sha256": reader.input_sha256(series),
        "quarantine_rule_set_version": QUARANTINE_RULE_SET_VERSION,
    }
    return out


def _price_census(series: dict[int, reader.ReadSeries], names: list[int], cutoff: date) -> dict[str, Any]:
    null_volume_names = 0
    null_volume_bars = 0
    some_null_names = 0
    high_not_above_low = 0
    bar_rules: Counter[str] = Counter()
    transition_rules: Counter[str] = Counter()
    corroboration: Counter[str] = Counter()
    provisional_through_cutoff = 0
    for read in series.values():
        nulls = sum(1 for bar in read.bars if bar.volume is None)
        if nulls == len(read.bars):
            null_volume_names += 1
            null_volume_bars += nulls
        elif nulls:
            some_null_names += 1
        high_not_above_low += sum(
            1 for bar in read.bars if bar.high is not None and bar.low is not None and bar.high <= bar.low
        )
        for verdict in read.verdicts.bars:
            bar_rules.update(verdict.rules)
            if verdict.provisional and verdict.price_date <= cutoff:
                provisional_through_cutoff += 1
        for transition in read.verdicts.transitions:
            transition_rules.update(transition.rules)
            if transition.corroboration != "not_applicable":
                corroboration[transition.corroboration] += 1
    return {
        "names_requested": len(names),
        "names_with_bars": len(series),
        "names_all_null_volume": null_volume_names,
        "bars_in_all_null_volume_names": null_volume_bars,
        "names_some_null_volume": some_null_names,
        "bars_high_not_above_low": high_not_above_low,
        "bar_rules": bar_rules,
        "transition_rules": transition_rules,
        "t3_triggered_corroboration": corroboration,
        "provisional_bars_through_c_k": provisional_through_cutoff,
    }


def _formation_census(
    population: reader.Population,
    run_map: ra.RunMap,
    sessions: tuple[date, ...],
    formations: tuple[int, ...],
    series: dict[int, reader.ReadSeries],
) -> dict[str, Any]:
    """Control sizes, arm_full vs a stored-``total_score`` selection, and arm_full's entered
    formations. Reads bars on t (control) and opens on e (entry), never a return."""
    weights = ra.weights()
    by_day = {iid: {bar.price_date: bar for bar in read.masked} for iid, read in series.items()}
    control_sizes: list[int] = []
    differing = 0
    max_difference = 0
    entered: list[int] = []
    idle = 0
    for t in formations:
        run = run_map.entries.get(sessions[t + ra.LAG])
        if run is None:
            idle += 1
            continue
        rows = population.runs[run.scored_at]
        day = sessions[t]
        control = frozenset(
            iid
            for iid in rows
            if (bar := by_day.get(iid, {}).get(day)) is not None
            and ra.bar_valid(bar.open, bar.high, bar.low, bar.close, bar.volume)
        )
        if not control:
            idle += 1
            continue
        control_sizes.append(len(control))
        arm = select_arm({i: ra.rebuilt_key(rows[i].row, weights) for i in control}, sign=+1, fraction=ra.FRACTION)
        stored = {i: rows[i].total_score for i in control}
        if all(score is not None and math.isfinite(score) for score in stored.values()):
            by_total = select_arm(
                {i: float(s) for i, s in stored.items() if s is not None}, sign=+1, fraction=ra.FRACTION
            )
            if by_total != arm:
                differing += 1
                max_difference = max(max_difference, len(by_total ^ arm))
        entry_day = sessions[t + ra.LAG]
        if any((bar := by_day.get(i, {}).get(entry_day)) is not None and bar.open is not None for i in arm):
            entered.append(t)
    return {
        "active": len(control_sizes),
        "idle": idle,
        "control_min": min(control_sizes, default=None),
        "control_max": max(control_sizes, default=None),
        "arm_full_differs_from_total_score": differing,
        "max_symmetric_difference": max_difference,
        "sparse_arm_count": sparse_arm_count(entered, ra.H),
        "sparse_arm_floor_required": MIN_ARM_FORMATIONS,
    }


# ---------------------------------------------------------------------------
# Terms, freeze and readout (spec v8 "Declaration")
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Timeline:
    population: reader.Population
    witnessed: reader.WitnessedRuns
    sessions: tuple[date, ...]
    run_map: ra.RunMap
    first_formation: date
    cutoff: date
    grid: Grid


def load_timeline(conn: psycopg.Connection[Any], readout_date: date) -> Timeline | str:
    """Scores, witnesses, the calendar and the grid at ``readout_date``. Reads no price."""
    population = reader.load_population(conn)
    witnessed = reader.witness_runs(population.runs, reader.load_job_windows(conn))
    if not witnessed.runs:
        return "no_witnessed_run"
    first_day = min(run.scored_at for run in witnessed.runs).astimezone(UTC).date() - timedelta(days=10)
    sessions = ra.nyse_sessions(first_day, readout_date)
    run_map = ra.map_runs(witnessed.runs, sessions)
    if not run_map.entries:
        return "no_entry_before_readout_date"
    index = {day: ordinal for ordinal, day in enumerate(sessions)}
    first_formation = sessions[index[min(run_map.entries)] - ra.LAG]
    cutoff = reader.cutoff_session(readout_date, sessions)
    if cutoff is None:
        return "no_session_before_provisional_window"
    grid = ra.reporting_grid(sessions, first_formation=first_formation, cutoff=cutoff)
    if isinstance(grid, StatRefused):
        return grid.reason
    return Timeline(population, witnessed, sessions, run_map, first_formation, cutoff, grid)


def git_state() -> tuple[str, bool]:
    """(HEAD, dirty). Ignored files do not count (``git status --porcelain``)."""

    def git(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=terms.REPO_ROOT, capture_output=True, text=True, check=True).stdout

    return git("rev-parse", "HEAD").strip(), bool(git("status", "--porcelain").strip())


def generate_terms(conn: psycopg.Connection[Any], fact_date: date) -> dict[str, Any]:
    """Write a new sidecar: today's semantic terms and the floor facts at ``fact_date``."""
    timeline = load_timeline(conn, fact_date)
    if isinstance(timeline, str):
        return {"outcome": "refused", "reason": timeline}
    facts = terms.frozen_facts(
        fact_date=fact_date,
        entry_sessions=timeline.run_map.entries,
        first_entry=timeline.sessions[timeline.grid.first],
        g_k=timeline.sessions[timeline.grid.last],
    )
    sidecar = terms.write_sidecar(terms.semantic_terms(), facts)
    return {"outcome": "written", "path": sidecar.path, "sha256": sidecar.sha256, "frozen_facts": facts}


def _selected() -> terms.Sidecar | str:
    return terms.select_sidecar(terms.semantic_terms(), terms.load_sidecars())


def freeze(conn: psycopg.Connection[Any], *, dry_run: bool) -> tuple[int, dict[str, Any]]:
    """Freeze the selected sidecar's declaration (spec "Freeze"). Returns (exit code, record)."""
    commit, dirty = git_state()
    sidecar = _selected()
    if isinstance(sidecar, str):
        return 1, {"outcome": "refused", "reason": sidecar}
    declaration = terms.build_declaration(sidecar)
    summary: dict[str, Any] = {
        "commit": commit,
        "sidecar": sidecar.path,
        "declaration_sha256": declaration.sha256,
        "refusals": list(declaration_refusals(declaration)),
    }
    if dry_run:
        return 0, {**summary, **declaration.digest_payload, "dirty_tree": dirty, "outcome": "dry_run"}
    if dirty:
        return 1, {**summary, "outcome": "refused", "reason": "dirty_tree"}
    try:
        declaration_id = freeze_preregistration(conn, declaration)
        outcome = "frozen"
    except psycopg.errors.UniqueViolation:
        conn.rollback()
        stored = load_preregistration(conn, declaration.strategy_id, declaration.strategy_version)
        if stored is None or stored.declaration_sha256 != declaration.sha256:
            stored_sha = None if stored is None else stored.declaration_sha256
            return 1, {**summary, "outcome": "conflicting_declaration_already_frozen", "stored_sha256": stored_sha}
        declaration_id, outcome = stored.declaration_id, "already_frozen_identical"
    except PreregDeclarationRefused as refused:
        conn.rollback()
        return 1, {**summary, "outcome": "refused", "reason": list(refused.refusals)}
    ((frozen_at,),) = reader.execute(conn, "frozen_at", {"declaration_id": declaration_id})
    conn.commit()
    return 0, {**summary, "declaration_id": declaration_id, "frozen_at": frozen_at, "outcome": outcome}


@dataclass(frozen=True)
class Gate:
    sidecar: terms.Sidecar
    declaration_id: int
    frozen_at: datetime


def readout_gate(conn: psycopg.Connection[Any], *, dirty: bool) -> Gate | str:
    """Every condition of the spec's "Readout gate", or the first that fails."""
    if dirty:
        return "dirty_tree"
    sidecar = _selected()
    if isinstance(sidecar, str):
        return sidecar
    declaration = terms.build_declaration(sidecar)
    stored = load_preregistration(conn, declaration.strategy_id, declaration.strategy_version)
    if stored is None:
        return "not_frozen"
    if stored.declaration_sha256 != declaration.sha256:
        return "stored_declaration_differs"
    if declaration_refusals(declaration):
        return "declaration_refused"
    ((frozen_at,),) = reader.execute(conn, "frozen_at", {"declaration_id": stored.declaration_id})
    return Gate(sidecar, stored.declaration_id, frozen_at)


def load_inputs(conn: psycopg.Connection[Any], readout_date: date) -> readout_mod.Inputs | str:
    """The price reader: scores, timeline, masked series and termination, in the caller's snapshot."""
    timeline = load_timeline(conn, readout_date)
    if isinstance(timeline, str):
        return timeline
    names = sorted({iid for run in timeline.population.runs.values() for iid in run})
    series = reader.load_series(
        conn, names, first_formation=timeline.first_formation, cutoff=timeline.cutoff, as_of=readout_date
    )
    links = reader.load_form25_links(conn, names, first_formation=timeline.first_formation, cutoff=timeline.cutoff)
    termination = reader.termination_classes(
        reader.stopped_series(series, timeline.cutoff), links=links, symbols=timeline.population.symbols
    )
    return readout_mod.Inputs(
        population=timeline.population,
        run_map=timeline.run_map,
        sessions=timeline.sessions,
        cutoff=timeline.cutoff,
        series=series,
        termination=termination,
    )


def _append_look(handle: Any, record: dict[str, Any]) -> None:
    handle.write(json.dumps(record, default=_json_default, sort_keys=True) + "\n")
    handle.flush()
    os.fsync(handle.fileno())


def run_readout(conn: psycopg.Connection[Any], readout_date: date, *, looks_path: Path = LOOKS_PATH) -> dict[str, Any]:
    """Gate, then log the look, then read prices. A refused gate reads no price."""
    commit, dirty = git_state()
    gate = readout_gate(conn, dirty=dirty)
    if isinstance(gate, str):
        return {"outcome": "refused", "reason": gate}
    ident = terms.identity(gate.sidecar.sha256)
    looks_path.parent.mkdir(parents=True, exist_ok=True)
    with looks_path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)  # readouts serialise; released on close
        look = {
            "uuid": str(uuid.uuid4()),
            "trial_id": ident.trial_id,
            "commit": commit,
            "readout_date": readout_date,
            "started_at": datetime.now(UTC),
        }
        _append_look(handle, look)
        inputs = load_inputs(conn, readout_date)
        if isinstance(inputs, str):
            return {"outcome": "refused", "reason": inputs, "look": look}
        return {
            "outcome": "read",
            "look": look,
            "declaration_id": gate.declaration_id,
            "frozen_at": gate.frozen_at,
            "sidecar": gate.sidecar.path,
            "cutoff_c_k": inputs.cutoff,
            "vintage": {
                "price_input_sha256": reader.input_sha256(inputs.series),
                "quarantine_rule_set_version": QUARANTINE_RULE_SET_VERSION,
                "excluded_rows": inputs.population.excluded_rows,
                "superseded_runs": [run.scored_at for run in inputs.run_map.superseded],
            },
            "results": readout_mod.readout(inputs, frozen_at=gate.frozen_at),
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--census", action="store_true", help="print the premise and prices census; read-only")
    mode.add_argument("--terms", action="store_true", help="write a new terms sidecar (floor facts at --readout-date)")
    mode.add_argument("--freeze", action="store_true", help="freeze the selected sidecar's declaration")
    mode.add_argument("--readout", action="store_true", help="gate, log the look, then compute the readout")
    parser.add_argument("--dry-run", action="store_true", help="with --freeze: print the payload, write nothing")
    parser.add_argument("--readout-date", type=date.fromisoformat, default=datetime.now(UTC).date())
    args = parser.parse_args(argv)
    if args.dry_run and not args.freeze:
        parser.error("--dry-run applies to --freeze only")
    code = 0
    with psycopg.connect(settings.database_url) as conn:
        if args.freeze and not args.dry_run:
            code, result = freeze(conn, dry_run=False)
        else:
            conn.isolation_level = IsolationLevel.REPEATABLE_READ
            conn.read_only = True
            try:
                if args.census:
                    result = census(conn, args.readout_date)
                elif args.terms:
                    result = generate_terms(conn, args.readout_date)
                elif args.freeze:
                    code, result = freeze(conn, dry_run=True)
                else:
                    result = run_readout(conn, args.readout_date)
                    code = 0 if result["outcome"] == "read" else 1
            finally:
                conn.rollback()
    sys.stdout.write(json.dumps(result, default=_json_default, indent=2, sort_keys=True) + "\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
