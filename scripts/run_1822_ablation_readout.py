"""#1822 route F: the ablation readout script. Read-only.

Spec ``docs/proposals/ta/2026-09-28-1822-ranking-ablation.md`` (v6), slice 1b.

``--census`` prints the spec's premise table and the "Prices" figures, recomputed from the
store in one REPEATABLE READ snapshot that is always rolled back. It reads prices to compute
verdicts, the control and entry availability, and it NEVER computes a forward return: the
declaration must freeze before any is read (spec "Declaration"). ``--freeze`` and
``--readout`` land in the next slice.

    PYTHONPATH=. uv run python scripts/run_1822_ablation_readout.py --census [--readout-date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg
from psycopg import IsolationLevel

from app.config import settings
from app.services import ranking_ablation as ra
from app.services import ranking_ablation_reader as reader
from app.services.hunt_evaluator import select_arm
from app.services.hunt_inference import MIN_ARM_FORMATIONS, SHORT_SAMPLE_LAGS, StatRefused, hunt_lag, sparse_arm_count
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.series_termination import TerminationClass

_PREMISE_SQL = {
    "financial_facts_min_filed_date": "SELECT min(filed_date) FROM financial_facts_raw",
    "financial_facts_instruments_filed_before_2009": (
        "SELECT count(DISTINCT instrument_id) FROM financial_facts_raw WHERE filed_date < DATE '2009-01-01'"
    ),
    "theses_min_created_at": "SELECT min(created_at) FROM theses",
    "theses_instruments": "SELECT count(DISTINCT instrument_id) FROM theses",
    "news_events_min_event_time": "SELECT min(event_time) FROM news_events",
}
_DIVIDEND_SQL = """
SELECT count(*), count(DISTINCT instrument_id) FROM dividend_events
WHERE ex_date BETWEEN %(start)s AND %(end)s AND instrument_id = ANY(%(ids)s::bigint[])
"""
#: Confidence's neutral value when no thesis exists (spec "Premise check", "0.5 on 89.9%").
_NEUTRAL_CONFIDENCE = 0.5


def _json_default(value: object) -> str:
    return value.isoformat() if isinstance(value, date | datetime) else str(value)


def _reconciliation(population: reader.Population) -> dict[str, Any]:
    """raw_total vs Σ w·s, total_score vs clip(raw_total − P + R), over every valid row."""
    weights = ra.weights()
    raw_gap = 0.0
    total_gap = 0.0
    clipped = 0
    neutral_confidence = 0
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
    return {
        "rows": rows,
        "max_abs_raw_total_minus_weighted_sum": raw_gap,
        "max_abs_total_score_minus_key_full": total_gap,
        "clipped_rows": clipped,
        "confidence_neutral_share": neutral_confidence / rows if rows else None,
    }


def census(conn: psycopg.Connection[Any], readout_date: date) -> dict[str, Any]:
    out: dict[str, Any] = {"readout_date": readout_date, "model_version": ra.MODEL_VERSION}
    out["premise"] = {key: conn.execute(sql).fetchone()[0] for key, sql in _PREMISE_SQL.items()}  # type: ignore[index]

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
    dividends = conn.execute(_DIVIDEND_SQL, {"start": first_formation, "end": cutoff, "ids": names}).fetchone()
    out["dividend_events_in_window"] = {"ex_dates": dividends[0], "names": dividends[1]} if dividends else None

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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--census", action="store_true", help="print the premise and prices census; read-only")
    parser.add_argument("--readout-date", type=date.fromisoformat, default=datetime.now(UTC).date())
    args = parser.parse_args(argv)
    if not args.census:
        parser.error("slice 1b ships --census only; --freeze and --readout land next")
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        try:
            result = census(conn, args.readout_date)
        finally:
            conn.rollback()
    sys.stdout.write(json.dumps(result, default=_json_default, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
