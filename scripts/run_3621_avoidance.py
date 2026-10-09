"""#3621 slice 4: the declaration, the hold-out access and the run.

Spec: ``docs/research/2026-10-08-3621-avoidance-filters.md`` §"Registration", §"Samples and design history" (Access)
and §"Slices" item 4 (PR #3724). Three modes:

* ``--print-declaration`` prints the register row's ``evidence`` for this checkout. It reads no artefact.
* ``--declare`` refuses if the trial is already in the register or the committed ledger, then appends the committed
  ``declared`` row pinning that register row's payload sha256 (step 2's pattern). The declaration PR adds the row
  verbatim, with a ``TRIAL_REGISTER_VERSION`` bump.
* ``--accessed-by <identity>`` is the run. It reads nothing before the gate and the access row:

  1. a clean checkout at ``origin/main`` after a fetch (``report_head``);
  2. the gate (:func:`check_declaration`): one register row, its committed ``declared`` pin, every ``evidence`` value
     equal to this checkout's (spec, construction and register-policy hashes, Python, step 0's manifest and the
     artefact pins), and no completed run. A refusal writes no row;
  3. ``started``, then the ``evaluate`` hold-out access, committed, then ``access_recorded``;
  4. both artefacts, verified (stage A pinned, stage B through step 2's committed capture), then the reproduction
     check: slice 1's module must give premise 1's per-formation counts (``measure_3621_filter_premise``) exactly on
     stage A, or the run stops;
  5. MAX fidelity, the 30 pairs and the pooled diagnostic, the report and the names file, ``report_written`` and
     ``completed``. Any failure after ``started`` ends the run ``failed``; a retry is a new run with its own access.

The construction hash is this module's import closure, so it covers slices 1-3, step 2's loaders and books and the
premise script (``CONSTRUCTION_ROOTS``).
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services.avoidance_filters import (
    FILTER_SETS,
    JKP_CODE_COMMIT,
    Filter,
    MaxMissing,
    MaxReading,
    MaxSeries,
    NameFlags,
    NameInputs,
    flag_formation,
)
from app.services.factor_book_declaration import construction_sha256, payload_sha256, register_policy_sha256
from app.services.factor_book_ledger import end_run_failed
from app.services.factor_book_path import PathResult
from app.services.factor_book_reference import load_ff12
from app.services.factor_book_series import ARMS
from app.services.factor_panel_artefact import sha256_file
from app.services.factor_panel_fidelity import append_ledger, month_last_day, read_ledger
from app.services.factor_panel_prices import DailyBar
from app.services.factor_panel_reference import parse_table9_signs
from app.services.result_ledger import HoldoutAccess, record_holdout_access
from app.services.strategy_result import AmbiguityArm
from app.services.trial_register import (
    TRIAL_REGISTER,
    TRIAL_REGISTER_VERSION,
    DeclaredTrial,
    TrialExactness,
    TrialRegister,
)
from scripts.build_3609_factor_panel import Frozen, VerifiedArtefact, read_verified_artefact
from scripts.capture_3609_step2 import capture_path, ledger_claim, verify_capture, write_exclusive
from scripts.measure_3621_filter_premise import stage_a_counts
from scripts.report_3609_step2 import (
    ADMITTED,
    CONSUMED_INPUTS,
    STAGE_A_ARTEFACT,
    STAGE_A_MANIFEST_SHA256,
    TABLE9_SIGNS,
    PanelMonth,
    read_panel,
)
from scripts.report_3609_step2_assembly import jsonable
from scripts.report_3609_step2_inputs import STEP0_MANIFEST, STEP0_RUN
from scripts.report_3609_step2_run import report_head
from scripts.report_3609_step2_universe import NYSE_CUTOFFS
from scripts.report_3609_step2_verdict import BASE
from scripts.report_3621_books import (
    SCENARIOS,
    VERDICT_POPULATIONS,
    PairResult,
    Population,
    book_paths,
    pair_result,
    populations,
    targets,
)
from scripts.report_3621_diagnostics import (
    SIZE_SEGMENTS,
    BookStats,
    ExcludedName,
    Undefined,
    Window,
    book_stats,
    cell_counts,
    differential,
    formation_counts,
    holding_month,
    names_file,
    screened_targets,
    spread,
    windows,
)
from scripts.report_3621_fidelity import (
    MAX_CHARACTERISTIC,
    STAGE_A_FORMATIONS,
    FidelityFormation,
    MaxFidelity,
    max_fidelity,
    max_holdings,
)

TRIAL_ID: Final = "3621-avoidance-filters-v1"
STRATEGY_ID: Final = "3621-avoidance-filters"
STRATEGY_VERSION: Final = "v1"
#: §"Registration": 5 filter sets x 6 populations x 2 arms, plus the MAX fidelity check's 2 arms.
SEARCHES: Final = len(FILTER_SETS) * len(VERDICT_POPULATIONS) * len(ARMS) + len(ARMS)
DECLARED_EVENT: Final = "declared"

_REPO_ROOT: Final = Path(__file__).resolve().parents[1]
SPEC_PATH: Final = _REPO_ROOT / "docs" / "research" / "2026-10-08-3621-avoidance-filters.md"
CONSTRUCTION_ROOTS: Final = (Path(__file__).resolve(),)
LEDGER_PATH: Final = _REPO_ROOT / "var" / "research" / "3621" / "ledger.jsonl"
COMMITTED_LEDGER_PATH: Final = _REPO_ROOT / "docs" / "research" / "3621-ledger.jsonl"
#: Stage B is step 2's committed capture (``docs/research/3609-ledger.jsonl``, ``data_frozen``).
STAGE_B_CAPTURE_RUN: Final = "2039b95f8d2d4e0fb11509addc64ec2f"
STAGE_B_CAPTURE_SHA256: Final = "932c2f94f7a0b45539d00faa4afef34520b9bef18ceb56fb389c3bdb0e7ea228"
STAGE_B_MANIFEST_SHA256: Final = "3eee10059e817724c3951496b6a25494398dedfd417a5f50b78034b1894c95bc"

DAILY: Final = f"inputs/{Frozen.DAILY}"
SESSIONS: Final = f"inputs/{Frozen.SESSIONS}"
JKP_RETURNS: Final = f"inputs/{Frozen.snapshot('jkp_usa_monthly_vw_cap')}"
STAGE_KEEP: Final = (*CONSUMED_INPUTS, NYSE_CUTOFFS, DAILY, SESSIONS)
_CUTOFF_KEYS: Final = ("nyse_p20", "nyse_p50", "nyse_p80")

SPEC_REFERENCE: Final = 'docs/research/2026-10-08-3621-avoidance-filters.md §"Registration" and §"Slices" item 4'
DESCRIPTION: Final = (
    "#3621 avoidance filters, version 1: a preregistered, non-claiming enumeration on the #3609 panel (step 1's "
    "restricted population, formations 2014-09..2024-07 over stages A and B, net of step 0's costs). Five filter sets "
    "(MAX, sub-$5, young, sub-$5 + young, all three) x six populations (micro, small, large, mega, top 1,000, rest) "
    "x two termination arms = 60, plus the MAX fidelity check's two arms against JKP's rmax1_21d = 62. A pair's "
    "verdict (ELIGIBLE, NOT ELIGIBLE, NO_EFFECT or REFUSED, zero margin, point estimates) only decides whether a "
    "later long-book spec may cite the set. No premium or non-inferiority is claimed, so no TrialDesign. "
    "Retrospective: both stages had been read before this declaration."
)
LABELS: Final = (
    "retrospective evidence: stage A is #3609's development sample and stage B reused validation; nothing is sealed",
    "every verdict is eligible under the spread-only model (step 0's bands and stress multiplier; no gap, slippage, "
    "minimum-ticket or fixed-fee term)",
    "MAX fidelity checks that the reconstructed factor-return series tracks JKP's rmax1_21d; it shows nothing about "
    "name-level ranking agreement, the top-decile threshold or micro caps",
    "restricted population: 10-K/10-Q filers our archive prices and our linkage identifies, not CRSP; survivorship "
    "unverified 2014-09..2018",
)


class RunError(RuntimeError):
    """The declaration, the artefacts or the reproduction check do not hold; the run stops."""


# --------------------------------------------------------------------------- the declaration


def current_labels() -> dict[str, str]:
    """Every ``label=value`` the register row's ``evidence`` names, for this checkout."""
    return {
        "spec_sha256": sha256_file(SPEC_PATH),
        "construction_sha256": construction_sha256(CONSTRUCTION_ROOTS, _REPO_ROOT),
        "register_policy_sha256": register_policy_sha256((_REPO_ROOT / "app/services/trial_register.py").read_bytes()),
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
        "stage_a_manifest_sha256": STAGE_A_MANIFEST_SHA256,
        "stage_b_capture_sha256": STAGE_B_CAPTURE_SHA256,
        "stage_b_manifest_sha256": STAGE_B_MANIFEST_SHA256,
        "step0_manifest_sha256": sha256_file(STEP0_RUN / STEP0_MANIFEST),
        "jkp_code_commit": JKP_CODE_COMMIT,
        "holdout_strategy_id": STRATEGY_ID,
        "holdout_strategy_version": STRATEGY_VERSION,
    }


def declaration_evidence(labels: Mapping[str, str]) -> str:
    pins = [f"{label}={value}" for label, value in labels.items()]
    return "; ".join([SPEC_REFERENCE, *pins, "ledger docs/research/3621-ledger.jsonl"])


def declared_trial(evidence: str) -> DeclaredTrial:
    return DeclaredTrial(
        trial_id=TRIAL_ID, description=DESCRIPTION, evidence=evidence, exactness=TrialExactness.EXACT, searches=SEARCHES
    )


def evidence_value(evidence: str, label: str) -> str:
    found = re.findall(rf"(?:^|[;\s]){re.escape(label)}=([^;\s]+)", evidence)
    if len(found) != 1:
        raise RunError(f"{TRIAL_ID} evidence names {label!r} {len(found)} times; exactly once is required")
    return found[0]


def require_undeclared(register: TrialRegister, committed: Sequence[Mapping[str, Any]]) -> None:
    rows = [t for t in register.trials if t.trial_id == TRIAL_ID]
    declared = [r for r in committed if r.get("event") == DECLARED_EVENT and r.get("trial_id") == TRIAL_ID]
    if rows or declared:
        raise RunError(f"{TRIAL_ID} is already declared: {len(rows)} register rows, {len(declared)} ledger rows")


def declared_row(trial: DeclaredTrial) -> dict[str, Any]:
    return {
        "event": DECLARED_EVENT,
        "trial_id": trial.trial_id,
        "payload_sha256": payload_sha256(trial),
        "at": datetime.now(UTC).isoformat(),
    }


def check_declaration(
    register: TrialRegister,
    committed: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    labels: Mapping[str, str],
) -> DeclaredTrial:
    """The declared trial, once its row, its committed payload pin and this checkout agree and no run completed."""
    matches = [t for t in register.trials if t.trial_id == TRIAL_ID]
    if len(matches) != 1:
        raise RunError(f"{len(matches)} register rows named {TRIAL_ID}; exactly one is required")
    (trial,) = matches
    if (
        trial.declared_for is not None
        or trial.design is not None
        or trial.exactness is not TrialExactness.EXACT
        or trial.searches != SEARCHES
    ):
        raise RunError(f"{TRIAL_ID} is not a non-claiming exact {SEARCHES}-search row")
    declared = [r for r in committed if r.get("event") == DECLARED_EVENT and r.get("trial_id") == TRIAL_ID]
    if len(declared) != 1 or declared[0].get("payload_sha256") != payload_sha256(trial):
        raise RunError(f"{TRIAL_ID}: the committed ledger does not hold one 'declared' row pinning this register row")
    moved = sorted(label for label, value in labels.items() if evidence_value(trial.evidence, label) != value)
    if moved:
        raise RunError(f"this checkout's {moved} differ from {TRIAL_ID}'s declared values")
    if any(r.get("event") == "completed" for r in rows):
        raise RunError(f"{TRIAL_ID} already has a completed run; a changed study needs a new trial")
    return trial


# --------------------------------------------------------------------------- reading the artefacts


def _lines(payload: bytes) -> Iterator[Any]:
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as handle:
        for line in handle:
            yield json.loads(line)


def read_nyse_cutoffs(payload: bytes) -> dict[date, tuple[float, float, float]]:
    """(p20, p50, p80) in USD by month end, from the frozen JKP snapshot (USD millions)."""
    found: dict[str, dict[date, float]] = {key: {} for key in _CUTOFF_KEYS}
    for key, day, value, unit in _lines(payload):
        if key in found:
            if unit != "usd_millions":
                raise RunError(f"{key} {day} unit {unit!r}, expected usd_millions")
            found[key][date.fromisoformat(day)] = float(value) * 1e6
    days = set.intersection(*(set(v) for v in found.values()))
    return {d: (found["nyse_p20"][d], found["nyse_p50"][d], found["nyse_p80"][d]) for d in days}


def read_published_max(payload: bytes) -> dict[str, float]:
    """JKP's published ``rmax1_21d`` by ``YYYY-MM``; every row must be dated the month's last day."""
    out: dict[str, float] = {}
    for name, day, value, unit in _lines(payload):
        if name != MAX_CHARACTERISTIC:
            continue
        when = date.fromisoformat(day)
        key = f"{when.year:04d}-{when.month:02d}"
        if when != month_last_day(key) or unit != "decimal_return" or key in out:
            raise RunError(f"{name} row {(day, unit)} is not one month-end decimal return")
        out[key] = float(value)
    return out


def max_readings(
    daily: bytes, sessions: Sequence[date], months: Sequence[PanelMonth]
) -> dict[date, dict[int, MaxReading]]:
    """Every admitted name's ``rmax1_21d`` reading at its formation, from one pass over the frozen daily bars."""
    position = {day: i for i, day in enumerate(sessions)}
    wanted: dict[int, list[tuple[date, int, int]]] = {}
    for month in months:
        if month.session not in position:
            raise RunError(f"s(M) {month.session} is not a frozen SPY session")
        for name, panel in month.admitted.items():
            wanted.setdefault(panel.series_id, []).append((month.formation, name, position[month.session]))
    out: dict[date, dict[int, MaxReading]] = {m.formation: {} for m in months}
    for series_id, raw in _lines(daily):
        if series_id not in wanted:
            continue
        bars = [DailyBar(date.fromisoformat(d), c, a, v, stamped, ok) for d, c, a, v, stamped, ok in raw]
        series = MaxSeries(bars, sessions)
        for formation, name, k in wanted[series_id]:
            out[formation][name] = series.at(k)
    short = [(m.formation, n) for m in months for n in m.admitted if n not in out[m.formation]]
    if short:
        raise RunError(f"{len(short)} admitted names have no frozen daily bars: {short[:3]}")
    return out


def formation_flags(month: PanelMonth, readings: Mapping[int, MaxReading]) -> dict[int, NameFlags]:
    names: dict[int, NameInputs] = {}
    for name in month.admitted:
        close, first = month.close.get(name), month.first_bar.get(name)
        if close is None or first is None:
            raise RunError(f"{month.formation}: admitted name {name} has no raw close or first bar")
        names[name] = NameInputs(readings[name], close, first)
    return flag_formation(names, month.session)[1]


@dataclass(frozen=True)
class Stage:
    months: list[PanelMonth]
    flags: list[dict[int, NameFlags]]
    readings: list[dict[int, MaxReading]]
    cutoffs: dict[date, tuple[float, float, float]]
    symbols: dict[int, str | None]


def load_stage(verified: VerifiedArtefact) -> Stage:
    months = read_panel(verified, load_ff12())
    sessions = [date.fromisoformat(d) for d in _lines(verified.files[SESSIONS])]
    readings = max_readings(verified.files[DAILY], sessions, months)
    return Stage(
        months=months,
        flags=[formation_flags(m, readings[m.formation]) for m in months],
        readings=[readings[m.formation] for m in months],
        cutoffs=read_nyse_cutoffs(verified.files[NYSE_CUTOFFS]),
        symbols={int(line["series_id"]): line["symbol"] for line in _lines(verified.files[ADMITTED])},
    )


# --------------------------------------------------------------------------- the reproduction check

#: Premise 1's flag columns, as ``measure_3621_filter_premise.FLAGS`` names them.
PREMISE_FLAGS: Final = ("admitted", "max", "max_screened", "max_short", "max_zero_heavy", "sub5", "young", "any")


def module_counts(
    month: PanelMonth, flags: Mapping[int, NameFlags], cutoffs: tuple[float, float, float]
) -> dict[str, dict[str, int]]:
    """Premise 1's counts for one formation, computed from slice 1's flags and slice 2a's populations."""
    pops = populations({name: panel.me for name, panel in month.admitted.items()}, cutoffs)
    out: dict[str, dict[str, int]] = {}
    for population in VERDICT_POPULATIONS:
        rows = [flags[n] for n in pops[population]]
        missing = [f.reading.missing for f in rows]
        out[str(population)] = {
            "admitted": len(rows),
            "max": sum(Filter.MAX in f.flagged for f in rows),
            "max_screened": missing.count(MaxMissing.SCREENED),
            "max_short": missing.count(MaxMissing.SHORT),
            "max_zero_heavy": missing.count(MaxMissing.ZERO_HEAVY),
            "sub5": sum(Filter.SUB5 in f.flagged for f in rows),
            "young": sum(Filter.YOUNG in f.flagged for f in rows),
            "any": sum(bool(f.flagged) for f in rows),
        }
    return out


def reproduce(expected: tuple[list[str], list[dict[str, dict[str, int]]]], stage: Stage) -> int:
    """Refuses unless slice 1's counts equal premise 1's on every stage-A formation; returns the formations checked."""
    grid, table = expected
    if [m.formation.isoformat() for m in stage.months] != grid:
        raise RunError("the stage-A panel's formations are not premise 1's grid")
    differ = []
    for month, flags, counts in zip(stage.months, stage.flags, table, strict=True):
        got = module_counts(month, flags, stage.cutoffs[month.formation])
        theirs = {p: {f: counts[p][f] for f in PREMISE_FLAGS} for p in got}
        if got != theirs:
            differ.append(month.formation.isoformat())
    if differ:
        raise RunError(f"REPRODUCTION: slice 1's counts differ from premise 1's at {len(differ)} formations: {differ}")
    return len(grid)


# --------------------------------------------------------------------------- the evaluation


def fidelity(stage_a: Stage, published: Mapping[str, float], sign: int) -> MaxFidelity:
    formations = [
        FidelityFormation(
            m.formation,
            max_holdings(readings, m.admitted),
            stage_a.cutoffs[m.formation][0],
            stage_a.cutoffs[m.formation][2],
        )
        for m, readings in zip(stage_a.months, stage_a.readings, strict=True)
        if m.formation in STAGE_A_FORMATIONS
    ]
    return max_fidelity(formations, published, sign)


def set_label(filter_set: frozenset[Filter]) -> str:
    return "+".join(str(f) for f in Filter if f in filter_set)


Paths = Mapping[tuple[AmbiguityArm, str], PathResult]


def _shown(value: object) -> object:
    """An exhausted book's statistic prints as §"Decision rule" words it."""
    return str(value) if isinstance(value, Undefined) else value


def _window_block(u: Paths, other: Paths, w: Window, flagged: Paths | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for arm in ARMS:
        for cost in SCENARIOS:
            key = (arm, cost)
            mine, theirs = book_stats(other[key], w), book_stats(u[key], w)
            cell: dict[str, Any] = {
                "filtered": _shown(mine),
                "unfiltered": _shown(theirs),
                "D": _shown(differential(u[key], other[key], w)),
            }
            if flagged is not None:
                cell["flagged"] = _shown(book_stats(flagged[key], w))
            if isinstance(mine, BookStats) and isinstance(theirs, BookStats):
                cell["turnover_difference"] = mine.turnover_per_year - theirs.turnover_per_year
                cell["cost_difference"] = mine.cost_per_year - theirs.cost_per_year
            out[f"{arm}|{cost}"] = cell
    return out


def pair_block(
    months: Sequence[PanelMonth],
    members: Sequence[frozenset[int]],
    flags: Sequence[Mapping[int, NameFlags]],
    filter_set: frozenset[Filter],
    u: Paths,
    max_passed: bool,
) -> tuple[PairResult, dict[str, Any]]:
    sets = [targets(p, f, filter_set) for p, f in zip(members, flags, strict=True)]
    uf = book_paths(months, [t.filtered for t in sets])
    bf = book_paths(months, [t.flagged for t in sets])
    excluded = {m.formation: len(t.flagged) for m, t in zip(months, sets, strict=True)}
    result = pair_result(u, uf, excluded, has_max=Filter.MAX in filter_set, max_fidelity_passed=max_passed)
    counts = [
        (holding_month(m.formation), formation_counts(p, f, filter_set))
        for m, p, f in zip(months, members, flags, strict=True)
    ]
    by_window: dict[str, Any] = {}
    for w in windows(list(u[(ARMS[0], BASE)].returns)):
        inside = [c for hm, c in counts if hm in w.months]
        by_window[w.label] = {
            "first": w.months[0],
            "last": w.months[-1],
            "months": len(w.months),
            "partial": w.partial,
            "formations_with_exclusions": sum(
                1 for m, t in zip(months, sets, strict=True) if t.flagged and holding_month(m.formation) in w.months
            ),
            "per_formation": {
                field: spread(getattr(c, field) for c in inside)
                for field in (
                    "admitted",
                    "max_above_cutoff",
                    "max_screened",
                    "max_short",
                    "max_zero_heavy",
                    "sub5",
                    "young",
                    "excluded_weight",
                )
            },
            "books": _window_block(u, uf, w, bf),
        }
    return result, {"windows": by_window}


def evaluate(stages: Sequence[Stage], fid: MaxFidelity) -> dict[str, Any]:
    """Every (F, P) including the pooled population: the verdicts, diagnostics, cell counts and excluded names."""
    months = [m for s in stages for m in s.months]
    flags = [f for s in stages for f in s.flags]
    pops = [
        populations({n: p.me for n, p in m.admitted.items()}, s.cutoffs[m.formation]) for s in stages for m in s.months
    ]
    symbols = {k: v for s in stages for k, v in s.symbols.items()}
    pairs: dict[str, Any] = {}
    screened: dict[str, Any] = {}
    for population in Population:
        members = [p[population] for p in pops]
        u = book_paths(months, members)
        only = book_paths(months, [screened_targets(p, f).filtered for p, f in zip(members, flags, strict=True)])
        screened[str(population)] = {
            w.label: _window_block(u, only, w) for w in windows(list(u[(ARMS[0], BASE)].returns))
        }
        for filter_set in FILTER_SETS:
            result, block = pair_block(months, members, flags, filter_set, u, fid.passed)
            verdict = population in VERDICT_POPULATIONS
            pairs[f"{set_label(filter_set)}|{population}"] = {
                "verdict": result if verdict else None,
                "pooled_delta_g": None if verdict else result.delta_g,
                **block,
            }
    cells = {
        set_label(fs): {
            m.formation: cell_counts(m, {s: p[s] for s in SIZE_SEGMENTS}, f, fs)
            for m, p, f in zip(months, pops, flags, strict=True)
        }
        for fs in FILTER_SETS
    }
    excluded = [
        ExcludedName(
            m.formation, name, symbols.get(m.admitted[name].series_id) or "", population, tuple(sorted(f[name].flagged))
        )
        for m, p, f in zip(months, pops, flags, strict=True)
        for population in Population
        for name in p[population]
        if f[name].flagged
    ]
    return {"pairs": pairs, "screened_only": screened, "cells": cells, "excluded": excluded}


def verdict_lines(pairs: Mapping[str, Any], fid: MaxFidelity) -> list[str]:
    lines = []
    for key, block in pairs.items():
        result = block["verdict"]
        if result is None:
            continue
        failed = f" ({', '.join(result.failed)})" if result.failed else ""
        label = " [no stage-B exclusions]" if result.no_stage_b_exclusions else ""
        lines.append(f"{key}: {result.verdict.value}{failed}{label}")
    lines.append(f"MAX fidelity: {fid.verdict.value}")
    return lines


# --------------------------------------------------------------------------- the run


RUN_CLAIM: Final = "run"
_TERMINAL: Final = frozenset({"completed", "failed"})


def open_runs(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    """Run ids with a ``started`` row and no terminal row."""
    started = [r["run_id"] for r in rows if r.get("event") == "started"]
    ended = {r.get("run_id") for r in rows if r.get("event") in _TERMINAL}
    return [run_id for run_id in started if run_id not in ended]


def run(
    run_id: str,
    *,
    head: str,
    command: Sequence[str],
    accessed_by: str,
    record_access: Callable[[HoldoutAccess], int],
    evaluate_run: Callable[[], tuple[dict[str, Any], bytes]],
    register: TrialRegister = TRIAL_REGISTER,
    ledger: Path = LEDGER_PATH,
    committed_ledger: Path = COMMITTED_LEDGER_PATH,
    labels: Callable[[], dict[str, str]] = current_labels,
) -> dict[str, Any]:
    """The gate, then ``started``, the access and the evaluation; returns the ``completed`` row.

    The whole run holds the ledger's run claim, so a second invocation waits and then meets the first one's terminal
    row: two runs can never both pass the gate. A run left open (killed between ``started`` and its terminal row)
    refuses every later run until it is ended by hand with a ``failed`` row."""
    with ledger_claim(ledger, RUN_CLAIM):
        current = labels()
        committed = read_ledger(committed_ledger)
        rows = [*committed, *(row for row in read_ledger(ledger) if row not in committed)]
        trial = check_declaration(register, committed, rows, current)
        if open_runs(rows):
            raise RunError(f"runs {open_runs(rows)} started and never ended; end each with a 'failed' row first")
        return _run_claimed(run_id, trial, current, head, command, accessed_by, record_access, evaluate_run, ledger)


def _run_claimed(
    run_id: str,
    trial: DeclaredTrial,
    current: Mapping[str, str],
    head: str,
    command: Sequence[str],
    accessed_by: str,
    record_access: Callable[[HoldoutAccess], int],
    evaluate_run: Callable[[], tuple[dict[str, Any], bytes]],
    ledger: Path,
) -> dict[str, Any]:
    out = ledger.parent / f"{run_id}.json"
    names_path = ledger.parent / f"{run_id}-names.jsonl.gz"
    step = "started"
    try:
        append_ledger(
            ledger,
            {
                "run_id": run_id,
                "event": step,
                "at": datetime.now(UTC).isoformat(),
                "git_sha": head,
                **current,
                "payload_sha256": payload_sha256(trial),
                "register_version": TRIAL_REGISTER_VERSION,
                "command": list(command),
            },
        )
        step = "access_recorded"
        # The access commits in the database before its ledger row: no transaction spans both. A failure between
        # them ends the run ``failed`` at this step, and the access stays attributable by ``result_version`` (the
        # run id) and its purpose, which is how step 2 classifies such a run too.
        access_id = record_access(
            HoldoutAccess(
                strategy_id=STRATEGY_ID,
                strategy_version=STRATEGY_VERSION,
                access_kind="evaluate",
                accessed_by=accessed_by,
                purpose=f"#3621 avoidance filters declared run {run_id}",
                result_version=run_id,
            )
        )
        append_ledger(
            ledger, {"run_id": run_id, "event": step, "at": datetime.now(UTC).isoformat(), "access_id": access_id}
        )
        step = "report_written"
        document, names = evaluate_run()
        write_exclusive(names_path, names)
        write_exclusive(out, json.dumps(document, indent=1, sort_keys=True, allow_nan=False).encode())
        append_ledger(
            ledger,
            {
                "run_id": run_id,
                "event": step,
                "at": datetime.now(UTC).isoformat(),
                "report": out.name,
                "report_sha256": sha256_file(out),
                "names": names_path.name,
                "names_sha256": sha256_file(names_path),
            },
        )
        step = "completed"
        row = {
            "run_id": run_id,
            "event": step,
            "at": datetime.now(UTC).isoformat(),
            "trial_id": TRIAL_ID,
            "verdicts": document["verdict_lines"],
        }
        append_ledger(ledger, row)
        return row
    except BaseException as exc:
        # BaseException on purpose: an interrupt after ``started`` must still end the run, or every later run refuses
        # it as open (step 2's ``_end_if_open`` does the same). A failed append may still have made its row durable,
        # so the run's state is re-read, not inferred from ``step``: end it only if it has a row and no terminal one.
        # Outputs are removed only when the re-read shows no durable row names them; an unknown state keeps them.
        # No cleanup error masks ``exc``.
        known = True
        try:
            events = {r.get("event") for r in read_ledger(ledger) if r.get("run_id") == run_id}
        except Exception as read_error:  # as step 2's ``_end_if_open``: end the run regardless
            exc.add_note(f"the ledger was not re-read ({read_error!r}); a 'failed' row is written regardless")
            events, known = {"started"}, False
        if events and not events & _TERMINAL:
            end_run_failed(ledger, run_id, step, exc)
        if known and "report_written" not in events:
            for path in (out, names_path):
                try:
                    path.unlink(missing_ok=True)
                except OSError as unlink_error:
                    exc.add_note(f"{path} was not removed ({unlink_error!r}); no ledger row names it")
        raise


def evaluate_artefacts() -> tuple[dict[str, Any], bytes]:
    stage_a_verified = read_verified_artefact(
        STAGE_A_ARTEFACT, STAGE_A_MANIFEST_SHA256, keep=(*STAGE_KEEP, JKP_RETURNS)
    )
    capture = verify_capture(capture_path(STAGE_B_CAPTURE_RUN), STAGE_B_CAPTURE_SHA256, STAGE_KEEP)
    if capture.manifest["stage_b"]["manifest_sha256"] != STAGE_B_MANIFEST_SHA256:
        raise RunError("step 2's capture does not bind the declared stage-B manifest")
    stage_a = load_stage(stage_a_verified)
    checked = reproduce(stage_a_counts(stage_a_verified), stage_a)
    stage_b = load_stage(capture.stage_b)
    sign = parse_table9_signs(stage_a_verified.files[TABLE9_SIGNS])[MAX_CHARACTERISTIC]
    fid = fidelity(stage_a, read_published_max(stage_a_verified.files[JKP_RETURNS]), sign)
    result = evaluate([stage_a, stage_b], fid)
    names, names_sha256 = names_file(result.pop("excluded"))
    document = {
        "trial_id": TRIAL_ID,
        "verdict_lines": verdict_lines(result["pairs"], fid),
        "labels": list(LABELS),
        "reproduction": {"stage_a_formations_matched": checked, "against": "scripts.measure_3621_filter_premise"},
        "max_fidelity": fid,
        "names_sha256": names_sha256,
        **result,
    }
    return jsonable(document), names


def _record_access(access: HoldoutAccess) -> int:
    with psycopg.connect(settings.database_url) as conn:
        access_id = record_holdout_access(conn, access)
        conn.commit()
    return access_id


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--print-declaration", action="store_true", help="print the register row's evidence; no read")
    mode.add_argument("--declare", action="store_true", help="append the committed 'declared' row; no read")
    mode.add_argument("--accessed-by", help="the operator or loop identity running the declared run")
    args = parser.parse_args(argv)
    if args.print_declaration or args.declare:
        trial = declared_trial(declaration_evidence(current_labels()))
        if args.declare:
            require_undeclared(TRIAL_REGISTER, read_ledger(COMMITTED_LEDGER_PATH))
            append_ledger(COMMITTED_LEDGER_PATH, declared_row(trial))
        print(json.dumps({"evidence": trial.evidence, "payload_sha256": payload_sha256(trial)}))
        return 0
    run_id = uuid.uuid4().hex
    row = run(
        run_id,
        head=report_head(),
        command=[sys.executable, "-m", "scripts.run_3621_avoidance", *(sys.argv[1:] if argv is None else argv)],
        accessed_by=args.accessed_by,
        record_access=_record_access,
        evaluate_run=evaluate_artefacts,
    )
    print(json.dumps(row, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
