"""#3621 slice 5b-2b: v2's declaration, hold-out access, stage-B rebuild and run (seven sets, the SI filter).

Spec: ``docs/research/2026-10-09-3621-slice5-short-interest.md`` (the addendum, PR #3734) over the base spec
``docs/research/2026-10-08-3621-avoidance-filters.md``. v1 (``scripts.run_3621_avoidance``, register r28) stays frozen;
this module reuses its readers, reproduction check, books and verdicts, and adds what the addendum amends. Modes:

* ``--print-declaration`` prints the register row's ``evidence`` for this checkout. It reads no artefact.
* ``--declare`` refuses if the trial is in the register or the committed ledger, then appends the committed
  ``declared`` row pinning that register row's payload sha256. The declaration PR adds the row verbatim.
* ``--accessed-by <identity>`` is the run. It reads nothing before the gate and the access row:

  1. a clean checkout at ``origin/main`` after a fetch (``report_head``);
  2. the gate (:func:`check_declaration`): one register row, its committed ``declared`` pin, every ``evidence``
     value equal to this checkout's, and no completed or open v2 run. A refusal writes no row;
  3. ``started`` (carrying ``trial_id``, so v2's rows sit beside v1's in the committed ledger), then the ``evaluate``
     hold-out access, committed, then ``access_recorded``;
  4. stage B rebuilt under Amendment 3 by step 2's builder (``publish_stage_b``) under v2's gate, which binds step 2's
     extended SUB artefact (``STEP2_SUB``): stage B freezes the SUB manifest into ``inputs/``, and that manifest names
     its publishing run, so only step 2's own SUB can leave every v1 input byte-equal. The builder writes
     ``stage_b_published`` with the manifest digest;
  5. the identity refusal against v1's stage B (``report_3621_si.stage_b_identity``), then stage A (Amendment 3) and
     premise 1's reproduction, then the SI readings and premise 2's reproduction (``load_3621_si.reproduce_si``);
  6. MAX fidelity, the 42 pairs and the pooled diagnostic with condition 5 on SI sets, the SI diagnostics, the report
     and the names file, ``report_written`` and ``completed``. Any failure after ``started`` ends the run ``failed``;
     a retry is a new run with its own access.

The construction hash is this module's import closure (v1's runner, step 2's builder and loaders, slices 5a, 5b-1 and
5b-2a). The register row's evidence pins the addendum, the base spec, both stage manifests, step 2's SUB, the step 0
manifest, the calendar, the payload manifest, premise 2's counts, the JKP and OSAP commits.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import uuid
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services.avoidance_filters import FILTER_SETS_V2, JKP_CODE_COMMIT, Filter, NameFlags
from app.services.factor_book_declaration import construction_sha256, payload_sha256, register_policy_sha256
from app.services.factor_book_ledger import end_run_failed, require_committed_access
from app.services.factor_book_series import ARMS
from app.services.factor_panel_artefact import sha256_file
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.factor_panel_reference import parse_table9_signs
from app.services.raw_filings import stored_body
from app.services.result_ledger import HoldoutAccess, record_holdout_access
from app.services.short_interest_flag import (
    DOCUMENT_KIND,
    SI_DECILE,
    FinraFile,
    Reading,
    RevisionTally,
    SiState,
    read_calendar_csv,
    revision_check,
    settlement_for,
)
from app.services.trial_register import (
    TRIAL_REGISTER,
    TRIAL_REGISTER_VERSION,
    DeclaredTrial,
    TrialExactness,
    TrialRegister,
)
from scripts.build_3609_factor_panel import (
    PUBLISH_ROOT,
    RESEARCH_ROOT,
    STAGE_B_EVENT,
    Frozen,
    StageBGate,
    VerifiedArtefact,
    publish_stage_b,
    read_verified_artefact,
    stage_b_pins,
)
from scripts.capture_3609_step2 import capture_path, ledger_claim, verify_capture, write_exclusive
from scripts.load_3621_si import (
    CALENDAR_PATH,
    COUNTS_PATH,
    PAYLOADS_PATH,
    PREMISE_NAMES_SHA256,
    FormationSi,
    SiFormation,
    read_payload_manifest,
    read_payloads,
    read_si,
    reproduce_si,
    si_names,
)
from scripts.measure_3621_filter_premise import stage_a_counts
from scripts.report_3609_step2 import TABLE9_SIGNS, PanelMonth
from scripts.report_3609_step2_assembly import jsonable
from scripts.report_3609_step2_inputs import STEP0_MANIFEST, STEP0_RUN
from scripts.report_3609_step2_run import report_head
from scripts.report_3609_step2_verdict import BASE
from scripts.report_3621_books import (
    SCENARIOS,
    VERDICT_POPULATIONS,
    Population,
    book_paths,
    populations,
    targets,
)
from scripts.report_3621_diagnostics import (
    SIZE_SEGMENTS,
    BookStats,
    ExcludedName,
    Undefined,
    book_stats,
    cell_counts,
    names_file,
    screened_targets,
    windows,
)
from scripts.report_3621_fidelity import MAX_CHARACTERISTIC, MaxFidelity
from scripts.report_3621_si import (
    counts_rows,
    coverage_shortfalls,
    si_counts,
    si_overlap,
    si_pair_result,
    stage_b_identity,
    valueless_weight,
    with_si,
)
from scripts.run_3621_avoidance import (
    DAILY,
    JKP_RETURNS,
    RUN_CLAIM,
    SESSIONS,
    STAGE_B_CAPTURE_RUN,
    STAGE_B_CAPTURE_SHA256,
    STAGE_B_MANIFEST_SHA256,
    STAGE_KEEP,
    RunError,
    Stage,
    declared_row,
    fidelity,
    load_stage,
    pair_block,
    read_published_max,
    reproduce,
    set_label,
    verdict_lines,
)
from scripts.run_3621_avoidance import LABELS as V1_LABELS
from scripts.run_3621_avoidance import _lines as gz_lines  # pyright: ignore[reportPrivateUsage]
from scripts.run_3621_avoidance import _window_block as window_block  # pyright: ignore[reportPrivateUsage]

TRIAL_ID: Final = "3621-avoidance-filters-v2"
STRATEGY_ID: Final = "3621-avoidance-filters"
STRATEGY_VERSION: Final = "v2"
#: §"Registration": 7 filter sets x 6 populations x 2 arms, plus the MAX fidelity check's 2 arms.
SEARCHES: Final = len(FILTER_SETS_V2) * len(VERDICT_POPULATIONS) * len(ARMS) + len(ARMS)
DECLARED_EVENT: Final = "declared"
STARTED_EVENT: Final = "started"
ACCESS_EVENT: Final = "access_recorded"
IDENTITY_EVENT: Final = "stage_b_identical"

_REPO_ROOT: Final = Path(__file__).resolve().parents[1]
SPEC_PATH: Final = _REPO_ROOT / "docs" / "research" / "2026-10-09-3621-slice5-short-interest.md"
BASE_SPEC_PATH: Final = _REPO_ROOT / "docs" / "research" / "2026-10-08-3621-avoidance-filters.md"
CONSTRUCTION_ROOTS: Final = (Path(__file__).resolve(),)
LEDGER_PATH: Final = _REPO_ROOT / "var" / "research" / "3621" / "ledger-v2.jsonl"
COMMITTED_LEDGER_PATH: Final = _REPO_ROOT / "docs" / "research" / "3621-ledger.jsonl"
#: Amendment 3's stage A (#3730 slice 2, ``af7889cc``).
STAGE_A_ARTEFACT: Final = RESEARCH_ROOT / "factor_panel_3609" / "2026-10-09-af7889cc-stageA"
STAGE_A_MANIFEST_SHA256: Final = "e0924564ef1639f9473586523679894eb5d1741a75a3a3b8ddce5858917cae6e"
#: Step 2's extended SUB artefact, the one v1's stage B froze (step 2's capture ``2039b95f…``, ``sub``).
STEP2_SUB: Final = (
    RESEARCH_ROOT / "factor_book_3609_step2_sub" / STAGE_B_CAPTURE_RUN,
    "d0f3f9a90f4c0fb49a51266ad55f9f7b3a8d7857338cb2661ad7e0545452f8dd",
)
#: OSAP's ``ShortInterest`` timing reference the addendum cites (§"Source rules", timing).
OSAP_CODE_COMMIT: Final = "8db89244"
SPLITS: Final = f"inputs/{Frozen.SPLITS}"
STAGE_B_KEEP: Final = (*STAGE_KEEP, SPLITS)
#: The first formation whose SI set can differ from U: formation 2021-06's trades fall in month 2021-06.
SI_FIRST_MONTH: Final = (2021, 6)
_CODE_LABELS: Final = ("spec_sha256", "construction_sha256", "register_policy_sha256", "python")

SPEC_REFERENCE: Final = (
    'docs/research/2026-10-09-3621-slice5-short-interest.md §"Registration" over '
    "docs/research/2026-10-08-3621-avoidance-filters.md"
)
DESCRIPTION: Final = (
    "#3621 avoidance filters, version 2: the base spec's preregistered, non-claiming enumeration re-run on the "
    "Amendment-3 (clipped holding return) panel, formations 2014-09..2024-07 over stages A and B, net of step 0's "
    "costs, with stage B rebuilt inside the run and refused unless identical to v1's outside prices.holding. Seven "
    "filter sets (v1's five, SI = top decile of FINRA short interest ratio from formation 2021-06, and MAX + sub-$5 + "
    "young + SI) x six populations x two termination arms = 84, plus the MAX fidelity check's two arms = 86. "
    "Condition 5 (SI sets): names with an SI value are at least 90% of U at every covered formation. A pair's verdict "
    "only decides whether a later long-book spec may cite the set; no premium or non-inferiority is claimed, so no "
    "TrialDesign. Retrospective: v1's sets were run on data whose outcomes are known."
)
LABELS: Final = (
    *V1_LABELS,
    "v1's five sets are retrospective twice over: their v1 outcomes were read before this declaration",
    "SI rests on 38 formations (2021-06..2024-07) in one regime; a descriptive screen, never significance",
    "SI identity is an unverified mapping exception: exact symbol match corroborated by FINRA's ADV within 1.2x",
    "SI inputs are a retrospectively retrieved FINRA archive, assumed first-published (premise 4 supports it)",
    "the 1.2 ADV tolerance and condition 5's 90% floor are full-period exploratory calibrations, not held out",
    "no factor-level fidelity exists for SI: JKP has no short-interest characteristic and OSAP's load has not run",
)


# --------------------------------------------------------------------------- the declaration


def current_labels() -> dict[str, str]:
    """Every ``label=value`` the register row's ``evidence`` names, for this checkout."""
    return {
        "spec_sha256": sha256_file(SPEC_PATH),
        "construction_sha256": construction_sha256(CONSTRUCTION_ROOTS, _REPO_ROOT),
        "register_policy_sha256": register_policy_sha256((_REPO_ROOT / "app/services/trial_register.py").read_bytes()),
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
        "base_spec_sha256": sha256_file(BASE_SPEC_PATH),
        "stage_a_manifest_sha256": STAGE_A_MANIFEST_SHA256,
        "v1_stage_b_manifest_sha256": STAGE_B_MANIFEST_SHA256,
        "step2_sub_manifest_sha256": STEP2_SUB[1],
        "step0_manifest_sha256": sha256_file(STEP0_RUN / STEP0_MANIFEST),
        "si_calendar_sha256": sha256_file(CALENDAR_PATH),
        "si_payloads_sha256": sha256_file(PAYLOADS_PATH),
        "si_counts_sha256": sha256_file(COUNTS_PATH),
        "si_names_sha256": PREMISE_NAMES_SHA256,
        "jkp_code_commit": JKP_CODE_COMMIT,
        "osap_code_commit": OSAP_CODE_COMMIT,
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


def own_rows(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """v2's rows: its ``declared`` row and every row of a run whose ``started`` row names v2. v1's runs share the
    committed ledger and are never v2's."""
    runs = {r.get("run_id") for r in rows if r.get("event") == STARTED_EVENT and r.get("trial_id") == TRIAL_ID}
    return [r for r in rows if r.get("trial_id") == TRIAL_ID or (r.get("run_id") is not None and r["run_id"] in runs)]


def check_declaration(
    register: TrialRegister, rows: Sequence[Mapping[str, Any]], labels: Mapping[str, str]
) -> DeclaredTrial:
    """The declared trial, once its row, its committed payload pin and this checkout agree and no v2 run completed.
    ``rows`` are the committed ledger plus the local one; only the committed ledger ever holds ``declared``."""
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
    mine = own_rows(rows)
    declared = [r for r in mine if r.get("event") == DECLARED_EVENT]
    if len(declared) != 1 or declared[0].get("payload_sha256") != payload_sha256(trial):
        raise RunError(f"{TRIAL_ID}: the committed ledger does not hold one 'declared' row pinning this register row")
    moved = sorted(label for label, value in labels.items() if evidence_value(trial.evidence, label) != value)
    if moved:
        raise RunError(f"this checkout's {moved} differ from {TRIAL_ID}'s declared values")
    if any(r.get("event") == "completed" for r in mine):
        raise RunError(f"{TRIAL_ID} already has a completed run; a changed study needs a new trial")
    return trial


# --------------------------------------------------------------------------- the SI flags and diagnostics


@dataclass(frozen=True)
class SiStage:
    """Stage B's SI: per formation (in ``Stage.months`` order) its reading and v1's flags with SI added."""

    si: list[FormationSi]
    flags: list[dict[int, NameFlags]]
    files: dict[date, FinraFile]


def no_si(calendar: Sequence[tuple[date, date]], months: Sequence[PanelMonth]) -> None:
    """Stage A precedes coverage: every formation must resolve to no settlement, so SI flags nothing there."""
    covered = [m.formation for m in months if settlement_for(calendar, m.session) is not None]
    if covered:
        raise RunError(f"stage-A formations {covered[:3]} resolve to an SI settlement")


def unreported_zero(readings: Mapping[int, Reading], q: float) -> tuple[float, int]:
    """Asquith, Pathak & Ritter's rule as a diagnostic: ``unmatched`` names enter N at zero; (q, flags moved)."""
    values = [r.sir for r in readings.values() if r.sir is not None]
    padded = sorted([0.0] * sum(r.state is SiState.UNMATCHED for r in readings.values()) + values)
    q_zero = padded[math.ceil(SI_DECILE * len(padded)) - 1]
    return q_zero, sum((v >= q) != (v >= q_zero) for v in values)


def _norm(name: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", name.upper())


def audit_list(months: Sequence[PanelMonth], si: Sequence[FormationSi]) -> list[dict[str, Any]]:
    """§"Source rules", identity: every series whose matched FINRA ``issueName`` changes over the window, with the
    first settlement at which each normalised name appears, its state and its ratio, untruncated."""
    seen: dict[int, list[tuple[date, str, str, float | None]]] = defaultdict(list)
    for month, formation in zip(months, si, strict=True):
        for name, r in formation.readings.items():
            if r.issue_name is not None and r.used is not None:
                seen[month.admitted[name].series_id].append((r.used, r.issue_name, str(r.state), r.log_ratio))
    out = []
    for series_id, matches in sorted(seen.items()):
        first: dict[str, tuple[date, str, float | None]] = {}
        for day, name, state, ratio in sorted(matches):
            first.setdefault(_norm(name), (day, state, ratio))
        if len(first) > 1:
            accepted = {_norm(n) for _d, n, s, _r in matches if s == str(SiState.VALID)}
            out.append(
                {
                    "series_id": series_id,
                    "accepted_change": len(accepted) > 1,
                    "names": [
                        {"name": n, "first": d, "state": s, "ratio": None if r is None else math.exp(r)}
                        for n, (d, s, r) in first.items()
                    ],
                }
            )
    return out


def revision_table(tally: RevisionTally) -> dict[str, Any]:
    return {
        "compared": [{"flagged": f, "agrees_with_prior": a, "rows": n} for (f, a), n in sorted(tally.compared.items())],
        "uncompared": {str(f): n for f, n in sorted(tally.uncompared.items())},
        "blank_previous": {str(f): n for f, n in sorted(tally.blank_previous.items())},
        "residue": [list(r) for r in tally.residue],
    }


def si_diagnostics(
    months: Sequence[PanelMonth], pops: Sequence[Mapping[Population, frozenset[int]]], stage: SiStage
) -> dict[str, Any]:
    """§"The study", SI diagnostics, per covered formation: the counts table, q, values above 1, the unreported-is-
    zero q and the flags it moves, and the overlap with each v1 filter; then the revision check and the audit list."""
    per: list[dict[str, Any]] = []
    for month, p, formation, flags in zip(months, pops, stage.si, stage.flags, strict=True):
        if formation.used is None or formation.q is None:
            continue
        counts = si_counts(p, formation.readings, formation.flagged)
        q_zero, moved = unreported_zero(formation.readings, formation.q)
        values = [r.sir for r in formation.readings.values() if r.sir is not None]
        per.append(
            {
                "M": month.formation,
                "settlement": formation.used,
                "q": formation.q,
                "valid": len(values),
                "above_one": sum(v > 1 for v in values),
                "q_unreported_zero": q_zero,
                "flags_moved_unreported_zero": moved,
                "counts": {str(k): v for k, v in counts.items()},
                "overlap": si_overlap(flags),
            }
        )
    ordered = sorted(stage.files.items())
    return {
        "formations": per,
        "revision_check": revision_table(revision_check(ordered)),
        "files": [
            {
                "settlement": d,
                "physical_rows": f.physical_rows,
                "physical_revised": f.physical_revised,
                "zero_short": f.zero_short,
                "duplicated_symbols": len(f.twice),
            }
            for d, f in ordered
        ],
        "audit": audit_list(months, stage.si),
    }


# --------------------------------------------------------------------------- the evaluation


def _increment(four: Mapping[Any, Any], three: Mapping[Any, Any], u_returns: Sequence[Any]) -> dict[str, Any]:
    """G("all four") - G("all three") on every window, arm and cost, an exhausted book's figure undefined."""
    out: dict[str, Any] = {}
    for w in windows(list(u_returns)):
        cells: dict[str, Any] = {}
        for arm in ARMS:
            for cost in SCENARIOS:
                s4, s3 = book_stats(four[(arm, cost)], w), book_stats(three[(arm, cost)], w)
                if isinstance(s4, BookStats) and isinstance(s3, BookStats):
                    cells[f"{arm}|{cost}"] = s4.g - s3.g
                else:
                    hits = [s.exhausted_at for s in (s4, s3) if isinstance(s, Undefined)]
                    cells[f"{arm}|{cost}"] = str(Undefined(min(hits)))
        out[w.label] = cells
    return out


def si_set_identity(
    months: Sequence[PanelMonth], members: Sequence[frozenset[int]], flags: Sequence[Mapping[int, NameFlags]]
) -> None:
    """Condition 1 on the SI set: U_F equals U at every formation whose trades fall before ``SI_FIRST_MONTH``, so
    the whole-path dG is the 2021-06..2024-08 dG times 39/119 (addendum §"The study"). Refuses otherwise."""
    si_only = frozenset({Filter.SI})
    differ = [
        m.formation
        for m, p, f in zip(months, members, flags, strict=True)
        if (m.formation.year, m.formation.month) < SI_FIRST_MONTH and targets(p, f, si_only).flagged
    ]
    if differ:
        raise RunError(f"the SI set excludes names before {SI_FIRST_MONTH}: {differ[:3]}")


if FILTER_SETS_V2[-1] - {Filter.SI} not in FILTER_SETS_V2 or Filter.SI not in FILTER_SETS_V2[-1]:
    raise RunError("the SI increment needs 'all four' last and 'all three' among the sets")


def evaluate(
    stages: Sequence[Stage], si_by_stage: Sequence[Sequence[FormationSi | None]], fid: MaxFidelity
) -> dict[str, Any]:
    """Every (F, P) of the seven sets, including the pooled population: verdicts (condition 5 on SI sets), the
    diagnostics, the SI increment, the cell counts and the excluded names."""
    months = [m for s in stages for m in s.months]
    flags = [f for s in stages for f in s.flags]
    #: Each formation's SI readings where it is covered, ``None`` where it is not (all of stage A, and 2021-05).
    covered: list[Mapping[int, Reading] | None] = [
        None if x is None or x.used is None else x.readings for s in si_by_stage for x in s
    ]
    if not (len(months) == len(flags) == len(covered)):
        raise RunError("months, flags and SI readings differ in length")
    pops = [
        populations({n: p.me for n, p in m.admitted.items()}, s.cutoffs[m.formation]) for s in stages for m in s.months
    ]
    formations = [m.formation for m in months]
    symbols = {k: v for s in stages for k, v in s.symbols.items()}
    pairs: dict[str, Any] = {}
    screened: dict[str, Any] = {}
    increment: dict[str, Any] = {}
    for population in Population:
        members = [p[population] for p in pops]
        u = book_paths(months, members)
        u_months = list(u[(ARMS[0], BASE)].returns)
        only = book_paths(months, [screened_targets(p, f).filtered for p, f in zip(members, flags, strict=True)])
        screened[str(population)] = {w.label: window_block(u, only, w) for w in windows(u_months)}
        si_set_identity(months, members, flags)
        shortfalls = coverage_shortfalls(members, covered, formations)
        for filter_set in FILTER_SETS_V2:
            result, block = pair_block(months, members, flags, filter_set, u, fid.passed)
            if Filter.SI in filter_set:
                result = si_pair_result(result, shortfalls)
                block["condition_5_shortfalls"] = shortfalls
                block["valueless_weight"] = {
                    m.formation: valueless_weight(targets(p, f, filter_set).filtered, r)
                    for m, p, f, r in zip(months, members, flags, covered, strict=True)
                    if r is not None
                }
            verdict = population in VERDICT_POPULATIONS
            pairs[f"{set_label(filter_set)}|{population}"] = {
                "verdict": result if verdict else None,
                "pooled_delta_g": None if verdict else result.delta_g,
                **block,
            }
        four = FILTER_SETS_V2[-1]
        three = four - {Filter.SI}
        increment[str(population)] = _increment(
            book_paths(months, [targets(p, f, four).filtered for p, f in zip(members, flags, strict=True)]),
            book_paths(months, [targets(p, f, three).filtered for p, f in zip(members, flags, strict=True)]),
            u_months,
        )
    cells = {
        set_label(fs): {
            m.formation: cell_counts(m, {s: p[s] for s in SIZE_SEGMENTS}, f, fs)
            for m, p, f in zip(months, pops, flags, strict=True)
        }
        for fs in FILTER_SETS_V2
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
    return {
        "pairs": pairs,
        "screened_only": screened,
        "si_increment": increment,
        "cells": cells,
        "excluded": excluded,
    }


# --------------------------------------------------------------------------- reading the artefacts


def load_si(
    stage_b: VerifiedArtefact,
    months: Sequence[PanelMonth],
    flags: Sequence[Mapping[int, NameFlags]],
    calendar: Sequence[tuple[date, date]],
    files: Mapping[date, FinraFile],
) -> SiStage:
    sessions = [date.fromisoformat(d) for d in gz_lines(stage_b.files[SESSIONS])]
    names = si_names(stage_b.rows, stage_b.files[DAILY], stage_b.files[SPLITS])
    si = [read_si(calendar, files, sessions, m.session, names.get(m.formation, {})) for m in months]
    return SiStage(si, [with_si(f, x.flagged) for f, x in zip(flags, si, strict=True)], dict(files))


def si_formations(stage: Stage, si: SiStage) -> list[SiFormation]:
    return [
        SiFormation(
            m.formation,
            populations({n: p.me for n, p in m.admitted.items()}, stage.cutoffs[m.formation]),
            {n: p.me for n, p in m.admitted.items()},
            x,
        )
        for m, x in zip(stage.months, si.si, strict=True)
    ]


def _fetch(accession: str) -> str | None:
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        return stored_body(conn, accession_number=accession, document_kind=DOCUMENT_KIND)


def evaluate_artefacts(stage_b_artefact: tuple[Path, str]) -> tuple[dict[str, Any], bytes]:
    """Everything after the rebuild: the identity refusal, both reproductions, then the evaluation."""
    capture = verify_capture(capture_path(STAGE_B_CAPTURE_RUN), STAGE_B_CAPTURE_SHA256)
    if capture.manifest["stage_b"]["manifest_sha256"] != STAGE_B_MANIFEST_SHA256:
        raise RunError("step 2's capture does not bind v1's stage-B manifest")
    v1 = capture.stage_b
    stage_b_verified = read_verified_artefact(*stage_b_artefact, pins=stage_b_pins(STEP2_SUB[1]), keep=STAGE_B_KEEP)
    compared = stage_b_identity(
        gz_lines(v1.rows), gz_lines(stage_b_verified.rows), v1.manifest["inputs"], stage_b_verified.manifest["inputs"]
    )
    stage_a_verified = read_verified_artefact(
        STAGE_A_ARTEFACT, STAGE_A_MANIFEST_SHA256, keep=(*STAGE_KEEP, JKP_RETURNS)
    )
    stage_a = load_stage(stage_a_verified)
    checked = reproduce(stage_a_counts(stage_a_verified), stage_a)
    calendar = read_calendar_csv(CALENDAR_PATH.read_text())
    no_si(calendar, stage_a.months)
    files = read_payloads(read_payload_manifest(PAYLOADS_PATH.read_text(), calendar), _fetch)
    stage_b = load_stage(stage_b_verified)
    si = load_si(stage_b_verified, stage_b.months, stage_b.flags, calendar, files)
    covered, names_sha = reproduce_si(si_formations(stage_b, si), COUNTS_PATH.read_text())
    stage_b = Stage(stage_b.months, si.flags, stage_b.readings, stage_b.cutoffs, stage_b.symbols)
    sign = parse_table9_signs(stage_a_verified.files[TABLE9_SIGNS])[MAX_CHARACTERISTIC]
    fid = fidelity(stage_a, read_published_max(stage_a_verified.files[JKP_RETURNS]), sign)
    result = evaluate([stage_a, stage_b], [[None] * len(stage_a.months), si.si], fid)
    pops_b = [
        populations({n: p.me for n, p in m.admitted.items()}, stage_b.cutoffs[m.formation]) for m in stage_b.months
    ]
    names, names_sha256 = names_file(result.pop("excluded"))
    document = {
        "trial_id": TRIAL_ID,
        "verdict_lines": verdict_lines(result["pairs"], fid),
        "labels": list(LABELS),
        "stage_b": {
            "manifest_sha256": stage_b_artefact[1],
            "identity_rows_compared": compared,
            "against": STAGE_B_MANIFEST_SHA256,
        },
        "reproduction": {
            "stage_a_formations_matched": checked,
            "si_formations_matched": covered,
            "si_names_sha256": names_sha,
            "against": ["scripts.measure_3621_filter_premise", "scripts.measure_3621_short_interest_premise"],
        },
        "max_fidelity": fid,
        "si": si_diagnostics(stage_b.months, pops_b, si),
        "si_counts_rows": [
            row
            for m, p, x in zip(stage_b.months, pops_b, si.si, strict=True)
            if x.used is not None
            for row in counts_rows(m.formation, si_counts(p, x.readings, x.flagged))
        ],
        "names_sha256": names_sha256,
        **result,
    }
    return jsonable(document), names


# --------------------------------------------------------------------------- the run


_TERMINAL: Final = frozenset({"completed", "failed"})


def access_purpose(run_id: str) -> str:
    return f"#3621 avoidance filters v2 declared run {run_id}"


def open_runs(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    """v2 run ids with a ``started`` row and no terminal row."""
    mine = own_rows(rows)
    started = [r["run_id"] for r in mine if r.get("event") == STARTED_EVENT]
    ended = {r.get("run_id") for r in mine if r.get("event") in _TERMINAL}
    return [run_id for run_id in started if run_id not in ended]


def stage_b_gate(register: TrialRegister, labels: Callable[[], dict[str, str]]) -> StageBGate:
    """v2's gate on step 2's builder: v2's declaration, v2's code hashes in the manifest, step 2's SUB bound."""

    def versions() -> dict[str, Any]:
        current = labels()
        return {label: current[label] for label in _CODE_LABELS}

    return StageBGate(lambda rows: check_declaration(register, rows, labels()), versions, sub=STEP2_SUB)


def run(
    run_id: str,
    *,
    head: str,
    command: Sequence[str],
    accessed_by: str,
    record_access: Callable[[HoldoutAccess], int],
    build_stage_b: Callable[[str], tuple[Path, str]],
    evaluate_run: Callable[[tuple[Path, str]], tuple[dict[str, Any], bytes]],
    register: TrialRegister = TRIAL_REGISTER,
    ledger: Path = LEDGER_PATH,
    committed_ledger: Path = COMMITTED_LEDGER_PATH,
    labels: Callable[[], dict[str, str]] = current_labels,
) -> dict[str, Any]:
    """The gate, then ``started``, the access, the stage-B rebuild and the evaluation; returns the ``completed`` row.

    The whole run holds the ledger's run claim (v1's pattern), so two runs never both pass the gate. A run left open
    refuses every later run until it is ended by hand with a ``failed`` row."""
    with ledger_claim(ledger, RUN_CLAIM):
        current = labels()
        committed = read_ledger(committed_ledger)
        rows = [*committed, *(row for row in read_ledger(ledger) if row not in committed)]
        trial = check_declaration(register, rows, current)
        if stale := open_runs(rows):
            raise RunError(f"runs {stale} started and never ended; end each with a 'failed' row first")
        return _run_claimed(
            run_id, trial, current, head, command, accessed_by, record_access, build_stage_b, evaluate_run, ledger
        )


def _run_claimed(
    run_id: str,
    trial: DeclaredTrial,
    current: Mapping[str, str],
    head: str,
    command: Sequence[str],
    accessed_by: str,
    record_access: Callable[[HoldoutAccess], int],
    build_stage_b: Callable[[str], tuple[Path, str]],
    evaluate_run: Callable[[tuple[Path, str]], tuple[dict[str, Any], bytes]],
    ledger: Path,
) -> dict[str, Any]:
    out = ledger.parent / f"{run_id}.json"
    names_path = ledger.parent / f"{run_id}-names.jsonl.gz"
    step = STARTED_EVENT
    try:
        append_ledger(
            ledger,
            {
                "run_id": run_id,
                "event": step,
                "at": datetime.now(UTC).isoformat(),
                "trial_id": TRIAL_ID,
                "git_sha": head,
                **current,
                "payload_sha256": payload_sha256(trial),
                "register_version": TRIAL_REGISTER_VERSION,
                "command": list(command),
            },
        )
        step = ACCESS_EVENT
        # As v1: the access commits before its ledger row; a failure between them ends the run ``failed`` here and
        # the access stays attributable by ``result_version`` (the run id) and its purpose.
        access_id = record_access(
            HoldoutAccess(
                strategy_id=STRATEGY_ID,
                strategy_version=STRATEGY_VERSION,
                access_kind="evaluate",
                accessed_by=accessed_by,
                purpose=access_purpose(run_id),
                result_version=run_id,
            )
        )
        append_ledger(
            ledger, {"run_id": run_id, "event": step, "at": datetime.now(UTC).isoformat(), "access_id": access_id}
        )
        # The builder runs its own gate and writes ``stage_b_published`` (or ``failed``) itself.
        step = STAGE_B_EVENT
        artefact = build_stage_b(run_id)
        step = "report_written"
        document, names = evaluate_run(artefact)
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
            "stage_b_manifest_sha256": artefact[1],
            "verdicts": document["verdict_lines"],
        }
        append_ledger(ledger, row)
        return row
    except BaseException as exc:
        # v1's cleanup: the run's state is re-read, never inferred from ``step``; it is ended only if it has a row
        # and no terminal one (the builder may already have ended it), and outputs are removed only when no durable
        # row names them. No cleanup error masks ``exc``.
        known = True
        try:
            events = {r.get("event") for r in read_ledger(ledger) if r.get("run_id") == run_id}
        except Exception as read_error:
            exc.add_note(f"the ledger was not re-read ({read_error!r}); a 'failed' row is written regardless")
            events, known = {STARTED_EVENT}, False
        if events and not events & _TERMINAL:
            end_run_failed(ledger, run_id, step, exc)
        if known and "report_written" not in events:
            for path in (out, names_path):
                try:
                    path.unlink(missing_ok=True)
                except OSError as unlink_error:
                    exc.add_note(f"{path} was not removed ({unlink_error!r}); no ledger row names it")
        raise


def _record_access(access: HoldoutAccess) -> int:
    with psycopg.connect(settings.database_url) as conn:
        access_id = record_holdout_access(conn, access)
        conn.commit()
    return access_id


def _confirm(run_id: str, access_id: int) -> None:
    # A fresh connection: it sees the access row only if the run committed it.
    with psycopg.connect(settings.database_url) as conn:
        require_committed_access(
            conn, run_id, access_id, strategy=(STRATEGY_ID, STRATEGY_VERSION), purpose=access_purpose(run_id)
        )


def _build_stage_b(run_id: str) -> tuple[Path, str]:
    return publish_stage_b(
        PUBLISH_ROOT,
        run_id,
        confirm_access=_confirm,
        ledger=LEDGER_PATH,
        committed_ledger=COMMITTED_LEDGER_PATH,
        gate=stage_b_gate(TRIAL_REGISTER, current_labels),
    )


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
        command=[sys.executable, "-m", "scripts.run_3621_avoidance_v2", *(sys.argv[1:] if argv is None else argv)],
        accessed_by=args.accessed_by,
        record_access=_record_access,
        build_stage_b=_build_stage_b,
        evaluate_run=evaluate_artefacts,
    )
    print(json.dumps(row, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
