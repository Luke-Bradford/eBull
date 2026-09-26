"""The hunt's audited door: building and freezing a validation declaration.

#3385 slice 2b-i, spec ``docs/proposals/ta/2026-09-26-3385-hunt-harness.md`` (v5), "The
audited door", "DSR" (M, V[SR]), "BY", "Power" and "Trial register (#2829)"; obligations
91 (in ``hunt_harness``), 108, 110 and 122-123. The holdout declaration, the readouts and
the abandonment script are slice 2b-ii.

A hunt's validation batch is ONE #2599 declaration, ``hunt-<n>-validation`` @ ``v1``. Its
document pins every candidate's validation spec and every freeze-time number: M (with the
``M_inh`` of the register at freeze), V[SR] with the trial ids it came from, the BY
readout that flagged each candidate, the power statement per candidate, and the
forward-shadow floor. The flow:

1. ``build_validation_declaration`` computes the document from the log (a readout, not a
   search) and ``write_declaration`` writes its canonical bytes;
2. the declaration PR commits the document and adds two register entries:
   ``hunt-<n>-discovery`` (``log_backed_evidence`` from ``discovery_register_evidence``,
   closed on the DB clock) and ``hunt-<n>-validation`` (``declaration_backed_evidence``
   naming the document's path, sha256 and pin count, ``declared_for`` the declaration);
3. from ``main``, ``freeze_validation_declaration`` re-reads the document, runs the
   tripwire, recomputes EVERY number in one REPEATABLE READ snapshot under the programme
   lock, refuses on any difference, and only then freezes the #2599 row and stores the
   document (``sql/428``) in the same transaction.

⚠ NOTHING FROZEN IS RECOMPUTED LATER. BY flags are recomputed on every readout and never
cached on an outcome (obligation 123); a declaration pins the readout that flagged its
candidates, as it stood at the freeze.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final, cast

import psycopg

from app.services import hunt_harness as hh
from app.services import hunt_inference
from app.services.hunt_compute import CANONICAL_CELL
from app.services.hunt_harness import TrialSpec
from app.services.hunt_inference import StatRefused, VPopulationMember
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.result_ledger import freeze_preregistration, load_preregistration
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION, structural_promotion_refusals
from app.services.trial_register import (
    TRIAL_REGISTER,
    DeclaredTrial,
    TrialExactness,
    TrialRegister,
    log_backed_evidence,
    ordered_ids_sha256,
    parse_declaration_backed_evidence,
    parse_log_backed_evidence,
)

REPO_ROOT: Final = Path(__file__).resolve().parents[2]

#: Power at 50% and 80% (the one-sided normal quantiles 0 and z_0.80).
POWER_Z: Final = (("50", 0.0), ("80", 0.8416))
SESSIONS_PER_YEAR: Final = 252
#: Power numbers compare as decimal strings at this many significant figures (contract decision).
POWER_SIGNIFICANT_FIGURES: Final = 6
POWER_STATEMENT: Final = (
    "Minimum detectable annualised active mean for the canonical cell's t > 3 bar only, under a normal "
    "approximation with the discovery long-run variance treated as known and carried over to the target split. "
    "Not power for DSR, for the other cells, or for the joint PASS. The discovery effect is selection-biased "
    "upward, so no verdict is predicted from it."
)

#: The #2599 stamps of a hunt declaration: the survivorship-free admitted set, and the
#: ``real_stock_long_x1`` lane, whose carry and FX are zero under ``COST_MODEL_ID``'s
#: prerequisites (USD order, USD account, real settlement; spec "Lanes").
UNIVERSE_BASIS: Final = "survivorship_free"
CARRY_UNMODELLED: Final = False
FX_UNMODELLED: Final = False

#: The canonical text of a discovery entry's closing query; the tripwire requires it verbatim.
DISCOVERY_LOG_QUERY: Final = (
    "SELECT hunt_trial_id FROM hunt_trials WHERE hunt_id = '{hunt}' AND split = 'discovery' "
    "AND registered_at <= '{closed_at}' ORDER BY hunt_trial_id"
)


class HuntDeclarationRefused(RuntimeError):
    """A freeze refused, with every code that fired. Nothing was written."""

    def __init__(self, hunt_id: str, codes: Sequence[str]) -> None:
        self.hunt_id = hunt_id
        self.codes = tuple(codes)
        super().__init__(f"{hunt_id} declaration refused: {'; '.join(self.codes)}")


# ---------------------------------------------------------------------------
# Power (pure)
# ---------------------------------------------------------------------------


def sig(value: float) -> str:
    """A number as the decimal string power compares at (6 significant figures)."""
    if not math.isfinite(value):
        raise ValueError(f"power numbers must be finite, got {value}")
    return format(value, f".{POWER_SIGNIFICANT_FIGURES}g")


def power_statement(discovery_active: Sequence[float] | None, *, target_observations: int, h: int) -> dict[str, Any]:
    """Spec "Power": the minimum detectable annualised active mean, (3 + z)·√(Ŝ_NW / T_x)·252.

    Ŝ_NW is the discovery canonical series' long-run variance at the TARGET split's L.
    Missing, degenerate or too-short discovery data, a target L longer than the series, or a
    target split that cannot clear ``short_sample`` block the declaration: the result then
    carries ``blocked`` and no number (contract decision "Power").
    """
    if discovery_active is None or not discovery_active:
        return {"blocked": "no_discovery_series"}
    if not all(math.isfinite(value) for value in discovery_active):
        return {"blocked": "non_finite_discovery_series"}
    lag = hunt_inference.hunt_lag(target_observations, h)
    if target_observations < hunt_inference.SHORT_SAMPLE_LAGS * lag:
        return {"blocked": "target_short_sample", "t_x": target_observations, "lag": lag}
    if lag >= len(discovery_active):
        return {"blocked": "target_lag_exceeds_discovery_series", "t_x": target_observations, "lag": lag}
    estimate = hunt_inference.hac_estimate(discovery_active, lag)
    if isinstance(estimate, StatRefused):
        return {"blocked": f"discovery_{estimate.reason}", "t_x": target_observations, "lag": lag}
    scale = math.sqrt(estimate.long_run_variance / target_observations) * SESSIONS_PER_YEAR
    return {
        "t_x": target_observations,
        "lag": lag,
        "long_run_variance": sig(estimate.long_run_variance),
        "clears_short_sample": True,
        **{f"min_detectable_annual_mean_{power}": sig((hunt_inference.T_BAR + z) * scale) for power, z in POWER_Z},
        "statement": POWER_STATEMENT,
    }


# ---------------------------------------------------------------------------
# Reading the log
# ---------------------------------------------------------------------------

_DISCOVERY_OUTCOMES = """
    SELECT t.hunt_trial_id, t.hunt_id, t.candidate_sha256, o.status, o.statistics, o.active_series,
           o.outcome_sha256
    FROM hunt_trials t
    JOIN hunt_trial_outcomes o USING (hunt_trial_id)
    WHERE t.split = 'discovery' AND t.purpose = 'evaluate'
    ORDER BY t.hunt_trial_id
"""

_HUNT_DISCOVERY_ROWS = """
    SELECT t.hunt_trial_id, t.purpose, t.floor, t.registered_at, o.hunt_trial_id IS NOT NULL
    FROM hunt_trials t
    LEFT JOIN hunt_trial_outcomes o USING (hunt_trial_id)
    WHERE t.hunt_id = %(hunt)s AND t.split = 'discovery'
    ORDER BY t.hunt_trial_id
"""

_LOG_TOTALS = """
    SELECT count(*), coalesce(bool_or(floor), false),
           count(*) FILTER (WHERE split = 'discovery')
    FROM hunt_trials
"""

_EVALUATE_KEYS = "SELECT candidate_sha256, split FROM hunt_trials WHERE purpose = 'evaluate'"

_FROZEN_DECLARATIONS = """
    SELECT declaration_id, hunt_id, split, doc_path, doc, doc_sha256
    FROM hunt_declarations
    ORDER BY declaration_id
"""


@dataclass(frozen=True)
class _DiscoveryOutcome:
    hunt_trial_id: int
    hunt_id: str
    candidate_sha256: str
    status: str
    statistics: Mapping[str, Any]
    active_series: tuple[float, ...] | None
    outcome_sha256: str


def _discovery_outcomes(conn: psycopg.Connection[Any]) -> list[_DiscoveryOutcome]:
    return [
        _DiscoveryOutcome(
            hunt_trial_id=int(row[0]),
            hunt_id=str(row[1]),
            candidate_sha256=str(row[2]),
            status=str(row[3]),
            statistics=hh.decode_form(row[4]),
            active_series=None if row[5] is None else tuple(hh.decode_form(row[5])),
            outcome_sha256=str(row[6]),
        )
        for row in conn.execute(_DISCOVERY_OUTCOMES).fetchall()
    ]


def _frozen_declarations(conn: psycopg.Connection[Any]) -> list[hh.HuntDeclaration]:
    return [hh.hunt_declaration_from_row(row) for row in conn.execute(_FROZEN_DECLARATIONS).fetchall()]


def _cells(statistics: Mapping[str, Any]) -> Mapping[str, Any]:
    cells = statistics.get("cells")
    return cells if isinstance(cells, Mapping) else {}


def stored_screening_p(status: str, statistics: Mapping[str, Any]) -> float:
    """BY's p for one stored discovery outcome: the largest base-cost p over its cells, 1
    when it was abandoned, has no cells, or any base cell refused (spec "BY")."""
    if status == "abandoned":
        return 1.0
    base = {key: cell for key, cell in _cells(statistics).items() if key.endswith("|base")}
    if not base:
        return 1.0
    worst = 0.0
    for cell in base.values():
        if not isinstance(cell, Mapping) or "refused" in cell:
            return 1.0
        p = cell.get("p")
        if p == hunt_inference.P_UNDERFLOW_LABEL:
            p = 0.0
        if not isinstance(p, float | int) or isinstance(p, bool) or not (0.0 <= p <= 1.0):
            raise hh.HuntHarnessError(f"a stored base cell carries an invalid p {p!r}")
        worst = max(worst, float(p))
    return worst


def _ruined(statistics: Mapping[str, Any]) -> bool:
    cell = _cells(statistics).get(CANONICAL_CELL)
    return isinstance(cell, Mapping) and cell.get("refused") == "book_ruin"


# ---------------------------------------------------------------------------
# The numbers a declaration pins
# ---------------------------------------------------------------------------


def _pin_form(spec: TrialSpec, discovery: _DiscoveryOutcome) -> dict[str, Any]:
    return {
        "spec_sha256": spec.spec_sha256,
        "candidate_sha256": spec.candidate_sha256,
        # Decoded: ``_document`` canonicalises the whole document once.
        "spec": hh.decode_form(spec.form()),
        "discovery_hunt_trial_id": discovery.hunt_trial_id,
        "discovery_outcome_sha256": discovery.outcome_sha256,
    }


def _pinned_keys(declarations: Iterable[Mapping[str, Any]]) -> set[tuple[str, str]]:
    return {(str(pin["candidate_sha256"]), str(pin["spec"]["split"])) for doc in declarations for pin in doc["pins"]}


def declaration_numbers(
    conn: psycopg.Connection[Any],
    *,
    hunt_id: str,
    pins: Sequence[TrialSpec],
    register: TrialRegister,
) -> tuple[dict[str, Any], list[str]]:
    """Every freeze-time number of a validation declaration, and every refusal code.

    ⚠ Read in the caller's snapshot: the freeze calls it inside one REPEATABLE READ
    transaction under the programme lock (spec "Atomicity").
    """
    codes: list[str] = []
    inherited = register.inherited_floor()
    outcomes = _discovery_outcomes(conn)
    frozen = _frozen_declarations(conn)
    by_candidate = {(outcome.hunt_id, outcome.candidate_sha256): outcome for outcome in outcomes}

    # --- M = M_inh + every hunt_trials row + every pinned spec with no row yet ---
    totals = conn.execute(_LOG_TOTALS).fetchone()
    rows_total, any_floor_row, discovery_rows = (0, False, 0) if totals is None else totals
    evaluated = {(str(row[0]), str(row[1])) for row in conn.execute(_EVALUATE_KEYS).fetchall()}
    this_doc = {"pins": [{"candidate_sha256": spec.candidate_sha256, "spec": {"split": spec.split}} for spec in pins]}
    reserved = _pinned_keys([*(declaration.doc for declaration in frozen), this_doc]) - evaluated
    m = inherited.searches + int(rows_total) + len(reserved)

    # --- V[SR] over this hunt's discovery evaluate trials with an outcome ---
    variance = hunt_inference.trial_sharpe_variance(
        VPopulationMember(outcome.hunt_trial_id, outcome.active_series, _ruined(outcome.statistics))
        for outcome in outcomes
        if outcome.hunt_id == hunt_id
    )
    if isinstance(variance, StatRefused):
        codes.append(f"v_population_{variance.reason}")
        v_form: dict[str, Any] = variance.form()
    else:
        v_form = variance.form()

    # --- BY, programme-wide, at this readout ---
    m_by = inherited.searches + int(discovery_rows)
    p_values = {outcome.hunt_trial_id: stored_screening_p(outcome.status, outcome.statistics) for outcome in outcomes}
    by = hunt_inference.by_screen(p_values, m=max(m_by, 1))
    flagged = sorted(cast(frozenset[int], by.flagged))

    # --- pins ---
    pin_forms: list[dict[str, Any]] = []
    power: dict[str, Any] = {}
    shadow_dates = shadow_weeks = 0
    other_pins = _pinned_keys(declaration.doc for declaration in frozen if declaration.hunt_id != hunt_id)
    seen: set[str] = set()
    for spec in pins:
        label = spec.spec_sha256[:12]
        if spec.hunt_id != hunt_id or spec.split != "validation":
            codes.append(f"pin_{label}_not_{hunt_id}_validation")
            continue
        if spec.harness_model_id != hh.HUNT_HARNESS_MODEL_ID:
            codes.append(f"pin_{label}_stale_harness_model")
        if spec.candidate_sha256 in seen:
            codes.append(f"pin_{label}_duplicate_candidate")
        seen.add(spec.candidate_sha256)
        if (spec.candidate_sha256, spec.split) in other_pins:
            codes.append(f"pin_{label}_pinned_by_other_hunt")
        if hh.burning_look(conn, split=spec.split, candidate_sha256=spec.candidate_sha256, hunt_id=hunt_id) is not None:
            codes.append(f"pin_{label}_split_burned")
        discovery = by_candidate.get((hunt_id, spec.candidate_sha256))
        if discovery is None:
            codes.append(f"pin_{label}_no_discovery_outcome")
            continue
        if discovery.hunt_trial_id not in by.flagged:
            codes.append(f"pin_{label}_not_by_flagged")
        pin_forms.append(_pin_form(spec, discovery))
        grid = hh.split_grid("validation", lag=spec.lag, h=spec.h)
        if isinstance(grid, StatRefused):
            power[spec.spec_sha256] = {"blocked": f"target_{grid.reason}"}
        else:
            power[spec.spec_sha256] = power_statement(
                discovery.active_series, target_observations=len(grid.sessions), h=spec.h
            )
            span = hh.split_sessions("validation")
            first, last = span[grid.first], span[grid.last]
            shadow_dates = max(shadow_dates, len(grid.formations) // spec.h)
            shadow_weeks = max(shadow_weeks, math.ceil((last - first).days / 7))
        if "blocked" in power[spec.spec_sha256]:
            codes.append(f"pin_{label}_power_{power[spec.spec_sha256]['blocked']}")
    if not pins:
        codes.append("no_pins")

    numbers = {
        # ⚠ No register version here: the register entry claiming this document names its
        # sha256, so the version that entry creates cannot be inside it. The freeze stores the
        # version it read in ``hunt_declarations.register_version``.
        "inherited": {"searches": inherited.searches, "is_floor": inherited.is_floor},
        "m": m,
        "m_is_floor": bool(inherited.is_floor or any_floor_row),
        "reserved_pins": len(reserved),
        "v_sr": v_form,
        "by": {
            "m": by.m,
            "c_m": by.c_m,
            "q": by.q,
            "p": {str(key): value for key, value in sorted(p_values.items())},
            "flagged": flagged,
        },
        "power": dict(sorted(power.items())),
        "forward_shadow": {"min_decision_dates": shadow_dates, "min_calendar_weeks": shadow_weeks},
    }
    pin_forms.sort(key=lambda pin: pin["spec_sha256"])
    return {"pins": pin_forms, "numbers": numbers}, codes


def _discovery_completeness(conn: psycopg.Connection[Any], hunt_id: str) -> list[str]:
    """The freeze refuses while any discovery registration of the hunt lacks an outcome."""
    missing = [
        int(row[0])
        for row in conn.execute(_HUNT_DISCOVERY_ROWS, {"hunt": hunt_id}).fetchall()
        if row[1] == "evaluate" and not row[4]
    ]
    return [f"discovery_registration_without_outcome:{','.join(map(str, missing))}"] if missing else []


# ---------------------------------------------------------------------------
# The register tripwire (obligations 108, 110)
# ---------------------------------------------------------------------------


def discovery_log_query(hunt_id: str, closed_at: datetime) -> str:
    return DISCOVERY_LOG_QUERY.format(hunt=hunt_id, closed_at=closed_at.isoformat())


def discovery_register_evidence(conn: psycopg.Connection[Any], hunt_id: str) -> tuple[str, TrialExactness, int]:
    """What the PR closing a hunt's discovery pastes into ``hunt-<n>-discovery``:
    ``(evidence, exactness, searches)``, closed on the DB clock (obligation 110)."""
    if not hh.HUNT_ID_PATTERN.match(hunt_id):
        raise ValueError(f"bad hunt id {hunt_id!r}")
    now = conn.execute("SELECT now()").fetchone()
    if now is None:
        raise hh.HuntHarnessError("the database returned no clock")
    closed_at = cast(datetime, now[0]).astimezone(UTC)
    rows = [row for row in conn.execute(_HUNT_DISCOVERY_ROWS, {"hunt": hunt_id}).fetchall() if row[3] <= closed_at]
    conn.commit()
    ids = [str(row[0]) for row in rows]
    exactness = TrialExactness.FLOOR if any(row[2] for row in rows) else TrialExactness.EXACT
    evidence = log_backed_evidence(query=discovery_log_query(hunt_id, closed_at), closed_at=closed_at, trial_ids=ids)
    return evidence, exactness, len(ids)


def _register_entry(register: TrialRegister, trial_id: str) -> DeclaredTrial | None:
    return next((trial for trial in register.trials if trial.trial_id == trial_id), None)


def tripwire(
    conn: psycopg.Connection[Any],
    *,
    hunt_id: str,
    register: TrialRegister,
    doc_path: str,
    doc_sha256: str,
    pinned: int,
    frozen: Sequence[hh.HuntDeclaration],
) -> list[str]:
    """``register_disagrees_with_log`` codes (spec "Trial register", obligation 108)."""
    codes: list[str] = []
    discovery_id = f"{hunt_id}-discovery"
    discovery = _register_entry(register, discovery_id)
    rows = conn.execute(_HUNT_DISCOVERY_ROWS, {"hunt": hunt_id}).fetchall()
    if discovery is None:
        codes.append(f"register_disagrees_with_log:{discovery_id}_missing")
    else:
        evidence = parse_log_backed_evidence(discovery.evidence)
        if evidence is None:
            codes.append(f"register_disagrees_with_log:{discovery_id}_evidence_unparsed")
        else:
            counted = [row for row in rows if row[3] <= evidence.closed_at]
            ids = [str(row[0]) for row in counted]
            if evidence.query != discovery_log_query(hunt_id, evidence.closed_at):
                codes.append(f"register_disagrees_with_log:{discovery_id}_query_not_canonical")
            if (ordered_ids_sha256(ids), len(ids)) != (evidence.ids_sha256, evidence.searches):
                codes.append(f"register_disagrees_with_log:{discovery_id}_ids")
            if len(counted) != len(rows):
                codes.append(f"register_disagrees_with_log:{discovery_id}_rows_newer_than_close")
            floor = any(row[2] for row in counted)
            if (discovery.exactness is TrialExactness.FLOOR) != floor:
                codes.append(f"register_disagrees_with_log:{discovery_id}_exactness")
    entries = [(f"{hunt_id}-validation", doc_path, doc_sha256, pinned)] + [
        (hh.declaration_strategy_id(d.hunt_id, d.split), d.doc_path, d.doc_sha256, len(d.doc["pins"])) for d in frozen
    ]
    for trial_id, path, sha, count in entries:
        entry = _register_entry(register, trial_id)
        parsed = None if entry is None else parse_declaration_backed_evidence(entry.evidence)
        if entry is None or parsed is None:
            codes.append(f"register_disagrees_with_log:{trial_id}_missing")
        elif (parsed.declaration_path, parsed.declaration_sha256, entry.searches) != (path, sha, count):
            codes.append(f"register_disagrees_with_log:{trial_id}_pins")
    return codes


# ---------------------------------------------------------------------------
# Building, writing and freezing
# ---------------------------------------------------------------------------


def _document(hunt_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
    return hh.canonical_form(
        {
            "kind": hh.HUNT_DECLARATION_KIND,
            "hunt_id": hunt_id,
            "split": "validation",
            "harness_model_id": hh.HUNT_HARNESS_MODEL_ID,
            **body,
        }
    )


def _repeatable_read(conn: psycopg.Connection[Any]) -> None:
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")


def build_validation_declaration(
    conn: psycopg.Connection[Any], *, hunt_id: str, pins: Sequence[TrialSpec], register: TrialRegister = TRIAL_REGISTER
) -> tuple[dict[str, Any], list[str]]:
    """The declaration document for ``pins`` and every code that would refuse its freeze.
    A readout: it reads stored outcomes only and writes nothing."""
    with hh.hunt_programme_lock(conn):
        _repeatable_read(conn)
        body, codes = declaration_numbers(conn, hunt_id=hunt_id, pins=pins, register=register)
        codes += _discovery_completeness(conn, hunt_id)
        conn.commit()
    return _document(hunt_id, body), codes


def document_bytes(doc: Any) -> bytes:
    """The committed file IS the canonical JSON, so the file's sha256 is the document's."""
    return (hh.dumps_form(doc) + "\n").encode("utf-8")


def document_sha256(doc: Any) -> str:
    return hh.sha256_form(doc)


def write_declaration(path: Path, doc: Any) -> str:
    path.write_bytes(document_bytes(doc))
    return document_sha256(doc)


def _read_document(doc_path: str) -> tuple[dict[str, Any], str]:
    raw = (REPO_ROOT / doc_path).read_bytes()
    doc = json.loads(raw)
    if document_bytes(doc) != raw:
        raise HuntDeclarationRefused(str(doc.get("hunt_id")), ["document_not_canonical"])
    return doc, document_sha256(doc)


def forward_shadow_floor(numbers: Mapping[str, Any]) -> ForwardShadowFloor:
    shadow = numbers["forward_shadow"]
    return ForwardShadowFloor(
        min_independent_decision_dates=int(shadow["min_decision_dates"]),
        min_calendar_weeks=int(shadow["min_calendar_weeks"]),
        derivation=(
            "BY CONSTRUCTION, no power calculation is published for a calendar-time active series: the largest "
            "validation evidence supply among the pinned candidates, as floor(formations / h) non-overlapping "
            "holds and the grid's span in weeks (#2901's precedent: the floor equals the evidence behind the "
            "pass). A forward record for a promoted candidate is a different strategy_version with its own "
            "declaration (programme step 8)."
        ),
    )


def freeze_validation_declaration(
    conn: psycopg.Connection[Any],
    *,
    doc_path: str,
    declared_by: str,
    register: TrialRegister = TRIAL_REGISTER,
) -> int:
    """Freeze ``hunt-<n>-validation`` from the committed document at ``doc_path``.

    Returns the #2599 ``declaration_id``. Refuses (``HuntDeclarationRefused``, nothing
    written) on: a non-canonical or foreign document; any tripwire code; any discovery
    registration without an outcome; any recomputed number or pin that differs from the
    document (obligation 122); and every refusal ``declaration_numbers`` names.
    """
    doc, doc_sha256 = _read_document(doc_path)
    hunt_id = str(doc.get("hunt_id"))
    header_codes = []
    if doc.get("kind") != hh.HUNT_DECLARATION_KIND or doc.get("split") != "validation":
        header_codes.append("document_not_a_validation_declaration")
    if not hh.HUNT_ID_PATTERN.match(hunt_id):
        header_codes.append("document_hunt_id_invalid")
    if doc.get("harness_model_id") != hh.HUNT_HARNESS_MODEL_ID:
        header_codes.append("document_harness_model_stale")
    if header_codes:
        raise HuntDeclarationRefused(hunt_id, header_codes)
    pins = [TrialSpec.from_form(pin["spec"]) for pin in doc["pins"]]

    with hh.hunt_programme_lock(conn):
        _repeatable_read(conn)
        frozen = _frozen_declarations(conn)
        # Closure is read under the programme lock (spec "Budget and closure").
        codes = ["hunt_closed"] if hunt_id in hh.HUNT_CLOSED else []
        codes += tripwire(
            conn,
            hunt_id=hunt_id,
            register=register,
            doc_path=doc_path,
            doc_sha256=doc_sha256,
            pinned=len(pins),
            frozen=frozen,
        )
        codes += _discovery_completeness(conn, hunt_id)
        body, number_codes = declaration_numbers(conn, hunt_id=hunt_id, pins=pins, register=register)
        codes += number_codes
        recomputed = _document(hunt_id, body)
        for key in sorted(set(recomputed) | set(doc)):
            if recomputed.get(key) != doc.get(key):
                codes.append(f"declaration_{key}_stale")
        if codes:
            conn.rollback()
            raise HuntDeclarationRefused(hunt_id, codes)
        declaration = PreregDeclaration(
            strategy_id=hh.declaration_strategy_id(hunt_id, "validation"),
            strategy_version=hh.HUNT_DECLARATION_VERSION,
            contract_version=hh.declaration_contract_version(doc_sha256),
            prereg_purpose="capital_candidate",
            structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
            declared_universe_basis=UNIVERSE_BASIS,
            declared_carry_unmodelled=CARRY_UNMODELLED,
            declared_fx_unmodelled=FX_UNMODELLED,
            expected_structural_refusals=structural_promotion_refusals(
                universe_basis=UNIVERSE_BASIS, carry_unmodelled=CARRY_UNMODELLED, fx_unmodelled=FX_UNMODELLED
            ),
            forward_shadow=forward_shadow_floor(body["numbers"]),
            declared_by=declared_by,
        )
        declaration_id = freeze_preregistration(cast(psycopg.Connection[tuple], conn), declaration)
        conn.execute(
            """
            INSERT INTO hunt_declarations (
                declaration_id, hunt_id, split, doc_path, doc, doc_sha256, register_version
            ) VALUES (
                %(declaration_id)s, %(hunt)s, 'validation', %(path)s, %(doc)s::jsonb, %(sha)s, %(register)s
            )
            """,
            {
                "declaration_id": declaration_id,
                "hunt": hunt_id,
                "path": doc_path,
                "doc": hh.dumps_form(doc),
                "sha": doc_sha256,
                "register": register.version,
            },
        )
        conn.commit()
    return declaration_id


# ---------------------------------------------------------------------------
# The validation readout (slice 2b-ii)
# ---------------------------------------------------------------------------

#: #3384 Finding 5: the validation window was examined before this programme existed.
PREVIOUSLY_EXAMINED: Final = "previously_examined"
#: Contract decision 114: a look recorded after the freeze qualifies the readout.
QUALIFIED_BY_LATE_LOOK: Final = "qualified_by_late_look"

_SELECT_PIN_OUTCOME = """
    SELECT t.hunt_trial_id, o.hunt_trial_id IS NOT NULL
    FROM hunt_trials t
    LEFT JOIN hunt_trial_outcomes o USING (hunt_trial_id)
    WHERE t.candidate_sha256 = %(candidate)s AND t.split = %(split)s AND t.purpose = 'evaluate'
"""

_SELECT_ACCESS = """
    SELECT a.strategy_id, a.strategy_version, a.access_kind, a.declaration_id, a.accessed_at > d.frozen_at
    FROM strategy_holdout_accesses a
    JOIN strategy_preregistration_declarations d ON d.declaration_id = a.declaration_id
    WHERE a.access_id = %(access_id)s
"""

#: ⚠ PROGRAMME-WIDE ON PURPOSE, not scoped to this hunt: the frozen M counts every
#: ``hunt_trials`` row of EVERY hunt (spec "DSR", M), so a look recorded in any hunt after
#: this freeze makes the frozen M an under-count, and the readout is qualified by it
#: (contract decision 114). ``hunt_declarations`` supplies only this freeze's time.
_SELECT_LATE_LOOKS = """
    SELECT count(*)
    FROM hunt_trials
    WHERE purpose = 'recorded_after'
      AND registered_at > (SELECT frozen_at FROM hunt_declarations WHERE declaration_id = %(declaration_id)s)
"""

_SELECT_REGISTER_VERSION = "SELECT register_version FROM hunt_declarations WHERE declaration_id = %(declaration_id)s"


@dataclass(frozen=True)
class CandidateReadout:
    spec_sha256: str
    #: ``None`` while the pinned spec has no stored outcome.
    hunt_trial_id: int | None
    verdict: hunt_inference.Verdict | None
    reasons: tuple[str, ...]
    #: Per base cell: the DSR, or the reason it was refused.
    dsr: Mapping[str, float | str]
    #: Descriptive, never a verdict input: the canonical active series' mean and session
    #: count before and after ``SURVIVOR_CONDITIONING_DATE`` (spec "Survivor conditioning").
    survivor_subspans: Mapping[str, Mapping[str, float | int | None]]


@dataclass(frozen=True)
class ValidationReadout:
    hunt_id: str
    declaration_id: int
    #: Every pinned spec has an outcome; only then are verdicts final and a closure given.
    complete: bool
    candidates: tuple[CandidateReadout, ...]
    closure: hunt_inference.HuntClosure | None
    m: int
    m_is_floor: bool
    labels: tuple[str, ...]


def _refused_form(form: Any) -> StatRefused | None:
    if isinstance(form, Mapping) and "refused" in form:
        return StatRefused(cast(hunt_inference.StatRefusalReason, str(form["refused"])), str(form.get("detail", "")))
    return None


def candidate_verdict(
    status: str,
    statistics: Mapping[str, Any],
    *,
    variance: hunt_inference.TrialSharpeVariance,
    declared_trials: int,
    trial_register_version: str,
) -> tuple[hunt_inference.VerdictResult, dict[str, float | str]]:
    """One stored validation (or holdout) outcome's verdict (spec "Verdicts"). Pure.

    Base cells are deflated from their stored moments against the declaration's frozen
    M and V[SR]; a DSR refusal refuses its base cell. An abandoned outcome, or one with no
    cells, is ``NOT_PASS_REFUSED``.
    """
    if status == "abandoned":
        return hunt_inference.VerdictResult(hunt_inference.Verdict.NOT_PASS_REFUSED, ("abandoned",)), {}
    cells = _cells(statistics)
    base: dict[str, hunt_inference.BaseReadout | StatRefused] = {}
    stress: dict[str, hunt_inference.StressReadout | StatRefused] = {}
    dsr_out: dict[str, float | str] = {}
    for key, form in cells.items():
        name, _, cost = str(key).rpartition("|")
        refused = _refused_form(form)
        if cost == "stress":
            if refused is not None:
                stress[name] = refused
            else:
                cell = hunt_inference.CellStatistics.from_form(form)
                stress[name] = hunt_inference.StressReadout(t_stat=cell.t_stat, mean=cell.mean)
            continue
        if cost != "base":
            raise hh.HuntHarnessError(f"unknown cell {key!r}")
        if refused is not None:
            base[name] = refused
            dsr_out[name] = f"refused: {refused.reason}"
            continue
        cell = hunt_inference.CellStatistics.from_form(form)
        dsr = hunt_inference.stored_hunt_dsr(
            form,
            variance=variance,
            declared_trials=declared_trials,
            trial_register_version=trial_register_version,
        )
        if isinstance(dsr, StatRefused):
            base[name] = dsr
            dsr_out[name] = f"refused: {dsr.reason}"
            continue
        dsr_out[name] = dsr.result.deflated_sharpe
        base[name] = hunt_inference.BaseReadout(t_stat=cell.t_stat, dsr=dsr.result.deflated_sharpe, mean=cell.mean)
    if not base:
        grid = _refused_form(statistics.get("grid"))
        reason = f"grid: {grid.reason}" if grid is not None else "no base cell was computed"
        return hunt_inference.VerdictResult(hunt_inference.Verdict.NOT_PASS_REFUSED, (reason,)), dsr_out
    return hunt_inference.decide_verdict(base, stress), dsr_out


def survivor_subspans(
    active: Sequence[float] | None, statistics: Mapping[str, Any], split: hh.Split
) -> dict[str, dict[str, float | int | None]]:
    """mean(a) and session count of the canonical series on each side of 2013-06-21.

    The series sits on the fixed grid: one value per split session from the stored
    ``grid.first_session``. Descriptive only; selecting a sub-span is a new trial.
    """
    grid = statistics.get("grid")
    if active is None or not isinstance(grid, Mapping) or "first_session" not in grid:
        return {}
    sessions = hh.split_sessions(split)
    first = sessions.index(date.fromisoformat(str(grid["first_session"])))
    days = sessions[first : first + len(active)]
    if len(days) != len(active):
        raise hh.HuntHarnessError("the stored active series runs past the split's sessions")
    spans: dict[str, list[float]] = {"before": [], "on_or_after": []}
    for day, value in zip(days, active, strict=True):
        spans["before" if day < hh.SURVIVOR_CONDITIONING_DATE else "on_or_after"].append(value)
    return {
        name: {"sessions": len(values), "mean": math.fsum(values) / len(values) if values else None}
        for name, values in spans.items()
    }


def _check_stored_access(
    conn: psycopg.Connection[Any], outcome_access: int | None, frozen: Any, strategy_id: str
) -> None:
    """Contract decision 96: a readout verifies each stored outcome's provenance and opens
    no fresh access. Any failure is an integrity fault, never a verdict."""
    if outcome_access is None:
        raise hh.HuntHarnessError("a validation outcome has no access row")
    row = conn.execute(_SELECT_ACCESS, {"access_id": outcome_access}).fetchone()
    if row is None:
        raise hh.HuntHarnessError(f"access {outcome_access} has no declaration")
    access_strategy, version, kind, declaration_id, frozen_before = row
    if (access_strategy, version, kind) != (strategy_id, hh.HUNT_DECLARATION_VERSION, "read"):
        raise hh.HuntHarnessError(f"access {outcome_access} is {access_strategy}/{version}/{kind}")
    if declaration_id not in frozen.chain_declaration_ids or not frozen_before:
        raise hh.HuntHarnessError(f"access {outcome_access} was not authorised by the frozen declaration")


def validation_readout(conn: psycopg.Connection[Any], hunt_id: str) -> ValidationReadout:
    """The hunt's validation verdicts: the only sanctioned route to them (spec "The audited door").

    Under the programme lock, in one REPEATABLE READ snapshot. It reads stored outcomes
    only, re-verifies each one's hash and access provenance, and deflates against the M,
    V[SR] and register version FROZEN with the declaration, never today's.
    """
    strategy_id = hh.declaration_strategy_id(hunt_id, "validation")
    with hh.hunt_programme_lock(conn):
        _repeatable_read(conn)
        frozen = load_preregistration(cast(psycopg.Connection[tuple], conn), strategy_id, hh.HUNT_DECLARATION_VERSION)
        if frozen is None or not frozen.digest_intact:
            raise hh.HuntHarnessError(f"{strategy_id} has no intact frozen declaration")
        declaration = hh.load_chain_hunt_declaration(conn, frozen)
        if declaration is None or frozen.declaration.contract_version != hh.declaration_contract_version(
            declaration.doc_sha256
        ):
            raise hh.HuntHarnessError(f"{strategy_id}'s declaration document is missing or not the frozen one")
        numbers = declaration.doc["numbers"]
        variance = hunt_inference.TrialSharpeVariance.from_form(hh.decode_form(numbers["v_sr"]))
        m = int(numbers["m"])
        register_row = conn.execute(_SELECT_REGISTER_VERSION, {"declaration_id": declaration.declaration_id}).fetchone()
        late = conn.execute(_SELECT_LATE_LOOKS, {"declaration_id": declaration.declaration_id}).fetchone()
        if register_row is None or late is None:
            raise hh.HuntHarnessError(f"{strategy_id}'s declaration row vanished inside the snapshot")
        candidates: list[CandidateReadout] = []
        for pin in declaration.doc["pins"]:
            spec_sha256 = str(pin["spec_sha256"])
            row = conn.execute(
                _SELECT_PIN_OUTCOME, {"candidate": str(pin["candidate_sha256"]), "split": "validation"}
            ).fetchone()
            if row is None or not row[1]:
                candidates.append(CandidateReadout(spec_sha256, None if row is None else int(row[0]), None, (), {}, {}))
                continue
            outcome = hh.read_outcome(conn, int(row[0]))
            if outcome.spec_sha256 != spec_sha256:
                raise hh.HuntHarnessError(f"trial {row[0]}'s registered spec is not the pinned {spec_sha256}")
            _check_stored_access(conn, outcome.access_id, frozen, strategy_id)
            result, dsr = candidate_verdict(
                outcome.status,
                outcome.statistics,
                variance=variance,
                declared_trials=m,
                trial_register_version=str(register_row[0]),
            )
            candidates.append(
                CandidateReadout(
                    spec_sha256,
                    outcome.hunt_trial_id,
                    result.verdict,
                    result.reasons,
                    dsr,
                    survivor_subspans(outcome.active_series, outcome.statistics, "validation"),
                )
            )
        conn.commit()
    complete = all(candidate.verdict is not None for candidate in candidates)
    closure = (
        hunt_inference.validation_closure(
            {candidate.spec_sha256: cast(hunt_inference.Verdict, candidate.verdict) for candidate in candidates}
        )
        if complete
        else None
    )
    labels = [PREVIOUSLY_EXAMINED]
    if int(late[0]) > 0:
        labels.append(QUALIFIED_BY_LATE_LOOK)
    return ValidationReadout(
        hunt_id=hunt_id,
        declaration_id=frozen.declaration_id,
        complete=complete,
        candidates=tuple(candidates),
        closure=closure,
        m=m,
        m_is_floor=bool(numbers["m_is_floor"]),
        labels=tuple(labels),
    )


__all__ = [
    "CandidateReadout",
    "HuntDeclarationRefused",
    "ValidationReadout",
    "candidate_verdict",
    "validation_readout",
    "build_validation_declaration",
    "declaration_numbers",
    "discovery_register_evidence",
    "document_bytes",
    "document_sha256",
    "forward_shadow_floor",
    "freeze_validation_declaration",
    "power_statement",
    "stored_screening_p",
    "tripwire",
    "write_declaration",
]
