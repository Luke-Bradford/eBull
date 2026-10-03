"""Ranking-pot-v2 looks: v1's §9.3 verdict plus v2's conditions 6–7 (#3592 slice 4b-i; spec
``2026-10-03-3592-ranking-pot-v2.md`` §7 "Looks" and "Verdict"; Appendix A R2-16–20).

v1's pure pieces are imported from ``ranking_pot_look`` (in v2's hash): ``LookFacts`` (the accumulator), ``evaluate``
(endpoints, minimum evidence, T, paths, conditions 1–4, harm), the endpoint arithmetic and the hash-agnostic reads
(``first_target_session``, ``_spy_half_spreads``, ``_stored_looks``, ``_lock``) and ``reconcile_state``. What v1's
module does with v1's policy hash (``compute_due_looks``, which checks ``rb._policy_ok`` and stamps
``RANKING_POT_POLICY_HASH``) and with the executed ledger (``_execution``) is copy-adapted here instead.

What differs from v1, and nothing else:

- **Execution.** Zero (``Execution(0, 0)``): v2 has no executed book (sql/463), so condition 5 is false by
  construction and v1's verdict is at best ``shadow_pass_execution_unproven``. A ``pass`` raises.
- **α.** The declaration's frozen ``per_look_alpha`` / ``harm_alpha`` (``ranking_pot_look.terms_of``), with the K + 3
  layout checked first (``ranking_pot_v2_step.book_terms``'s check, inline: the step imports this module);
  ``LookFacts`` refuses a control column not K wide.
- **Condition 6, v1-reference.** The reference book's stored step document (``ranking_pot_steps.reference``, the
  shadow's format) streams through its own ``LookFacts`` with no controls, so its T, path, lifecycles and occupancy
  are the shadow's constructions. Holds iff the shadow's T **>** the reference's T (a tie fails) and the shadow's path
  at E ≥ the reference's. Unevaluable (v2 reasons): ``reference_below_minimum`` when the reference has fewer than
  ``MIN_LIFECYCLES_PER_SLOT`` × N lifecycles or mean occupancy below ``MIN_OCCUPANCY`` (v1's minimum evidence),
  ``reference_t_undefined`` when it has no records.
- **Condition 7, turnover (§2).** The mean over the decided rebalances applied in T₀ … E **after the first** (the
  initial fill is not turnover) of the shadow's ``entered`` at that step (lifecycles opened at the target session) / N,
  against the declaration's frozen ``turnover_bar`` (≤). Months whose target session has no decided attempt are not in
  the mean; their count is stored. ``None`` with no rebalance after the first, which minimum evidence already makes
  unevaluable (lifecycles open only at applied rebalances, so ≥ 2N lifecycles needs one after the first fill).
- **One representation, one insert (§7 "Verdict").** The ``result`` row's ``verdict`` / ``harm`` / ``reasons`` are
  v1's own; ``detail.v2`` holds conditions 6–7 with their operands, the v2 reasons and ``v2_verdict``:
  ``unevaluable`` if any v1 or v2 reason holds, else ``research_pass`` iff v1's verdict is
  ``shadow_pass_execution_unproven`` and 6 and 7 hold, else ``not_passed``. Readers decode it with ``v2_detail_of``,
  which raises when it is absent or malformed.
- **Policy.** v2's hash (``ranking_pot_v2_job.policy_ok``); the row is stamped ``RANKING_POT_V2_POLICY_HASH``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any, Final, Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.services import ranking_pot_look as look
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services.ranking_pot_v2_job import policy_ok
from app.services.ranking_pot_v2_policy import LOOK_MONTHS, RANKING_POT_V2_POLICY_HASH

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

V2Verdict = Literal["research_pass", "not_passed", "unevaluable"]
V2_VERDICTS: Final = frozenset({"research_pass", "not_passed", "unevaluable"})
V2_REASONS: Final = frozenset({"reference_below_minimum", "reference_t_undefined"})
#: v2 has no executed book: condition 5 can never hold.
NO_EXECUTION: Final = look.Execution(executing=timedelta(0), lifecycles=0)


# ---------------------------------------------------------------------------
# Pure: turnover
# ---------------------------------------------------------------------------
@dataclass
class TurnoverFacts:
    """§2's turnover operands, accumulated from the step rows T₀ … E in order."""

    t0: date
    #: (target session, shadow lifecycles opened there) per applied rebalance after the first, in order.
    entries: list[tuple[date, int]] = field(default_factory=list)
    first_seen: bool = False

    def add(self, session: date, applied: bool, entered: int) -> None:
        if not applied:
            return
        if not self.first_seen:
            if session != self.t0:
                raise ValueError(f"the first applied rebalance targets {session}, not T0 {self.t0}")
            self.first_seen = True
            return
        self.entries.append((session, entered))


def turnover(entries: list[tuple[date, int]], n: int) -> Fraction | None:
    """Mean over the rebalances after the first of entries / N; ``None`` when there are none."""
    if not entries:
        return None
    return sum((Fraction(e, n) for _, e in entries), Fraction(0)) / len(entries)


# ---------------------------------------------------------------------------
# Pure: the v2 verdict
# ---------------------------------------------------------------------------
def evaluate(
    shadow: look.LookFacts,
    reference: look.LookFacts,
    turnover_facts: TurnoverFacts,
    terms: look.Terms,
    *,
    h0: Decimal,
    h_end: Decimal,
    turnover_bar: Fraction,
    skipped_targets: int,
) -> look.LookResult:
    """§7: v1's evaluation with zero execution, then conditions 6–7 and ``v2_verdict`` in ``detail.v2``."""
    v1 = look.evaluate(shadow, terms, h0=h0, h_end=h_end, execution=NO_EXECUTION)
    if v1.verdict == "pass":
        raise rb.SnapshotIntegrityError("a v2 look passed condition 5: v2 has no executed book")

    if reference.k != 0:
        raise ValueError("the reference book's facts carry no controls")
    # v1's T (``ranking_pot_look._t``, Decimal under ``ranking_pot_sim.CTX``): t_shadow is v1's stored ``t_obs``.
    t_shadow = look._t(shadow.shadow_sum, shadow.shadow_count)
    t_ref = look._t(reference.shadow_sum, reference.shadow_count)
    ref_lifecycles = len(reference.invested)
    ref_occupancy = Fraction(reference.held_total, reference.sessions * terms.n) if reference.sessions else Fraction(0)
    reasons: list[str] = []
    if ref_lifecycles < look.MIN_LIFECYCLES_PER_SLOT * terms.n or ref_occupancy < look.MIN_OCCUPANCY:
        reasons.append("reference_below_minimum")
    if t_ref is None:
        reasons.append("reference_t_undefined")
    shadow_end, ref_end = shadow.shadow_path[-1], reference.shadow_path[-1]
    c6: bool | None = None if t_shadow is None or t_ref is None else t_shadow > t_ref and shadow_end >= ref_end

    mean = turnover(turnover_facts.entries, terms.n)
    c7: bool | None = None if mean is None else mean <= turnover_bar

    v2_verdict: V2Verdict
    if v1.reasons or reasons:
        v2_verdict = "unevaluable"
    elif v1.verdict == "shadow_pass_execution_unproven" and c6 is True and c7 is True:
        v2_verdict = "research_pass"
    else:
        v2_verdict = "not_passed"

    detail = dict(v1.detail)
    detail["v2"] = {
        "reasons": reasons,
        "conditions": {"6_reference": c6, "7_turnover": c7},
        "reference": {
            "t_shadow": look._f(t_shadow),
            "t_reference": look._f(t_ref),
            "shadow_return": look._f(shadow_end - 1),
            "reference_return": look._f(ref_end - 1),
            "lifecycles": ref_lifecycles,
            "occupancy": look._f(ref_occupancy),
            "sessions": reference.sessions,
        },
        "turnover": {
            "mean": look._f(mean),
            "bar": look._f(turnover_bar),
            "rebalances": len(turnover_facts.entries),
            "entered": [[d.isoformat(), e] for d, e in turnover_facts.entries],
            "skipped_targets": skipped_targets,
        },
        "v2_verdict": v2_verdict,
    }
    return look.LookResult(v1.verdict, v1.harm, v1.reasons, detail)


@dataclass(frozen=True)
class V2Detail:
    v2_verdict: V2Verdict
    reasons: tuple[str, ...]
    condition_6: bool | None
    condition_7: bool | None


def v2_detail_of(detail: Mapping[str, Any]) -> V2Detail:
    """A stored v2 look's ``detail.v2``; raises when it is absent or malformed (§7: readers fail closed)."""
    block = detail.get("v2")
    if not isinstance(block, Mapping):
        raise rb.SnapshotIntegrityError("a v2 look without its detail.v2 block")
    verdict, reasons, conditions = block.get("v2_verdict"), block.get("reasons"), block.get("conditions")
    if verdict not in V2_VERDICTS:
        raise rb.SnapshotIntegrityError(f"detail.v2: v2_verdict {verdict!r}")
    if not isinstance(reasons, list) or not all(r in V2_REASONS for r in reasons):
        raise rb.SnapshotIntegrityError(f"detail.v2: reasons {reasons!r}")
    if not isinstance(conditions, Mapping) or set(conditions) != {"6_reference", "7_turnover"}:
        raise rb.SnapshotIntegrityError(f"detail.v2: conditions {conditions!r}")
    c6, c7 = conditions["6_reference"], conditions["7_turnover"]
    if not all(c is None or isinstance(c, bool) for c in (c6, c7)):
        raise rb.SnapshotIntegrityError(f"detail.v2: conditions {conditions!r}")
    if verdict == "research_pass" and (reasons or c6 is not True or c7 is not True):
        raise rb.SnapshotIntegrityError("detail.v2: research_pass without its conditions")
    if reasons and verdict != "unevaluable":
        raise rb.SnapshotIntegrityError("detail.v2: v2 reasons on an evaluable verdict")
    return V2Detail(verdict, tuple(reasons), c6, c7)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class V2StepRow:
    shadow: look.StepRow
    reference: look.StepRow
    applied: bool


def _step_rows(conn: Conn, declaration_id: int, *, t0: date, end: date, at_end: bool) -> Iterator[V2StepRow]:
    """v1's ``_step_rows`` plus the reference document and whether a rebalance was applied."""
    col = "sum_return_charged" if at_end else "sum_return"
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT session, forced, shadow, reference, applied_attempt_id IS NOT NULL AS applied, "
            "inputs -> 'spy' AS spy, controls -> 'records' AS records, "
            f"controls -> '{col}' AS sums "  # noqa: S608 — `col` is one of two literals above
            "FROM ranking_pot_steps WHERE declaration_id = %s AND session >= %s AND session "
            + ("= %s" if at_end else "< %s")
            + " ORDER BY session",
            (declaration_id, t0, end),
        )
        for r in cur:
            if r["reference"] is None:
                raise rb.SnapshotIntegrityError(f"{r['session']}: a v2 step row without its reference book")
            spy = None if r["spy"] is None else sim.Bar(*(Decimal(v) for v in r["spy"]))
            controls = {"records": r["records"], col: r["sums"]}
            yield V2StepRow(
                look.StepRow(r["session"], r["forced"], r["shadow"], controls, spy),
                look.StepRow(r["session"], r["forced"], r["reference"], {"records": [], col: []}, spy),
                bool(r["applied"]),
            )


def _skipped_targets(conn: Conn, declaration_id: int, *, t0: date, end: date) -> int:
    """Target sessions in (T₀, E] with an attempt but no decided one: the months not in the turnover mean."""
    row = conn.execute(
        "SELECT count(DISTINCT target_session) FROM ranking_pot_rebalance_attempts a "
        "WHERE a.declaration_id = %(d)s AND a.target_session > %(t0)s AND a.target_session <= %(e)s "
        "AND NOT EXISTS (SELECT 1 FROM ranking_pot_rebalance_attempts b WHERE b.declaration_id = %(d)s "
        "                AND b.target_session = a.target_session AND b.outcome = 'decided')",
        {"d": declaration_id, "t0": t0, "e": end},
    ).fetchone()
    return 0 if row is None else int(row[0])


def compute_look(conn: Conn, decl: rb.PotDeclaration, *, months: int, t0: date) -> look.LookResult:
    """One v2 look from the stored rows (v1's ``compute_look`` with the reference and turnover beside the shadow).
    The caller has checked E_m was stepped."""
    t = decl.doc["terms"]
    # The K + 3 layout (``ranking_pot_v2_step.book_terms``'s check; that module imports this one).
    if t.get("book_count") != int(t["k_controls"]) + 3:
        raise rb.SnapshotIntegrityError(f"declaration {decl.declaration_id}: book_count is not K + 3")
    end = look.endpoint(t0, months)
    terms = look.terms_of(decl)
    shadow = look.LookFacts(t0=t0, endpoint=end, k=terms.k)
    reference = look.LookFacts(t0=t0, endpoint=end, k=0)
    turn = TurnoverFacts(t0=t0)

    def add(row: V2StepRow) -> None:
        shadow.add(row.shadow)
        reference.add(row.reference)
        turn.add(row.shadow.session, row.applied, int(row.shadow.shadow["entered"]))

    for row in _step_rows(conn, decl.declaration_id, t0=t0, end=end, at_end=False):
        add(row)
    last = list(_step_rows(conn, decl.declaration_id, t0=t0, end=end, at_end=True))
    if len(last) != 1:
        raise rb.SnapshotIntegrityError(f"declaration {decl.declaration_id}: endpoint {end} is not stepped")
    add(last[0])
    if shadow.sessions != look.sessions_between(t0, end):
        raise rb.SnapshotIntegrityError(f"declaration {decl.declaration_id}: step rows {t0}..{end} are not contiguous")
    spreads = look._spy_half_spreads(conn, decl.declaration_id, end)
    if not spreads or spreads[0][0] != t0:
        raise rb.SnapshotIntegrityError("the first decided snapshot does not target T0")
    result = evaluate(
        shadow,
        reference,
        turn,
        terms,
        h0=spreads[0][1],
        h_end=spreads[-1][1],
        turnover_bar=Fraction(t["turnover_bar"]),
        skipped_targets=_skipped_targets(conn, decl.declaration_id, t0=t0, end=end),
    )
    result.detail.update(look_months=months, anniversary=look.anniversary(t0, months).isoformat())
    return result


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------
def compute_due_looks(conn: Conn, decl: rb.PotDeclaration, *, as_of: datetime) -> list[str]:
    """v1's ``compute_due_looks`` under v2's hash: every look whose endpoint has been stepped and which has no
    ``result`` row, each in its own transaction, one insert each. ``conn`` must be autocommit."""
    notes: list[str] = []
    for months in LOOK_MONTHS:
        with conn.transaction():
            look._lock(conn, decl.declaration_id)
            t0 = look.first_target_session(conn, decl.declaration_id)
            if t0 is None or months in look._stored_looks(conn, decl.declaration_id):
                continue
            end = look.endpoint(t0, months)
            row = conn.execute(
                "SELECT max(session), min(session) FILTER (WHERE wind_down_event_id IS NOT NULL) "
                "FROM ranking_pot_steps WHERE declaration_id = %s",
                (decl.declaration_id,),
            ).fetchone()
            last, wound = (None, None) if row is None else (row[0], row[1])
            # A wind-down applied before E liquidated the books inside the window: no look (v1 spec §9.3).
            if last is None or last < end or (wound is not None and wound < end):
                continue
            if not policy_ok(decl):
                # The step that follows records its own `policy_drift` refusal and fails the run.
                notes.append(f"look {months}m not computed: policy drift")
                return notes
            result = compute_look(conn, decl, months=months, t0=t0)
            conn.execute(
                "INSERT INTO ranking_pot_looks (declaration_id, look_months, endpoint_session, kind, verdict, harm, "
                "reasons, detail, policy_hash, computed_at) VALUES (%s, %s, %s, 'result', %s, %s, %s, %s, %s, %s)",
                (
                    decl.declaration_id,
                    months,
                    end,
                    result.verdict,
                    result.harm,
                    list(result.reasons),
                    Jsonb(result.detail),
                    RANKING_POT_V2_POLICY_HASH,
                    as_of,
                ),
            )
            v2v = result.detail["v2"]["v2_verdict"]
            notes.append(f"look {months}m at {end}: {v2v} (v1 {result.verdict}){', harm' if result.harm else ''}")
    return notes


def run_looks(conn: Conn, decl: rb.PotDeclaration, as_of: datetime, notes: list[str]) -> None:
    """v1's step-job ``_looks`` without the executed book's stamps: reconcile the state, compute every due look,
    reconcile again (a harm or last look's ``winding_down`` is written in this fire)."""
    for note in (look.reconcile_state(conn, decl.declaration_id), *compute_due_looks(conn, decl, as_of=as_of)):
        if note is not None:
            notes.append(note)
            logger.info("ranking pot v2 look: %s", note)
    if (note := look.reconcile_state(conn, decl.declaration_id)) is not None:
        notes.append(note)
        logger.info("ranking pot v2 look: %s", note)


__all__ = [
    "NO_EXECUTION",
    "V2_REASONS",
    "V2_VERDICTS",
    "TurnoverFacts",
    "V2Detail",
    "V2StepRow",
    "compute_due_looks",
    "compute_look",
    "evaluate",
    "run_looks",
    "turnover",
    "v2_detail_of",
]
