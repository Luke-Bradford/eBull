"""Which approved evidence IS the monitoring baseline for a deployed strategy version.

#2500 asks for *"an immutable monitoring envelope derived only from the approved recent
evidence"*, pinned at promotion.  The envelope itself already exists and is already
immutable: ``strategy_promotion_evidence`` (#2505, ``sql/327``) stores one <=64 KiB
aggregate record per ``result_id`` -- expected shortfall, drawdown, worst gap,
concentration, calibration, EV buckets, target/stop/timeout counts, capacity and the
whole cost vector -- with ``payload_sha256`` beside it and a ``BEFORE UPDATE OR DELETE``
trigger refusing every mutation.  ``promote_strategy`` pins the ``result_id``s into
``strategy_promotion_results``.  Nothing needs copying.

⚠⚠ WHAT IS MISSING IS THE POINTER, NOT THE RECORD.  ``enable_paper`` pins **no** result
ids, deliberately -- ``strategy_operator_promotion._assemble_evidence`` says so in place:
*"No result ids: `paper_enabled` is not a `_RESULT_EVIDENCE_STAGES` member.  Re-pinning
the historical matrix here would double-count one denominator as two independent pieces
of evidence."*  So the promotion that actually DEPLOYS a strategy carries no envelope,
and until this module there was no rule saying which earlier promotion's evidence the
deployed version is to be monitored against.

This module answers exactly that and stops.  It chooses no threshold, no window and no
ratio, so it is not a data-treatment decision; the comparison that will read it is, and
that comparison needs a declared repeated-look method before it can be written.

See ``docs/proposals/ta/2026-09-16-2500-promotion-monitoring-baseline.md``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

import psycopg

from app.services.strategy_control_plane import Stage
from app.services.strategy_promotion_evidence import PromotionEvidence
from app.services.strategy_promotion_evidence_store import load_promotion_evidences

#: Bumped whenever the SELECTION rule below changes.  A baseline resolved under a
#: different rule version is a different claim about which evidence was approved, so a
#: monitor that stored a verdict must re-derive rather than compare across versions.
BASELINE_RULE_VERSION: Final = "monitoring-baseline-v1"

#: The evidence stages a baseline may be drawn from, MOST PREFERRED FIRST.  Mirrors
#: ``strategy_control_plane._RESULT_EVIDENCE_STAGES`` -- those are the only two stages
#: ``promote_strategy`` lets pin a ``result_id`` at all.
#:
#: ⚠ The ORDER is a source rule, not a preference.  ``forward_observation`` is pinned
#: under ``weakening_refusals(previously_covered=load_pinned_identities(...,
#: to_stage="historical_validated"), now_covered=...)``, so the transition refuses when
#: the forward evidence covers LESS than the historical stage already did.  The forward
#: pin therefore cannot be the weaker statement of the two.
_BASELINE_STAGE_PREFERENCE: Final[tuple[Stage, ...]] = ("forward_observation", "historical_validated")

#: What #2505's record does NOT carry, and where each would have to come from.  Written
#: down here rather than rediscovered: this is the build list for the monitor itself.
#:
#: * ``firing_interarrival_range`` -- ``strategy_signals`` / ``strategy_decision_calendar``.
#:   Forward observation, not a backtest quantity.
#: * ``broker_fill_rejection_range`` -- ``strategy_order_reconciliation_state`` and
#:   ``strategy_trade_orders``.  Also forward-only.
#: * ``resolved_outcome_maturity_range`` -- resolution lag on ``strategy_outcomes``.  The
#:   backtest analogue ``strategy_results_store.median_hold_days`` is NULL on 324 of 580
#:   rows (55.9%, measured 2026-09-16 on the full store), so it is not a substitute.
#: * ``checkpoint_plan_and_error_budget`` -- stored nowhere.  Needs a declared
#:   alpha-spending or anytime-valid rule first; #2500 forbids reusing an ordinary 95%
#:   interval across repeated looks.
MISSING_ENVELOPE_COMPONENTS: Final = (
    "firing_interarrival_range",
    "broker_fill_rejection_range",
    "resolved_outcome_maturity_range",
    "checkpoint_plan_and_error_budget",
)

#: Closed vocabulary.  Every reason a version has no baseline, in the order the clauses
#: are evaluated -- a caller reading the first entry gets the most specific cause.
NO_PROMOTION_FOR_VERSION: Final = "no_promotion_for_version"
NO_EVIDENCE_STAGE_PROMOTION: Final = "no_evidence_stage_promotion"
NO_PINNED_RESULTS: Final = "no_pinned_results"
PROMOTION_EVIDENCE_MISSING: Final = "promotion_evidence_missing"
DUPLICATE_EVIDENCE_STAGE_PROMOTION: Final = "duplicate_evidence_stage_promotion"


@dataclass(frozen=True)
class BaselinePromotionCandidate:
    """One promotion row of the version under consideration, evidence-bearing or not."""

    to_stage: str
    promotion_id: int
    promoted_at: datetime
    result_ids: tuple[int, ...]


@dataclass(frozen=True)
class BaselineChoice:
    """The chosen promotion, or the refusals that left there being none."""

    candidate: BaselinePromotionCandidate | None
    refusals: tuple[str, ...]


@dataclass(frozen=True)
class MonitoringBaseline:
    """The approved envelope a deployed version is to be monitored against."""

    strategy_id: str
    strategy_version: str
    baseline_rule_version: str
    baseline_stage: str
    promotion_id: int
    promoted_at: datetime
    result_ids: tuple[int, ...]
    evidence_payload_sha256: tuple[str, ...]
    evidence: tuple[PromotionEvidence, ...]
    absent_components: tuple[str, ...]


def select_baseline_promotion(candidates: Sequence[BaselinePromotionCandidate]) -> BaselineChoice:
    """Pure: which of a version's promotions supplies its monitoring baseline.

    ⚠ An evidence-stage promotion carrying zero pinned results REFUSES rather than
    falling back to the earlier stage.  ``promote_strategy`` already refuses to create
    one (*"forward_observation requires at least one pinned result_id"*), so reaching
    this clause means the row is not what the transition rule says it is -- and falling
    back would monitor against the statement the empty one was allowed to supersede.

    ⚠ TWO ROWS AT ONE EVIDENCE STAGE REFUSE TOO, rather than one of them being picked.
    Settled decision #2612 makes stage arrival single-entry and
    ``idx_strategy_promotions_one_successor`` enforces it, so this cannot happen today --
    but a ``dict`` keyed on ``to_stage`` would have SILENTLY kept whichever row iterated
    last, which is to say the oldest under the caller's ``promotion_id DESC``.  The
    module's whole job is naming which evidence was approved; resolving a contradiction
    about that by iteration order is the one answer it must not give.
    """
    if not candidates:
        return BaselineChoice(None, (NO_PROMOTION_FOR_VERSION,))
    for stage in _BASELINE_STAGE_PREFERENCE:
        at_stage = [candidate for candidate in candidates if candidate.to_stage == stage]
        if not at_stage:
            continue
        if len(at_stage) > 1:
            return BaselineChoice(None, (DUPLICATE_EVIDENCE_STAGE_PROMOTION,))
        candidate = at_stage[0]
        if not candidate.result_ids:
            return BaselineChoice(None, (NO_PINNED_RESULTS,))
        return BaselineChoice(candidate, ())
    return BaselineChoice(None, (NO_EVIDENCE_STAGE_PROMOTION,))


_CANDIDATES_SQL: Final = """
    SELECT p.to_stage,
           p.promotion_id,
           p.promoted_at,
           coalesce(array_agg(pr.result_id ORDER BY pr.result_id)
                    FILTER (WHERE pr.result_id IS NOT NULL), '{}') AS result_ids
    FROM strategy_promotions p
    LEFT JOIN strategy_promotion_results pr ON pr.promotion_id = p.promotion_id
    WHERE p.strategy_id = %s AND p.strategy_version = %s
    GROUP BY p.to_stage, p.promotion_id, p.promoted_at
    ORDER BY p.promotion_id DESC
"""

_EVIDENCE_DIGEST_SQL: Final = """
    SELECT result_id, payload_sha256
    FROM strategy_promotion_evidence
    WHERE result_id = ANY(%(result_ids)s::bigint[])
"""


def load_baseline_candidates(
    conn: psycopg.Connection[Any], *, strategy_id: str, strategy_version: str
) -> tuple[BaselinePromotionCandidate, ...]:
    """Every promotion recorded for one version, newest first."""
    rows = conn.execute(_CANDIDATES_SQL, (strategy_id, strategy_version)).fetchall()
    return tuple(
        BaselinePromotionCandidate(
            to_stage=str(row[0]),
            promotion_id=int(row[1]),
            promoted_at=row[2],
            result_ids=tuple(int(value) for value in row[3]),
        )
        for row in rows
    )


def resolve_monitoring_baseline(
    conn: psycopg.Connection[Any], *, strategy_id: str, strategy_version: str
) -> MonitoringBaseline | None:
    """The approved envelope for one version, or ``None`` with no baseline.

    ⚠⚠ FAIL-CLOSED, AND NOT BY SWALLOWING EXCEPTIONS.  ``None`` means a refusal clause
    fired on data that was read successfully.  A database error PROPAGATES: catching it
    would report "no baseline" while leaving the caller's transaction aborted, and would
    collapse *nothing was approved* and *the database is down* into one state that a
    monitor cannot tell apart.  Callers treat ``None`` as "cannot report healthy".
    """
    choice = select_baseline_promotion(
        load_baseline_candidates(conn, strategy_id=strategy_id, strategy_version=strategy_version)
    )
    candidate = choice.candidate
    if candidate is None:
        return None
    evidences = load_promotion_evidences(conn, candidate.result_ids)
    # A partial mapping is the absent case, per ``load_promotion_evidences``' own
    # contract -- one pinned result without its immutable record means the envelope is
    # incomplete, and an incomplete envelope is not a baseline.
    if len(evidences) != len(candidate.result_ids):
        return None
    digests = dict(conn.execute(_EVIDENCE_DIGEST_SQL, {"result_ids": list(candidate.result_ids)}).fetchall())
    return MonitoringBaseline(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        baseline_rule_version=BASELINE_RULE_VERSION,
        baseline_stage=candidate.to_stage,
        promotion_id=candidate.promotion_id,
        promoted_at=candidate.promoted_at,
        result_ids=candidate.result_ids,
        evidence_payload_sha256=tuple(str(digests[result_id]) for result_id in candidate.result_ids),
        evidence=tuple(evidences[result_id] for result_id in candidate.result_ids),
        absent_components=MISSING_ENVELOPE_COMPONENTS,
    )


def baseline_unavailable_reasons(
    conn: psycopg.Connection[Any], *, strategy_id: str, strategy_version: str
) -> tuple[str, ...]:
    """Why one version has no baseline; empty when it has one.

    Separate from ``resolve_monitoring_baseline`` so the refusal can be surfaced without
    parsing the envelope, and so both read the SAME clauses -- the reasons are derived
    here, never restated.
    """
    candidates = load_baseline_candidates(conn, strategy_id=strategy_id, strategy_version=strategy_version)
    choice = select_baseline_promotion(candidates)
    if choice.candidate is None:
        return choice.refusals
    evidences = load_promotion_evidences(conn, choice.candidate.result_ids)
    if len(evidences) != len(choice.candidate.result_ids):
        return (PROMOTION_EVIDENCE_MISSING,)
    return ()


__all__ = [
    "BASELINE_RULE_VERSION",
    "DUPLICATE_EVIDENCE_STAGE_PROMOTION",
    "MISSING_ENVELOPE_COMPONENTS",
    "NO_EVIDENCE_STAGE_PROMOTION",
    "NO_PINNED_RESULTS",
    "NO_PROMOTION_FOR_VERSION",
    "PROMOTION_EVIDENCE_MISSING",
    "BaselineChoice",
    "BaselinePromotionCandidate",
    "MonitoringBaseline",
    "baseline_unavailable_reasons",
    "load_baseline_candidates",
    "resolve_monitoring_baseline",
    "select_baseline_promotion",
]
