"""The ordered operator path from a registered candidate to ``paper_enabled`` (#2770).

``promote_strategy`` is a strict primitive, but it takes caller-supplied ``result_ids``
and validates each one INDIVIDUALLY. A caller could therefore pin the three windows
where a strategy happened to look good, and every pinned row would pass its own checks
while the promotion rested on a cherry-picked denominator. Nothing downstream could
tell — the promotion row would look identical to an honest one.

So this module owns the denominator instead of the caller. The operator names an
ACTION; the evidence is assembled here, inside the same transaction and under the same
per-version advisory lock the primitive takes, from declarations that already exist:

- the six windows of ``strategy_recent_evidence.RECENT_EVIDENCE_WINDOWS``;
- the four ambiguity x quarantine arms;
- ``strategy_result_identity.current_identity_pins()``, which is what makes the 24 rows
  COMPARABLE rather than merely present.

The third is the one that is easy to miss. Twenty-four rows with no gaps can still mix
results measured against different corpora, cost models or rule sets — label-complete
and meaningless. Binding the whole pin dict is the cross-row coherence check; there is
deliberately no second one.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Literal, cast

import psycopg

from app.services.strategy_control_plane import (
    _NEXT_STAGE,
    Promotion,
    Stage,
    StrategyControlError,
    current_stage,
    lock_strategy_control,
    promote_strategy,
    registered_strategy_purpose,
)
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS
from app.services.strategy_result_identity import current_identity_pins, current_result_versions

OperatorAction = Literal[
    "register_research_candidate",
    "validate_historical",
    "start_forward_observation",
    "enable_paper",
]

#: Successors of a stage that this path deliberately does NOT drive. ``paused`` and
#: ``retired`` belong to ``POST /strategies/{id}/lifecycle``; ``live_enabled`` belongs
#: to the dedicated measured live gate and ``promote_strategy`` refuses it outright.
_NON_OPERATOR_STAGES: Final[frozenset[Stage]] = frozenset({"paused", "retired", "live_enabled"})

_ACTION_FOR_TARGET: Final[dict[Stage, OperatorAction]] = {
    "research_candidate": "register_research_candidate",
    "historical_validated": "validate_historical",
    "forward_observation": "start_forward_observation",
    "paper_enabled": "enable_paper",
}
_TARGET_FOR_ACTION: Final[dict[OperatorAction, Stage]] = {action: stage for stage, action in _ACTION_FOR_TARGET.items()}

#: Only these three carry evidence. ``research_candidate`` is not in
#: ``_EXTERNAL_EVIDENCE_STAGES``, so the primitive lets a non-capital-candidate
#: register — refusing it here on purpose would silently narrow the declared DAG.
_EVIDENCE_ACTIONS: Final[frozenset[OperatorAction]] = frozenset(
    {"validate_historical", "start_forward_observation", "enable_paper"}
)

_AMBIGUITY_ARMS: Final = ("best_case", "worst_case")
_QUARANTINE_ARMS: Final = ("admitted", "masked")
EXPECTED_ARMS: Final[tuple[tuple[str, str], ...]] = tuple(
    (ambiguity, quarantine) for ambiguity in _AMBIGUITY_ARMS for quarantine in _QUARANTINE_ARMS
)
#: 6 windows x 4 arms. Named so a test can assert the number rather than trust prose.
EXPECTED_EVIDENCE_COMBINATIONS: Final = len(RECENT_EVIDENCE_WINDOWS) * len(EXPECTED_ARMS)

RECENT_EVIDENCE_REF_PREFIX: Final = "recent-evidence-v1"
PROSPECTIVE_ASSESSMENT_REF_PREFIX: Final = "prospective-assessment-v1"

#: The overview's own future-clock tolerance (`app/api/strategies.py`), inherited
#: deliberately. One predicate, one behaviour — otherwise the page can read "fresh"
#: where this transaction reads "stale", which is the worst kind of disagreement
#: because the operator sees an enabled control that 409s.
ASSESSMENT_FUTURE_TOLERANCE: Final = timedelta(seconds=5)


def operator_targets(stage: Stage | None) -> frozenset[Stage]:
    """The forward edges of ``_NEXT_STAGE`` this path drives, for one stage."""
    return frozenset(_NEXT_STAGE[stage]) - _NON_OPERATOR_STAGES


def allowed_operator_action(stage: Stage | None) -> OperatorAction | None:
    """The one action available from ``stage``, or ``None`` at a terminal stage.

    Derived from ``_NEXT_STAGE`` rather than hand-written beside it, so a stage-graph
    edit cannot leave this path describing a graph that no longer exists. It raises
    rather than guessing if the graph ever offers two operator successors, because
    "which one did the operator mean" is a design question and not a runtime one.
    """
    targets = operator_targets(stage)
    if not targets:
        return None
    if len(targets) != 1:
        raise StrategyControlError(f"stage {stage!r} has {len(targets)} operator successors; expected 1")
    return _ACTION_FOR_TARGET[next(iter(targets))]


def action_target(action: OperatorAction) -> Stage:
    return _TARGET_FOR_ACTION[action]


@dataclass(frozen=True)
class EvidenceRow:
    """One selected hold-out result, reduced to what the matrix rule reads."""

    window_id: str
    ambiguity_arm: str
    quarantine_arm: str
    result_id: int


def recent_evidence_refusals(rows: Sequence[EvidenceRow]) -> tuple[str, ...]:
    """Refusal codes for the 24-combination matrix; empty means complete.

    There is no "close enough" tier. A partial matrix is a refusal, because the whole
    point is that the denominator is not the caller's to choose — and "five of six
    windows" is a chosen denominator whoever chose it.
    """
    by_window: dict[str, list[EvidenceRow]] = defaultdict(list)
    for row in rows:
        by_window[row.window_id].append(row)

    refusals: set[str] = set()

    for window_id in sorted(set(by_window) - set(RECENT_EVIDENCE_WINDOWS)):
        refusals.add(f"recent_evidence_window_unknown:{window_id}")

    for window_id in RECENT_EVIDENCE_WINDOWS:
        window_rows = by_window.get(window_id, [])
        if not window_rows:
            # ⚠ ALONE, not with four `arm_missing` companions. A wholly absent window
            # is one fact about one window; spelling it four more ways makes the
            # refusal list scale with the damage rather than describe it.
            refusals.add(f"recent_evidence_window_missing:{window_id}")
            continue
        seen: dict[tuple[str, str], int] = defaultdict(int)
        for row in window_rows:
            seen[(row.ambiguity_arm, row.quarantine_arm)] += 1
        for arm in EXPECTED_ARMS:
            if seen.get(arm, 0) == 0:
                refusals.add(f"recent_evidence_arm_missing:{window_id}/{arm[0]}/{arm[1]}")
        for arm, count in seen.items():
            if arm not in EXPECTED_ARMS:
                refusals.add(f"recent_evidence_arm_unknown:{window_id}/{arm[0]}/{arm[1]}")
            elif count > 1:
                # Unreachable through `load_authoritative_recent_evidence`, which runs
                # `select_latest_rows` first. Kept because duplicates on this identity
                # are REAL in the store (measured 2026-08-21: 328 rows over 268
                # distinct identities), so this asserts the resolution actually
                # happened rather than assuming it did.
                refusals.add(f"recent_evidence_arm_duplicate:{window_id}/{arm[0]}/{arm[1]}")

    return tuple(sorted(refusals))


def recent_evidence_ref(*, strategy_id: str, strategy_version: str, rows: Sequence[EvidenceRow]) -> str:
    """A deterministic reference to the exact evidence set pinned.

    ⚠ ``GOVERNANCE_GATE_VERSION`` is deliberately NOT in the payload. This names the
    evidence SET; the gate version is recorded in its own column on the promotion row.
    Mixing them would give the same evidence two references across a gate bump and
    make "did the denominator change?" unanswerable from the reference alone.

    Canonical JSON (sorted keys, no whitespace) rather than string concatenation, so
    two different tuples cannot serialise to one byte string.
    """
    payload = json.dumps(
        {
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "evidence": sorted([row.window_id, row.ambiguity_arm, row.quarantine_arm, row.result_id] for row in rows),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"{RECENT_EVIDENCE_REF_PREFIX}+{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"


EvidenceIdentity = tuple[str, str, str]


def evidence_identities(rows: Iterable[EvidenceRow]) -> frozenset[EvidenceIdentity]:
    return frozenset((row.window_id, row.ambiguity_arm, row.quarantine_arm) for row in rows)


def weakening_refusals(
    *, previously_covered: Iterable[EvidenceIdentity], now_covered: Iterable[EvidenceIdentity]
) -> tuple[str, ...]:
    """Forward observation may add evidence; it may not stop covering any.

    ⚠⚠ COMPARES IDENTITIES, NOT RESULT IDS, and the distinction is the whole rule
    (Codex ckpt-2). An id comparison is wrong in the ordinary case: a re-run between
    the two promotions replaces every id, ``select_latest_rows`` returns the new ones,
    and every previously pinned id reads as "dropped". Because the store is
    append-only and pins are ``ON DELETE RESTRICT``, that verdict would be permanent —
    one routine `refresh_recent` re-run would block `start_forward_observation`
    forever. Superseding a row is not weakening the evidence; ceasing to cover a
    window or an arm is.

    ⚠ It cannot fire while both promotions demand a complete matrix over the SAME
    declared windows, because two complete matrices have identical identity sets. What
    it catches is the declared set MOVING between the two steps: shrink
    ``RECENT_EVIDENCE_WINDOWS`` and the second "complete" matrix is smaller than the
    first, which is a real weakening and is invisible to every completeness check.
    """
    dropped = sorted(set(previously_covered) - set(now_covered))
    if not dropped:
        return ()
    return tuple(
        f"recent_evidence_weakened:{window}/{ambiguity}/{quarantine}" for window, ambiguity, quarantine in dropped
    )


@dataclass(frozen=True)
class AssessmentCandidate:
    assessment_id: int
    policy_id: str
    checked_at: datetime
    passed: bool
    max_assessment_age_days: int


def select_prospective_assessment(
    *,
    policy_present: bool,
    candidates: Sequence[AssessmentCandidate],
    as_of: datetime,
    forward_started_at: datetime | None,
) -> tuple[AssessmentCandidate | None, tuple[str, ...]]:
    """The assessment that authorises paper, or the single reason there isn't one.

    One refusal, not a list: these are ordered stages of the same question, and
    reporting "missing" alongside "stale" would be incoherent.
    """
    if not policy_present:
        return None, ("prospective_assessment_policy_missing",)
    if not candidates:
        return None, ("prospective_assessment_missing",)
    passed = [item for item in candidates if item.passed]
    if not passed:
        return None, ("prospective_assessment_not_passed",)
    fresh = [
        item
        for item in passed
        if item.checked_at >= as_of - timedelta(days=item.max_assessment_age_days)
        and item.checked_at <= as_of + ASSESSMENT_FUTURE_TOLERANCE
    ]
    if not fresh:
        return None, ("prospective_assessment_stale",)
    if forward_started_at is not None:
        # An assessment computed before forward observation began is not evidence
        # FROM forward observation. Without this the whole chain can be walked
        # back-to-back on backtest evidence alone, which would make the
        # forward_observation stage decorative.
        fresh = [item for item in fresh if item.checked_at >= forward_started_at]
        if not fresh:
            return None, ("prospective_assessment_predates_forward_observation",)
    chosen = max(fresh, key=lambda item: (item.checked_at, item.assessment_id))
    return chosen, ()


def prospective_assessment_ref(candidate: AssessmentCandidate) -> str:
    return f"{PROSPECTIVE_ASSESSMENT_REF_PREFIX}+{candidate.assessment_id}@{candidate.policy_id}"


def select_latest_rows(rows: Iterable[EvidenceRow]) -> tuple[EvidenceRow, ...]:
    """One row per ``(window, ambiguity, quarantine)`` — the highest ``result_id``.

    ⚠ A re-run ADDS a row rather than replacing one: ``strategy_results_store`` is
    unique on ``(strategy_id, strategy_version, result_version)``, not on the
    window/arm identity, and pinned results are ``ON DELETE RESTRICT``. Measured on
    dev 2026-08-21: 328 rows over 268 distinct identities, ten identities carrying
    two rows apiece from one s1 re-run. So "refuse on duplicate" would make any
    re-run strategy permanently unpromotable, and some resolution rule is forced.

    Latest-wins is the safe direction: a re-run that measured WORSE supersedes a
    better old row, never the other way round, and no caller can influence the
    choice. ``result_id`` is the primary key, so the order is total.

    ⚠ ONE implementation, deliberately not a SQL ``DISTINCT ON`` plus a Python copy.
    The read surface and the promotion transaction must agree about which row is
    current; two implementations of "latest" is how they stop agreeing.
    """
    latest: dict[tuple[str, str, str], EvidenceRow] = {}
    for row in rows:
        key = (row.window_id, row.ambiguity_arm, row.quarantine_arm)
        current = latest.get(key)
        if current is None or row.result_id > current.result_id:
            latest[key] = row
    return tuple(sorted(latest.values(), key=lambda row: (row.window_id, row.ambiguity_arm, row.quarantine_arm)))


_AUTHORITATIVE_EVIDENCE_SQL = """
    SELECT r.evidence_window_id, r.ambiguity_arm, r.quarantine_arm, r.result_id
    FROM strategy_results_store r
    WHERE r.strategy_id = %(strategy_id)s
      AND r.strategy_version = %(strategy_version)s
      AND r.evidence_window_id IS NOT NULL
      AND r.namespace = %(namespace)s
      AND r.corpus_version = %(corpus_version)s
      AND r.cost_model_id = %(cost_model_id)s
      AND r.sizing_rule = %(sizing_rule)s
      AND r.benchmark_rule = %(benchmark_rule)s
      AND r.return_basis = %(return_basis)s
      AND r.ambiguity_rule_version = %(ambiguity_rule_version)s
      AND r.position_rule_set_version = %(position_rule_set_version)s
      AND r.outcome_rule_set_version = %(outcome_rule_set_version)s
      AND r.input_rule_set_version = %(input_rule_set_version)s
    ORDER BY r.evidence_window_id, r.ambiguity_arm, r.quarantine_arm, r.result_id
"""


@dataclass(frozen=True)
class RecentEvidenceBundle:
    rows: tuple[EvidenceRow, ...]
    result_ids: tuple[int, ...]
    evidence_ref: str
    refusals: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.refusals


def load_authoritative_recent_evidence(
    conn: psycopg.Connection[Any], *, strategy_id: str, strategy_version: str
) -> RecentEvidenceBundle:
    """The pinned hold-out matrix, assembled from the store and never from a caller.

    ⚠ NOT filtered to the declared window ids in SQL. Filtering there would make
    ``recent_evidence_window_unknown`` undetectable — a row on this comparability
    basis naming an undeclared window is exactly the thing worth refusing, and a
    whitelist in the WHERE clause hides it instead. Classification happens in
    ``recent_evidence_refusals``.

    Duplicate identities are resolved by ``select_latest_rows``, in Python rather than
    in the SQL, so that the read surface can apply the same function to the rows the
    overview already holds instead of maintaining a second notion of "latest".
    """
    params: dict[str, object] = {
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        **current_identity_pins(),
    }
    fetched = conn.execute(_AUTHORITATIVE_EVIDENCE_SQL, params).fetchall()
    rows = select_latest_rows(
        EvidenceRow(
            window_id=str(row[0]),
            ambiguity_arm=str(row[1]),
            quarantine_arm=str(row[2]),
            result_id=int(row[3]),
        )
        for row in fetched
    )
    return RecentEvidenceBundle(
        rows=rows,
        result_ids=tuple(sorted(row.result_id for row in rows)),
        evidence_ref=recent_evidence_ref(strategy_id=strategy_id, strategy_version=strategy_version, rows=rows),
        refusals=recent_evidence_refusals(rows),
    )


def load_pinned_identities(
    conn: psycopg.Connection[Any], *, strategy_id: str, strategy_version: str, to_stage: Stage
) -> frozenset[EvidenceIdentity]:
    """Which window/arm combinations a past promotion actually pinned.

    ⚠ Resolves the pinned RESULT IDS back to their identities rather than returning
    the ids. The ids are superseded by any re-run; the identities are what the
    promotion was a statement about.
    """
    rows = conn.execute(
        """
        SELECT r.evidence_window_id, r.ambiguity_arm, r.quarantine_arm
        FROM strategy_promotions p
        JOIN strategy_promotion_results pr ON pr.promotion_id = p.promotion_id
        JOIN strategy_results_store r ON r.result_id = pr.result_id
        WHERE p.strategy_id = %s AND p.strategy_version = %s AND p.to_stage = %s
          AND r.evidence_window_id IS NOT NULL
        """,
        (strategy_id, strategy_version, to_stage),
    ).fetchall()
    return frozenset((str(row[0]), str(row[1]), str(row[2])) for row in rows)


def load_stage_entered_at(
    conn: psycopg.Connection[Any], *, strategy_id: str, strategy_version: str, to_stage: Stage
) -> datetime | None:
    """When this version arrived at ``to_stage``.

    ``max`` is defensive rather than meaningful: settled decision #2612 makes arrival
    single-entry, enforced by ``_NEXT_STAGE`` and by
    ``idx_strategy_promotions_one_successor``. If that ever stops holding, taking the
    latest arrival is the reading that keeps the freshness bound tightest.
    """
    row = conn.execute(
        """
        SELECT max(promoted_at) FROM strategy_promotions
        WHERE strategy_id = %s AND strategy_version = %s AND to_stage = %s
        """,
        (strategy_id, strategy_version, to_stage),
    ).fetchone()
    return None if row is None or row[0] is None else row[0]


_CURRENT_ASSESSMENT_SQL = """
    SELECT c.assessment_id, p.policy_id, c.checked_at, a.passed, p.max_assessment_age_days
    FROM strategy_forecast_assessment_policies p
    JOIN strategy_forecast_assessment_current c ON c.policy_id = p.policy_id
    JOIN strategy_forecast_assessments a
      ON a.assessment_id = c.assessment_id
     AND a.policy_id = c.policy_id
     AND a.strategy_id = c.strategy_id
     AND a.strategy_version = c.strategy_version
    WHERE p.policy_id = (
        SELECT policy_id FROM strategy_forecast_assessment_policies
        WHERE effective_from <= %(as_of)s
        ORDER BY effective_from DESC LIMIT 1
    )
      AND c.strategy_id = %(strategy_id)s
      AND c.strategy_version = %(strategy_version)s
"""


def load_assessment_candidates(
    conn: psycopg.Connection[Any], *, strategy_id: str, strategy_version: str, as_of: datetime
) -> tuple[bool, tuple[AssessmentCandidate, ...]]:
    """``(an effective policy exists, its assessments for this version)``.

    The two are separate because "no policy has ever been registered" and "a policy
    exists but this version has no assessment under it" are different operator
    problems with different fixes, and collapsing them into one empty list would
    report the second when the first is true.
    """
    policy = conn.execute(
        """
        SELECT policy_id FROM strategy_forecast_assessment_policies
        WHERE effective_from <= %s ORDER BY effective_from DESC LIMIT 1
        """,
        (as_of,),
    ).fetchone()
    if policy is None:
        return False, ()
    rows = conn.execute(
        _CURRENT_ASSESSMENT_SQL,
        {"as_of": as_of, "strategy_id": strategy_id, "strategy_version": strategy_version},
    ).fetchall()
    return True, tuple(
        AssessmentCandidate(
            assessment_id=int(row[0]),
            policy_id=str(row[1]),
            checked_at=row[2],
            passed=bool(row[3]),
            max_assessment_age_days=int(row[4]),
        )
        for row in rows
    )


@dataclass(frozen=True)
class AdvanceOutcome:
    strategy_id: str
    strategy_version: str
    from_stage: Stage | None
    stage: Stage
    promotion: Promotion
    evidence_ref: str | None
    pinned_result_count: int


def _assemble_evidence(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    action: OperatorAction,
    as_of: datetime,
) -> tuple[str | None, tuple[int, ...]]:
    if action == "register_research_candidate":
        return None, ()

    if action == "enable_paper":
        policy_present, candidates = load_assessment_candidates(
            conn, strategy_id=strategy_id, strategy_version=strategy_version, as_of=as_of
        )
        chosen, refusals = select_prospective_assessment(
            policy_present=policy_present,
            candidates=candidates,
            as_of=as_of,
            forward_started_at=load_stage_entered_at(
                conn,
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                to_stage="forward_observation",
            ),
        )
        if chosen is None:
            raise StrategyControlError("; ".join(refusals))
        # ⚠ No result ids: `paper_enabled` is not a `_RESULT_EVIDENCE_STAGES` member.
        # Re-pinning the historical matrix here would double-count one denominator as
        # two independent pieces of evidence.
        return prospective_assessment_ref(chosen), ()

    bundle = load_authoritative_recent_evidence(conn, strategy_id=strategy_id, strategy_version=strategy_version)
    refusals = bundle.refusals
    if action == "start_forward_observation":
        refusals = refusals + weakening_refusals(
            previously_covered=load_pinned_identities(
                conn,
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                to_stage="historical_validated",
            ),
            now_covered=evidence_identities(bundle.rows),
        )
    if refusals:
        raise StrategyControlError("; ".join(refusals))
    return bundle.evidence_ref, bundle.result_ids


def advance_strategy(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    action: OperatorAction,
    advanced_by: str,
    reason: str,
    as_of: datetime,
) -> AdvanceOutcome:
    """Advance one strategy by one declared step, on evidence assembled here.

    The caller supplies an action and a reason. It does not supply a stage, a version,
    or a result id — those are the three inputs that would let a browser choose its own
    denominator, and none of them crosses this boundary.
    """
    versions = current_result_versions()
    strategy_version = versions.get(strategy_id)
    if strategy_version is None:
        raise StrategyControlError(f"unknown strategy {strategy_id!r}")

    # Lock FIRST, then read the stage. The other order reads a stage that a concurrent
    # request may already have advanced, and decides against it.
    lock_strategy_control(conn, strategy_id, strategy_version)
    from_stage = current_stage(conn, strategy_id, strategy_version)
    expected = allowed_operator_action(from_stage)
    if expected != action:
        raise StrategyControlError(
            f"action {action!r} is not available from stage {from_stage!r}"
            + (f"; the available action is {expected!r}" if expected else "; this stage is terminal")
        )

    if action in _EVIDENCE_ACTIONS:
        purpose = registered_strategy_purpose(strategy_id)
        if purpose != "capital_candidate":
            raise StrategyControlError(
                f"{strategy_id} is registered as {purpose!r}; only a capital_candidate carries evidence stages"
            )

    evidence_ref, result_ids = _assemble_evidence(
        conn,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        action=action,
        as_of=as_of,
    )
    to_stage = action_target(action)
    promotion = promote_strategy(
        conn,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        to_stage=to_stage,
        promoted_by=advanced_by,
        reason=reason,
        evidence_ref=evidence_ref,
        result_ids=result_ids,
    )
    return AdvanceOutcome(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        from_stage=from_stage,
        stage=to_stage,
        promotion=promotion,
        evidence_ref=evidence_ref,
        pinned_result_count=len(result_ids),
    )


def next_operator_action_view(
    *,
    stage: str | None,
    purpose: str | None,
    evidence_refusals: Sequence[str],
    assessment_refusals: Sequence[str] = (),
) -> tuple[OperatorAction | None, tuple[str, ...]]:
    """What ``/strategies`` should show: the action, and why it would refuse.

    ⚠ ADVISORY, and the action is named ALONGSIDE its refusals rather than nulled by
    them — an operator who cannot act needs to know which step is blocked, not that
    there is no step. ``None`` means a terminal stage with no forward edge.

    ⚠ This cannot be transactionally coupled to a later ``advance_strategy`` call, so
    the transaction stays authoritative and a stale page gets a 409 it must render.

    ``stage`` is typed ``str`` and not ``Stage`` because it arrives from a DB column.
    A value the graph does not know offers no action — a read surface must not raise
    on data it cannot classify, and the write path re-reads the stage anyway.
    """
    if stage is not None and stage not in _NEXT_STAGE:
        return None, ()
    action = allowed_operator_action(cast("Stage | None", stage))
    if action is None:
        return None, ()
    refusals: list[str] = []
    if action in _EVIDENCE_ACTIONS and purpose != "capital_candidate":
        refusals.append("strategy_not_capital_candidate")
    if action in {"validate_historical", "start_forward_observation"}:
        refusals.extend(evidence_refusals)
    if action == "enable_paper":
        # ⚠ Codex ckpt-2: without this the button was ENABLED whenever a strategy
        # reached `forward_observation`, even where the overview had already
        # established that the assessment was missing, failed or stale — one click
        # for a guaranteed 409. The page knows; it should say so.
        #
        # ⚠ Necessarily a SUBSET of what the transaction checks: the overview does
        # not compute `prospective_assessment_predates_forward_observation`, which
        # needs the promotion timestamp. An enabled button is still a claim that
        # nothing KNOWN refuses, not a promise the request will succeed.
        refusals.extend(assessment_refusals)
    return action, tuple(refusals)


def evidence_refusal_summary(refusals: Sequence[str]) -> tuple[str, ...]:
    """Collapse the per-combination codes into something a card can render.

    Twenty-four `recent_evidence_arm_missing:` lines is an accurate list and an
    unreadable one. The code PREFIX is the class; the count is the size.
    """
    grouped: dict[str, list[str]] = defaultdict(list)
    for refusal in refusals:
        grouped[refusal.split(":", 1)[0]].append(refusal)
    # A class with ONE member keeps its detail: "window year-2023 is missing" is
    # actionable, "one window is missing" is not. Only a class large enough to be
    # unreadable is collapsed to its count.
    return tuple(
        members[0] if len(members) == 1 else f"{code} x{len(members)}" for code, members in sorted(grouped.items())
    )


__all__ = [
    "ASSESSMENT_FUTURE_TOLERANCE",
    "EXPECTED_ARMS",
    "EXPECTED_EVIDENCE_COMBINATIONS",
    "AdvanceOutcome",
    "AssessmentCandidate",
    "EvidenceRow",
    "OperatorAction",
    "RecentEvidenceBundle",
    "action_target",
    "advance_strategy",
    "allowed_operator_action",
    "EvidenceIdentity",
    "evidence_identities",
    "evidence_refusal_summary",
    "load_authoritative_recent_evidence",
    "load_pinned_identities",
    "load_stage_entered_at",
    "next_operator_action_view",
    "operator_targets",
    "prospective_assessment_ref",
    "recent_evidence_ref",
    "recent_evidence_refusals",
    "select_latest_rows",
    "select_prospective_assessment",
    "weakening_refusals",
]
