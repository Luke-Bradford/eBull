"""Frozen §3.4 ambiguity-comparison records for the promotion transition (#2625).

``check_promotable``'s ambiguity clause reads ``PromotionCandidate.ambiguity_material``,
a verdict ``backtest_run._write_rows`` computes in memory from the run's arm
measurements and never stores. The refusal it produced was returned in
``WrittenRow.refusals`` and died with the writer's return value, so
``promote_strategy`` could not re-detect a result whose two ambiguity arms
disagreed materially. This module persists the comparison's INPUTS, immutable
and hashed (the ``sql/334`` frozen-universe shape), and gives the transition a
pure verdict plus a pure refusal function over the frozen record.

⚠ THE INPUTS, NOT THE VERDICT. A stored boolean cannot be audited — nothing can
be checked against it. Storing the basis, the two arm Sharpes and the cohort
threshold lets the verdict be re-derived at the transition, and lets an auditor
disagree with it. Same argument as #2621 storing the two instrument-id sets
rather than a subset flag.

⚠ FROZEN AT RESULT TIME. The comparison is a property of measurements taken
during the run; there is no later observation that could re-derive it, so the
"replay against today" option #2505's cost-staleness clause exercises does not
arise here. The transition's full temporal policy — which inputs replay FROZEN,
which replay against TODAY, and which must not be re-read at all — is declared
in ``app.services.strategy_promotion_replay``.

⚠ WHAT THIS RECORD DOES NOT REPRODUCE. ``_ambiguity_material_for`` also RAISES
on malformed arm structure (measurements present that are not exactly the two
declared arms; a namespace missing from an arm) and silently collapses
duplicate same-arm measurements through a dict comprehension. Those are
properties of the in-memory arm collection, not of a three-scalar record. They
stay enforced at write time, where the arms exist, and the record makes no
claim about them.

Source rule: §3.4 fixes the materiality comparison (the arm gap versus the
strategy's margin above the random cohort's 95th-percentile Sharpe). The cases
§3.4 does not reach — a shared measurement, and an absent cohort — are fixed by
``app.services.backtest_run._ambiguity_material_for``, the settled current
implementation, and are cited from it rather than re-derived here.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final, Literal

import psycopg

from app.services.strategy_ambiguity_policy import (
    AMBIGUITY_RULE_VERSION,
    LEGACY_AMBIGUITY_RULE_VERSION,
    matched_control_margin,
)

#: How the run arrived at its comparison.
#:
#: ⚠ ``shared_measurement`` IS NOT INFERABLE FROM THE SHARPES, which is why it
#: is stored rather than derived. ``_ambiguity_material_for`` returns ``False``
#: from the mere PRESENCE of a matching measurement whose ``ambiguity_arm`` is
#: ``None`` — before it reads a Sharpe, and regardless of their values. Two
#: equal Sharpes are not equivalent evidence: they cannot distinguish one shared
#: measurement copied to both stored identities from two independently
#: evaluated arms that happened to tie.
ComparisonBasis = Literal["shared_measurement", "arm_sharpes"]

#: The version under which a record's verdict rule was computed.
#:
#: ⚠ FAIL CLOSED IS AN ALLOWLIST, exactly as ``RECOGNISED_UNIVERSE_RULE_VERSIONS``
#: is: when §3.4's comparison changes the constant bumps, old records refuse
#: ``ambiguity_rule_unrecognised``, and the successor decides explicitly whether
#: to re-admit them. Never a silent re-read under new semantics.
#:
RECOGNISED_AMBIGUITY_RULE_VERSIONS: Final[frozenset[str]] = frozenset({AMBIGUITY_RULE_VERSION})


@dataclass(frozen=True)
class AmbiguityRecord:
    """§3.4's comparison inputs for one result, as the run measured them."""

    ambiguity_rule_version: str
    comparison_basis: ComparisonBasis
    best_case_sharpe: float | None = None
    worst_case_sharpe: float | None = None
    #: §3.4's smaller positive arm margin above its matched random cohort's
    #: 95th-percentile Sharpe. ``None`` means the pair was not comparable.
    cohort_gap_threshold: float | None = None

    def __post_init__(self) -> None:
        if not self.ambiguity_rule_version:
            raise ValueError("ambiguity_rule_version must be non-empty")
        if self.comparison_basis not in ("shared_measurement", "arm_sharpes"):
            raise ValueError(f"unknown comparison_basis {self.comparison_basis!r}")
        for name in ("best_case_sharpe", "worst_case_sharpe", "cohort_gap_threshold"):
            value = getattr(self, name)
            # ⚠ Mirrors the table's CHECK rather than trusting it: this dataclass
            # is also built from in-memory measurements at write time, where no
            # constraint has run yet. A NaN Sharpe compares false against
            # everything and would silently read as "arms agree".
            if value is not None and not math.isfinite(value):
                raise ValueError(f"{name} must be finite, got {value!r}")
        if self.cohort_gap_threshold is not None and self.cohort_gap_threshold < 0:
            raise ValueError("cohort_gap_threshold is a gap and cannot be negative")
        # A shared measurement is decided by its basis alone, so carrying
        # numbers beside it would record inputs the verdict provably did not
        # consult — and would give one verdict two canonical forms, which is
        # what makes the payload hash meaningless.
        if self.comparison_basis == "shared_measurement" and any(
            value is not None for value in (self.best_case_sharpe, self.worst_case_sharpe, self.cohort_gap_threshold)
        ):
            raise ValueError("a shared_measurement record carries no arm Sharpes and no threshold")


def _canonical_payload(record: AmbiguityRecord) -> bytes:
    """One byte-stable encoding — JSON, not ad-hoc joins, so a rule version
    containing any delimiter cannot make two records hash alike."""
    return json.dumps(
        {
            "ambiguity_rule_version": record.ambiguity_rule_version,
            "comparison_basis": record.comparison_basis,
            "best_case_sharpe": record.best_case_sharpe,
            "worst_case_sharpe": record.worst_case_sharpe,
            "cohort_gap_threshold": record.cohort_gap_threshold,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def record_sha256(record: AmbiguityRecord) -> str:
    return hashlib.sha256(_canonical_payload(record)).hexdigest()


def ambiguity_verdict(record: AmbiguityRecord) -> bool | None:
    """§3.4's ``ambiguity_material`` verdict, re-derived from the frozen inputs.

    Pure. Reproduces ``backtest_run._ambiguity_material_for`` for every state
    that function can reach, and extends it to the one §3.4 declares but no
    current writer supplies (a present cohort threshold).

    ⚠ THE BASIS IS CHECKED FIRST, AND THE PRECEDENCE IS THE POINT.
    ``_ambiguity_material_for`` returns ``False`` on any matching shared
    measurement before it looks at a Sharpe, so a record whose basis is
    ``shared_measurement`` is ``False`` unconditionally.

    ⚠ ``None`` MEANS "NOT COMPARED", NOT "ABSENT". Absence is the caller's
    ``load_result_ambiguity`` returning ``None``, and the two produce different
    refusals — collapsing them would make an unrecorded row indistinguishable
    from a measured-but-unjudged one, the same collapse ``check_promotable``
    refuses between "not measured" and "measured and bad".
    """
    if record.comparison_basis == "shared_measurement":
        return False
    if record.best_case_sharpe is None or record.worst_case_sharpe is None:
        return None
    gap = abs(record.best_case_sharpe - record.worst_case_sharpe)
    # Equal Sharpes prove a zero gap, so materiality is decided without needing
    # the cohort — the one case `_ambiguity_material_for` can answer today.
    if gap == 0:
        return False
    if record.cohort_gap_threshold is None:
        return None
    return gap > record.cohort_gap_threshold


def store_result_ambiguity(conn: psycopg.Connection[Any], *, result_id: int, record: AmbiguityRecord) -> None:
    """Insert one frozen record. Runs in the CALLER's transaction — the writer
    stores it atomically with the result's arm pair."""
    conn.execute(
        """
        INSERT INTO strategy_result_ambiguity (
            result_id, ambiguity_rule_version, comparison_basis,
            best_case_sharpe, worst_case_sharpe, cohort_gap_threshold, payload_sha256
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            result_id,
            record.ambiguity_rule_version,
            record.comparison_basis,
            record.best_case_sharpe,
            record.worst_case_sharpe,
            record.cohort_gap_threshold,
            record_sha256(record),
        ),
    )


_SELECT_AMBIGUITY_COLUMNS = (
    "ambiguity_rule_version, comparison_basis, best_case_sharpe, "
    "worst_case_sharpe, cohort_gap_threshold, payload_sha256"
)


def _record_from_row(result_id: int, row: Any) -> AmbiguityRecord:
    """Verify and rebuild one row. Shared so the single and batch reads cannot
    verify differently."""
    basis = str(row[1])
    if basis not in ("shared_measurement", "arm_sharpes"):
        raise RuntimeError(f"result ambiguity record for result {result_id} has unknown basis {basis!r}")
    record = AmbiguityRecord(
        ambiguity_rule_version=str(row[0]),
        comparison_basis=basis,
        best_case_sharpe=None if row[2] is None else float(row[2]),
        worst_case_sharpe=None if row[3] is None else float(row[3]),
        cohort_gap_threshold=None if row[4] is None else float(row[4]),
    )
    if record_sha256(record) != str(row[5]):
        raise RuntimeError(f"result ambiguity hash mismatch for result {result_id}")
    return record


def load_result_ambiguity(conn: psycopg.Connection[Any], result_id: int) -> AmbiguityRecord | None:
    """The frozen record, hash-verified, or ``None`` when no record exists.

    ⚠ Corruption RAISES rather than refuses, matching ``load_result_universe``
    and ``load_promotion_evidence``: a record that fails its own hash is an
    integrity failure to surface loudly, not a gate verdict to report politely.
    The cost is named in the #2625 spec — a corrupt record aborts before the
    other refusals are gathered, so it MASKS them.
    """
    row = conn.execute(
        f"""
        SELECT {_SELECT_AMBIGUITY_COLUMNS}
        FROM strategy_result_ambiguity
        WHERE result_id = %s
        """,  # noqa: S608 - module-level column literal, no caller input
        (result_id,),
    ).fetchone()
    if row is None:
        return None
    return _record_from_row(result_id, row)


def load_result_ambiguities(conn: psycopg.Connection[Any], result_ids: Sequence[int]) -> dict[int, AmbiguityRecord]:
    """Every frozen record for ``result_ids``, in ONE statement (#2641).

    Keys are only the results that HAVE a record; an absent one is already named
    by ``ambiguity_verdict_unrecorded``, so a partial mapping preserves the
    distinction. Same snapshot bound as ``load_result_universes``: one instant
    for this record type, not across record types.
    """
    if not result_ids:
        return {}
    rows = conn.execute(
        f"""
        SELECT result_id, {_SELECT_AMBIGUITY_COLUMNS}
        FROM strategy_result_ambiguity
        WHERE result_id = ANY(%(result_ids)s::bigint[])
        """,  # noqa: S608 - module-level column literal, no caller input
        {"result_ids": list(result_ids)},
    ).fetchall()
    return {int(row[0]): _record_from_row(int(row[0]), row[1:]) for row in rows}


def ambiguity_promotion_refusals(record: AmbiguityRecord | None) -> tuple[str, ...]:
    """Every ambiguity reason the transition may not promote this result.

    Pure, returns ALL refusals rather than the first (the ``check_promotable``
    contract). The two verdict clauses reuse that gate's own codes; the two new
    codes name states only the transition can see — a record that is absent, or
    frozen under a rule version this code cannot interpret.

    ⚠ Four distinguishable states, four distinct outcomes: absent →
    ``ambiguity_verdict_unrecorded``; verdict ``None`` →
    ``ambiguity_arms_not_compared``; verdict ``True`` → ``ambiguity_material``;
    verdict ``False`` → nothing.
    """
    if record is None:
        return ("ambiguity_verdict_unrecorded",)
    refusals: list[str] = []
    if record.ambiguity_rule_version not in RECOGNISED_AMBIGUITY_RULE_VERSIONS:
        refusals.append("ambiguity_rule_unrecognised")
    # ⚠ Same two clauses, same order, as ``check_promotable``'s §3.4 block.
    verdict = ambiguity_verdict(record)
    if verdict is None:
        refusals.append("ambiguity_arms_not_compared")
    elif verdict:
        refusals.append("ambiguity_material")
    return tuple(refusals)


def composed_holdout_ambiguity_refusals(
    local_record: AmbiguityRecord | None,
    support_record: AmbiguityRecord | None,
) -> tuple[str, ...]:
    """Compose one holdout pair with its exact in-sample §3.4 support (#2749).

    The holdout record is always authoritative when it is absent, corrupt
    (raised while loading), unrecognised, material, or directly proves a zero
    gap. Only the honest ``ambiguity_arms_not_compared`` state may consult the
    derived in-sample companion. This never applies an in-sample threshold to
    holdout Sharpes; it replays the companion's own complete verdict instead.

    ``support_record is None`` covers both no unique identity-compatible
    companion and a companion with no frozen ambiguity record. In either case
    the local measured-but-unjudged verdict remains the truthful refusal.
    """
    local_refusals = ambiguity_promotion_refusals(local_record)
    if local_refusals != ("ambiguity_arms_not_compared",) or support_record is None:
        return local_refusals
    return ambiguity_promotion_refusals(support_record)


def exact_ambiguity_support_id(candidate_count: int, support_id: int | None) -> int | None:
    """Accept only the view's one-candidate/one-id state.

    The view already emits ``NULL`` unless its count is exactly one. Rechecking
    both fields here is deliberate defence in depth: a future view must not be
    able to hand promotion a favourable id while admitting multiple candidates.
    """
    if candidate_count != 1 or support_id is None:
        return None
    return support_id


_SELECT_PROMOTION_SUPPORT = """
    SELECT r.result_id, r.namespace,
           COALESCE(s.candidate_count, 0), s.control_result_id
    FROM strategy_results_store r
    LEFT JOIN strategy_result_control_support s
      ON s.holdout_result_id = r.result_id
    WHERE r.result_id = ANY(%(result_ids)s::bigint[])
"""


def load_promotion_ambiguity_refusals(
    conn: psycopg.Connection[Any], result_ids: Sequence[int]
) -> dict[int, tuple[str, ...]]:
    """Batched ambiguity replay for pinned results, including #2749 support.

    The support view derives its candidate from immutable identity pins; no
    caller supplies a support id and these reads record no holdout access. The
    result is refusal codes rather than records, so this cannot become another
    public door for reading withheld metrics.
    """
    if not result_ids:
        return {}
    rows = conn.execute(_SELECT_PROMOTION_SUPPORT, {"result_ids": list(result_ids)}).fetchall()
    census = {int(row[0]): (str(row[1]), int(row[2]), None if row[3] is None else int(row[3])) for row in rows}
    missing = set(result_ids) - set(census)
    if missing:
        raise RuntimeError(f"no stored result row for result_id(s) {sorted(missing)}")

    local_records = load_result_ambiguities(conn, result_ids)
    support_ids_by_result: dict[int, int] = {}
    for result_id in result_ids:
        namespace, candidate_count, support_id = census[result_id]
        local_refusals = ambiguity_promotion_refusals(local_records.get(result_id))
        exact_support = exact_ambiguity_support_id(candidate_count, support_id)
        if namespace == "hold_out" and local_refusals == ("ambiguity_arms_not_compared",) and exact_support is not None:
            support_ids_by_result[result_id] = exact_support
    support_records = load_result_ambiguities(conn, sorted(set(support_ids_by_result.values())))

    return {
        result_id: (
            composed_holdout_ambiguity_refusals(
                local_records.get(result_id),
                support_records.get(support_ids_by_result[result_id]) if result_id in support_ids_by_result else None,
            )
            if census[result_id][0] == "hold_out"
            else ambiguity_promotion_refusals(local_records.get(result_id))
        )
        for result_id in result_ids
    }


__all__ = [
    "AMBIGUITY_RULE_VERSION",
    "LEGACY_AMBIGUITY_RULE_VERSION",
    "RECOGNISED_AMBIGUITY_RULE_VERSIONS",
    "AmbiguityRecord",
    "ComparisonBasis",
    "ambiguity_promotion_refusals",
    "ambiguity_verdict",
    "composed_holdout_ambiguity_refusals",
    "exact_ambiguity_support_id",
    "load_result_ambiguity",
    "load_promotion_ambiguity_refusals",
    "matched_control_margin",
    "record_sha256",
    "store_result_ambiguity",
]
