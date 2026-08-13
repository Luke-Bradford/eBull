"""Frozen universe inputs for the promotion transition (#2621).

``check_promotable``'s universe clause compares two sets that no relation held:
the result's evaluated instrument ids and the §4.0 validated universe the run
loaded. The refusal it produced was returned in ``WrittenRow.refusals`` and
died with the writer's return value, so ``promote_strategy`` — the transition
that authorises ``historical_validated`` — could not re-detect a result whose
evaluated set left the universe. This module persists both sets, immutable and
hashed (the ``sql/327`` promotion-evidence shape), and gives the transition a
pure refusal function over the frozen record.

⚠ FROZEN AT RESULT TIME, NEVER TODAY'S ``load_validated_universe`` (#2621 scope
item 3). Three reasons, recorded because picking silently is the defect the
issue names:

- the transition re-derives refusals from frozen records; today's date enters
  that gate only where a validity window was DECLARED (``cost_observed_on`` /
  ``cost_valid_through``). The universe declares no validity window, so a
  today-check would be an undeclared freshness rule.
- today's universe filters on ``is_tradable`` — today's listing state — so a
  re-check against it retroactively invalidates a passing historical result on
  any delisting, while the survivorship-free corpus (#2597, the only basis
  ``universe_basis_not_survivorship_free`` will ever pass) deliberately
  evaluates delisted names.
- enforcement against the CURRENT universe belongs to the execution guard at
  order time (§4.0 allocation invariant 2), not to evidence admission.

⚠ WHAT THE RE-CHECK DOES AND DOES NOT PROVE. Both frozen sets come from the
same writer, so replaying the subtraction does NOT certify the writer was
honest at write time — the write-time criterion-8 cross-check raise does that.
What persistence buys: a result stored by any OTHER path refuses
(``evaluated_universe_unrecorded``); the record is immutable and independently
auditable (against ``instrument_universe_membership``, #2290, as-of the result
window); the record must agree with the row's own ``evaluated_instrument_count``;
and the check stays replayable after the live universe has moved on.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Final

import psycopg

from app.services.strategies.validated_universe import VALIDATED_UNIVERSE_RULE_VERSION

#: The versions the transition knows how to interpret. ⚠ FAIL CLOSED IS AN
#: ALLOWLIST: when the §4.0 definition evolves the constant bumps, old records
#: refuse ``evaluated_universe_rule_unrecognised``, and the successor decides
#: explicitly whether to re-admit the old version — never a silent reread under
#: new semantics. Same shape as ``trial_register_superseded``.
RECOGNISED_UNIVERSE_RULE_VERSIONS: Final[frozenset[str]] = frozenset({VALIDATED_UNIVERSE_RULE_VERSION})


@dataclass(frozen=True)
class ResultUniverseRecord:
    """The two frozen sets plus the definition version they were produced under."""

    universe_rule_version: str
    evaluated_instrument_ids: frozenset[int]
    validated_universe_ids: frozenset[int]

    def __post_init__(self) -> None:
        if not self.universe_rule_version:
            raise ValueError("universe_rule_version must be non-empty")
        for name in ("evaluated_instrument_ids", "validated_universe_ids"):
            if any(type(item) is not int for item in getattr(self, name)):
                raise ValueError(f"{name} must contain only ints")


def _canonical_payload(record: ResultUniverseRecord) -> bytes:
    """One byte-stable encoding — JSON, not ad-hoc joins, so a rule version
    containing any delimiter cannot make two records hash alike."""
    return json.dumps(
        {
            "universe_rule_version": record.universe_rule_version,
            "evaluated_instrument_ids": sorted(record.evaluated_instrument_ids),
            "validated_universe_ids": sorted(record.validated_universe_ids),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def record_sha256(record: ResultUniverseRecord) -> str:
    return hashlib.sha256(_canonical_payload(record)).hexdigest()


def store_result_universe(conn: psycopg.Connection[Any], *, result_id: int, record: ResultUniverseRecord) -> None:
    """Insert one frozen record. Runs in the CALLER's transaction — the writer
    stores it atomically with the result's arm pair."""
    conn.execute(
        """
        INSERT INTO strategy_result_universe (
            result_id, universe_rule_version, evaluated_instrument_ids,
            validated_universe_ids, payload_sha256
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (
            result_id,
            record.universe_rule_version,
            sorted(record.evaluated_instrument_ids),
            sorted(record.validated_universe_ids),
            record_sha256(record),
        ),
    )


def load_result_universe(conn: psycopg.Connection[Any], result_id: int) -> ResultUniverseRecord | None:
    """The frozen record, hash-verified, or ``None`` when no record exists.

    ⚠ Corruption RAISES rather than refuses, matching ``load_promotion_evidence``:
    a record that fails its own hash or canonical form is an integrity failure
    to surface loudly, not a gate verdict to report politely.
    """
    row = conn.execute(
        """
        SELECT universe_rule_version, evaluated_instrument_ids, validated_universe_ids, payload_sha256
        FROM strategy_result_universe
        WHERE result_id = %s
        """,
        (result_id,),
    ).fetchone()
    if row is None:
        return None
    evaluated = [int(item) for item in row[1]]
    universe = [int(item) for item in row[2]]
    for name, ids in (("evaluated_instrument_ids", evaluated), ("validated_universe_ids", universe)):
        if ids != sorted(set(ids)):
            raise RuntimeError(f"result universe {name} is not sorted-unique for result {result_id}")
    record = ResultUniverseRecord(
        universe_rule_version=str(row[0]),
        evaluated_instrument_ids=frozenset(evaluated),
        validated_universe_ids=frozenset(universe),
    )
    if record_sha256(record) != str(row[3]):
        raise RuntimeError(f"result universe hash mismatch for result {result_id}")
    return record


def universe_promotion_refusals(
    record: ResultUniverseRecord | None, *, evaluated_instrument_count: int
) -> tuple[str, ...]:
    """Every universe reason the transition may not promote this result.

    Pure, returns ALL refusals rather than the first (the ``check_promotable``
    contract). The two set clauses reuse that gate's own codes; the three new
    codes name states only the transition can see — a record that is absent,
    frozen under a version this code cannot interpret, or inconsistent with the
    row it claims to describe.
    """
    if record is None:
        return ("evaluated_universe_unrecorded",)
    refusals: list[str] = []
    if record.universe_rule_version not in RECOGNISED_UNIVERSE_RULE_VERSIONS:
        refusals.append("evaluated_universe_rule_unrecognised")
    if len(record.evaluated_instrument_ids) != evaluated_instrument_count:
        refusals.append("evaluated_universe_count_mismatch")
    # ⚠ Same two clauses, same order, as ``check_promotable`` — an empty
    # evaluated set is refused separately because ``set() - anything`` is empty.
    if not record.evaluated_instrument_ids:
        refusals.append("no_instruments_evaluated")
    elif record.evaluated_instrument_ids - record.validated_universe_ids:
        refusals.append("instrument_outside_validated_universe")
    return tuple(refusals)


__all__ = [
    "RECOGNISED_UNIVERSE_RULE_VERSIONS",
    "ResultUniverseRecord",
    "load_result_universe",
    "record_sha256",
    "store_result_universe",
    "universe_promotion_refusals",
]
