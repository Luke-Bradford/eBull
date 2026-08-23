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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

import psycopg

from app.services.strategies.validated_universe import VALIDATED_UNIVERSE_RULE_VERSION
from app.services.universe_selection import UNIVERSE_SELECTION_RULE_VERSION

#: The versions the transition knows how to interpret. ⚠ FAIL CLOSED IS AN
#: ALLOWLIST: when the §4.0 definition evolves the constant bumps, old records
#: refuse ``evaluated_universe_rule_unrecognised``, and the successor decides
#: explicitly whether to re-admit the old version — never a silent reread under
#: new semantics. Same shape as ``trial_register_superseded``.
#:
#: #2721 step 3 adds the survivorship-free selection rule: a record produced
#: under it freezes series-level membership (``evaluated_series_ids``)
#: alongside the instrument ids.
RECOGNISED_UNIVERSE_RULE_VERSIONS: Final[frozenset[str]] = frozenset(
    {VALIDATED_UNIVERSE_RULE_VERSION, UNIVERSE_SELECTION_RULE_VERSION}
)


@dataclass(frozen=True)
class ResultUniverseRecord:
    """The two frozen sets plus the definition version they were produced under."""

    universe_rule_version: str
    evaluated_instrument_ids: frozenset[int]
    validated_universe_ids: frozenset[int]
    #: #2721 step 3 — series ids of evaluated names with NO instrument link
    #: (the dead names). Empty on every survivor-only record. ⚠ The engine's
    #: in-pass synthetic key ``-series_id`` is never stored; this field is its
    #: only persisted form, and the negative-key write-boundary test holds the
    #: line.
    evaluated_series_ids: frozenset[int] = frozenset()

    def __post_init__(self) -> None:
        if not self.universe_rule_version:
            raise ValueError("universe_rule_version must be non-empty")
        for name in ("evaluated_instrument_ids", "validated_universe_ids", "evaluated_series_ids"):
            if any(type(item) is not int for item in getattr(self, name)):
                raise ValueError(f"{name} must contain only ints")
        for name in ("evaluated_instrument_ids", "validated_universe_ids", "evaluated_series_ids"):
            if any(item <= 0 for item in getattr(self, name)):
                raise ValueError(f"{name} must contain only positive ids — a synthetic name key may not be stored")


def _canonical_payload(record: ResultUniverseRecord) -> bytes:
    """One byte-stable encoding — JSON, not ad-hoc joins, so a rule version
    containing any delimiter cannot make two records hash alike."""
    return json.dumps(
        {
            "universe_rule_version": record.universe_rule_version,
            "evaluated_instrument_ids": sorted(record.evaluated_instrument_ids),
            "validated_universe_ids": sorted(record.validated_universe_ids),
            "evaluated_series_ids": sorted(record.evaluated_series_ids),
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
            validated_universe_ids, evaluated_series_ids, payload_sha256
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            result_id,
            record.universe_rule_version,
            sorted(record.evaluated_instrument_ids),
            sorted(record.validated_universe_ids),
            sorted(record.evaluated_series_ids),
            record_sha256(record),
        ),
    )


_SELECT_UNIVERSE_COLUMNS = (
    "universe_rule_version, evaluated_instrument_ids, validated_universe_ids, payload_sha256, evaluated_series_ids"
)


def _record_from_row(result_id: int, row: Any) -> ResultUniverseRecord:
    """Verify and rebuild one row. Shared so the single and batch reads cannot
    verify differently — a batch that skipped the hash check would be a second
    door onto the same record with weaker integrity."""
    evaluated = [int(item) for item in row[1]]
    universe = [int(item) for item in row[2]]
    series = [int(item) for item in row[4]]
    for name, ids in (
        ("evaluated_instrument_ids", evaluated),
        ("validated_universe_ids", universe),
        ("evaluated_series_ids", series),
    ):
        if ids != sorted(set(ids)):
            raise RuntimeError(f"result universe {name} is not sorted-unique for result {result_id}")
    record = ResultUniverseRecord(
        universe_rule_version=str(row[0]),
        evaluated_instrument_ids=frozenset(evaluated),
        validated_universe_ids=frozenset(universe),
        evaluated_series_ids=frozenset(series),
    )
    if record_sha256(record) != str(row[3]):
        raise RuntimeError(f"result universe hash mismatch for result {result_id}")
    return record


#: sql/354's closed stratum vocabulary, mirrored (the HoldoutAccess-style
#: deliberate duplication): a malformed stratum fails HERE naming the value,
#: and the CHECK stays as the backstop for any writer that bypasses this
#: module. Derived from ``series_termination``'s enums rather than retyped, so
#: a class added there without a migration fails the mirror test, not silently.
TERMINATION_CENSUS_STRATA: Final[frozenset[str]] = frozenset(
    {
        "terminated_exchange_failure",
        "terminated_exchange_failure_a4",
        "terminated_operation_of_law",
        "terminated_linked_unparsed_provision",
        "terminated_q_suffix_otc_unverified",
        "terminated_unknown_termination",
        "termination_skipped_series_break",
        "termination_skipped_unresolved_outcome",
        "termination_skipped_close_bar_unfillable",
        "termination_price_unlocatable",
        "universe_admitted_total",
        "universe_unlinked_alive_excluded",
        "universe_linked_early_reuse_suspect",
        "universe_exchange_test_issues_excluded",
        "universe_unharvested_excluded",
        "universe_vendor_series_total",
    }
)

#: The MECE universe terms carried by every survivorship-free result.  The
#: reuse-suspect count is deliberately absent: it is a diagnostic SUBSET of
#: ``universe_admitted_total`` and adding it would double-count those series.
_TERMINATION_CENSUS_RECONCILIATION_TERMS: Final[tuple[str, ...]] = (
    "universe_admitted_total",
    "universe_unlinked_alive_excluded",
    "universe_exchange_test_issues_excluded",
    "universe_unharvested_excluded",
    "universe_vendor_series_total",
)


def store_termination_census(conn: psycopg.Connection[Any], *, result_id: int, census: Mapping[str, int]) -> None:
    """Insert one result's termination census rows (#2721 step 3).

    Runs in the CALLER's transaction, atomically with the result's arm pair —
    the same contract as ``store_result_universe``. The writer (not this
    function) decides that a ``survivorship_free`` row REQUIRES a census; this
    function refuses malformed content AND proves the selection terms reconcile
    before the first write.  A caller cannot turn a partial census into a
    durable result merely by omitting the denominator or one exclusion bucket.
    """
    unknown = set(census) - TERMINATION_CENSUS_STRATA
    if unknown:
        raise ValueError(f"termination census carries strata outside the closed vocabulary: {sorted(unknown)}")
    negative = {stratum for stratum, count in census.items() if count < 0}
    if negative:
        raise ValueError(f"termination census carries negative counts: {sorted(negative)}")
    required_terms: set[str] = set(_TERMINATION_CENSUS_RECONCILIATION_TERMS)
    missing = required_terms - set(census)
    if missing:
        raise ValueError(f"termination census is missing reconciliation terms: {sorted(missing)}")
    admitted = census["universe_admitted_total"]
    unlinked_alive = census["universe_unlinked_alive_excluded"]
    exchange_test_issues = census["universe_exchange_test_issues_excluded"]
    unharvested = census["universe_unharvested_excluded"]
    vendor_total = census["universe_vendor_series_total"]
    reconciled = admitted + unlinked_alive + exchange_test_issues + unharvested
    if reconciled != vendor_total:
        raise ValueError(
            "termination census does not reconcile to the vendor series total: "
            f"admitted {admitted} + unlinked-alive {unlinked_alive} + "
            f"exchange-test-issues {exchange_test_issues} + unharvested {unharvested} "
            f"= {reconciled}, vendor total {vendor_total}"
        )
    reuse_suspects = census.get("universe_linked_early_reuse_suspect", 0)
    if reuse_suspects > admitted:
        raise ValueError(
            f"termination census reuse-suspect subset exceeds admitted total: {reuse_suspects} > {admitted}"
        )
    for stratum in sorted(census):
        conn.execute(
            "INSERT INTO strategy_result_termination_census (result_id, stratum, count) VALUES (%s, %s, %s)",
            (result_id, stratum, int(census[stratum])),
        )


def load_termination_census(conn: psycopg.Connection[Any], result_id: int) -> dict[str, int]:
    """The stored census for one result row — empty dict when none exists."""
    rows = conn.execute(
        "SELECT stratum, count FROM strategy_result_termination_census WHERE result_id = %s",
        (result_id,),
    ).fetchall()
    return {str(stratum): int(count) for stratum, count in rows}


def load_result_universe(conn: psycopg.Connection[Any], result_id: int) -> ResultUniverseRecord | None:
    """The frozen record, hash-verified, or ``None`` when no record exists.

    ⚠ Corruption RAISES rather than refuses, matching ``load_promotion_evidence``:
    a record that fails its own hash or canonical form is an integrity failure
    to surface loudly, not a gate verdict to report politely.
    """
    row = conn.execute(
        f"""
        SELECT {_SELECT_UNIVERSE_COLUMNS}
        FROM strategy_result_universe
        WHERE result_id = %s
        """,  # noqa: S608 - module-level column literal, no caller input
        (result_id,),
    ).fetchone()
    if row is None:
        return None
    return _record_from_row(result_id, row)


def load_result_universes(conn: psycopg.Connection[Any], result_ids: Sequence[int]) -> dict[int, ResultUniverseRecord]:
    """Every frozen record for ``result_ids``, in ONE statement (#2641).

    Keys are only the results that HAVE a record — an absent record is a state
    the caller's refusal vocabulary already names (``universe_record_absent``),
    so returning a partial mapping keeps that distinction rather than inventing
    a sentinel.

    ⚠ One statement means one snapshot for this record type: every result's
    record is read at the same instant, where the per-result loop read each at a
    slightly different one under READ COMMITTED. It does NOT put the universe,
    ambiguity, evidence and row reads on a single snapshot between them — that
    would need a repeatable-read transaction, and the tables are immutable by
    trigger, so the remaining skew is unobservable rather than merely small.
    """
    if not result_ids:
        return {}
    rows = conn.execute(
        f"""
        SELECT result_id, {_SELECT_UNIVERSE_COLUMNS}
        FROM strategy_result_universe
        WHERE result_id = ANY(%(result_ids)s::bigint[])
        """,  # noqa: S608 - module-level column literal, no caller input
        {"result_ids": list(result_ids)},
    ).fetchall()
    return {int(row[0]): _record_from_row(int(row[0]), row[1:]) for row in rows}


def universe_promotion_refusals(
    record: ResultUniverseRecord | None,
    *,
    evaluated_instrument_count: int,
    expected_opportunity_digest: str | None = None,
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
    evaluated_name_count = len(record.evaluated_instrument_ids) + len(record.evaluated_series_ids)
    if evaluated_name_count != evaluated_instrument_count:
        refusals.append("evaluated_universe_count_mismatch")
    if expected_opportunity_digest is not None and record_sha256(record) != expected_opportunity_digest:
        refusals.append("metric_axis_unproven")
    # ⚠ Same two clauses, same order, as ``check_promotable`` — an empty
    # evaluated set is refused separately because ``set() - anything`` is empty.
    if evaluated_name_count == 0:
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
