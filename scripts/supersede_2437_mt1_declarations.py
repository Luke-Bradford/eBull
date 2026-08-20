"""Repair MT-1 declarations for #2697's metric-axis policy bump.

The original declarations were frozen under v3 from merged code, but while
PR #2757's v4 policy change was still in flight.  This script can change only
the policy-version fields that ``supersede_preregistration`` permits.  It pins
the two predecessor IDs and digests, verifies every substantive term against
the merged declaration builders, and commits both successors atomically.

Run ``--dry-run`` first.  The write path is permitted only after the v4 policy
is on ``origin/main`` and while neither trial has an access or hold-out result.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from typing import Final

import psycopg

from app.config import settings
from app.services.prereg_contract import (
    PreregDeclaration,
    Supersession,
    changed_supersession_terms,
    declaration_refusals,
)
from app.services.result_ledger import holdout_access_counts, load_preregistration, supersede_preregistration
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION, structural_promotion_refusals
from scripts._prereg_freeze_guard import assert_policy_version_merged, policy_version_report
from scripts.freeze_2437_mt1_declarations import build_declarations

PREDECESSOR_POLICY_VERSION: Final = "structural-refusal-policy-2026-08-15-v3-survivorship-free-satisfiable"
DECLARED_BY: Final = "scripts/supersede_2437_mt1_declarations.py (#2697/#2437)"
ATTESTATION: Final = (
    "No price, return, performance, sample-composition or outcome value for either MT-1 trial was accessed before "
    "this policy-only supersession. Read-only verification on 2026-08-15 found zero holdout evaluations and zero "
    "recorded accesses for both identities. This repairs the known #2697 metric-axis policy-version bump and "
    "changes no trial term."
)
_PREDECESSORS: Final = {
    "mt1-capped-volatility-managed-relative-strength-v1": (
        8,
        "11aeefa42edc47b553a1f90329f4b961e728988b596eab08345f571500f8604a",
    ),
    "mt1-s8-capped-volatility-negative-control-v1": (
        9,
        "ebdee0b9645a8b070e10bc9dad2c0d8fe57e523285774b513cafa8479efa5334",
    ),
}


def build_successor(predecessor: PreregDeclaration) -> PreregDeclaration:
    expected_refusals = structural_promotion_refusals(
        universe_basis=predecessor.declared_universe_basis,
        carry_unmodelled=predecessor.declared_carry_unmodelled,
        fx_unmodelled=predecessor.declared_fx_unmodelled,
    )
    return replace(
        predecessor,
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        expected_structural_refusals=expected_refusals,
        declared_by=DECLARED_BY,
    )


def _preflight(
    conn: psycopg.Connection[tuple],
) -> tuple[tuple[PreregDeclaration, PreregDeclaration, int, int], ...]:
    pairs: list[tuple[PreregDeclaration, PreregDeclaration, int, int]] = []
    for expected in build_declarations():
        frozen = load_preregistration(conn, expected.strategy_id, expected.strategy_version)
        if frozen is None:
            raise RuntimeError(f"{expected.strategy_id}@{expected.strategy_version}: predecessor is not frozen")
        predecessor_id, predecessor_digest = _PREDECESSORS[expected.strategy_id]
        if predecessor_id not in frozen.chain_declaration_ids:
            raise RuntimeError(f"{expected.strategy_id}: declaration chain does not contain {predecessor_id}")
        stored_predecessor = conn.execute(
            """SELECT declaration_sha256, structural_refusal_policy_version
                 FROM strategy_preregistration_declarations WHERE declaration_id = %s""",
            (predecessor_id,),
        ).fetchone()
        if stored_predecessor != (predecessor_digest, PREDECESSOR_POLICY_VERSION):
            raise RuntimeError(f"{expected.strategy_id}: pinned predecessor differs from the recorded v3 declaration")
        if not frozen.digest_intact:
            raise RuntimeError(f"{expected.strategy_id}: current declaration digest is not intact")
        if frozen.declaration.structural_refusal_policy_version == PREDECESSOR_POLICY_VERSION:
            if frozen.declaration_id != predecessor_id:
                raise RuntimeError(f"{expected.strategy_id}: pinned v3 predecessor is not current")
        elif frozen.declaration.structural_refusal_policy_version == STRUCTURAL_REFUSAL_POLICY_VERSION:
            if frozen.supersedes_declaration_id != predecessor_id:
                raise RuntimeError(f"{expected.strategy_id}: current v4 declaration does not supersede the pin")
        else:
            raise RuntimeError(
                f"{expected.strategy_id}: current policy is neither the pinned predecessor nor current v4"
            )
        changed = changed_supersession_terms(frozen.declaration, expected)
        if changed:
            raise RuntimeError(f"{expected.strategy_id}: frozen substantive terms differ: {changed}")
        successor = (
            frozen.declaration
            if frozen.declaration.structural_refusal_policy_version == STRUCTURAL_REFUSAL_POLICY_VERSION
            else build_successor(frozen.declaration)
        )
        refusals = declaration_refusals(successor)
        if refusals:
            raise RuntimeError(f"{expected.strategy_id}: successor is incoherent: {refusals}")
        exposure = holdout_access_counts(conn, expected.strategy_id, expected.strategy_version)
        if exposure.holdout_evaluations or exposure.recorded_accesses:
            raise RuntimeError(
                f"{expected.strategy_id}: supersession refused after "
                f"{exposure.holdout_evaluations} holdout evaluations and {exposure.recorded_accesses} accesses"
            )
        pairs.append(
            (
                frozen.declaration,
                successor,
                exposure.holdout_evaluations,
                exposure.recorded_accesses,
            )
        )
    return tuple(pairs)


def _report(
    predecessor: PreregDeclaration,
    successor: PreregDeclaration,
    holdout_evaluations: int,
    recorded_accesses: int,
    *,
    outcome: str,
) -> dict[str, object]:
    return {
        **successor.digest_payload,
        "predecessor_policy_version": predecessor.structural_refusal_policy_version,
        "predecessor_declaration_sha256": predecessor.sha256,
        "successor_declaration_sha256": successor.sha256,
        "supersession_reason": "structural_refusal_policy_superseded",
        "supersession_attestation": ATTESTATION,
        "holdout_evaluations": holdout_evaluations,
        "recorded_accesses": recorded_accesses,
        "outcome": outcome,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="print both policy-only successors without writing")
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        pairs = _preflight(conn)
        if args.dry_run:
            conn.rollback()
            policy = policy_version_report()
            for predecessor, successor, holdout_evaluations, recorded_accesses in pairs:
                outcome = (
                    "already_current"
                    if predecessor.structural_refusal_policy_version == STRUCTURAL_REFUSAL_POLICY_VERSION
                    else "dry_run"
                )
                report = _report(
                    predecessor,
                    successor,
                    holdout_evaluations,
                    recorded_accesses,
                    outcome=outcome,
                )
                sys.stdout.write(json.dumps({**report, **policy}, sort_keys=True) + "\n")
            return 0

        policy = assert_policy_version_merged()
        reports: list[dict[str, object]] = []
        for predecessor, successor, holdout_evaluations, recorded_accesses in pairs:
            if predecessor.structural_refusal_policy_version == STRUCTURAL_REFUSAL_POLICY_VERSION:
                reports.append(
                    _report(
                        predecessor,
                        successor,
                        holdout_evaluations,
                        recorded_accesses,
                        outcome="already_current",
                    )
                )
                continue
            successor_id = supersede_preregistration(
                conn,
                successor,
                Supersession(reason="structural_refusal_policy_superseded", attestation=ATTESTATION),
            )
            reports.append(
                {
                    **_report(
                        predecessor,
                        successor,
                        holdout_evaluations,
                        recorded_accesses,
                        outcome="superseded",
                    ),
                    "successor_declaration_id": successor_id,
                }
            )
        conn.commit()
    for report in reports:
        sys.stdout.write(json.dumps({**report, **policy}, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ATTESTATION", "DECLARED_BY", "PREDECESSOR_POLICY_VERSION", "build_successor", "main"]
