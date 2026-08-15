"""Freeze MT-1 and its S-8 control declarations before any outcome access.

The two rows jointly bind the four-arm controlled experiment frozen in
``docs/proposals/ta/2026-08-15-mt1-volatility-managed-relative-strength-
preregistration.md``. Run ``--dry-run`` first and inspect both complete digest
payloads. The write path is one transaction: either both declarations exist
with the expected digests or neither new row is committed.

This script must be merged before it is run. The structural-policy guard
refreshes ``origin/main`` and refuses a write when the policy in this tree is
not the merged policy. Outcomes remain sealed until a separate evaluator path
successfully requires these exact declarations.
"""

from __future__ import annotations

import argparse
import json
import sys

import psycopg

from app.config import settings
from app.services.prereg_contract import PreregDeclaration
from app.services.result_ledger import PreregDeclarationRefused, freeze_preregistration, load_preregistration
from app.services.strategy_mt1_preregistration import (
    DECLARED_BY,
    MIN_FORWARD_CALENDAR_WEEKS,
    MIN_FORWARD_DECISION_DATES,
    build_declarations,
    build_mt1_declaration,
    build_s8_control_declaration,
)
from scripts._prereg_freeze_guard import assert_policy_version_merged, policy_version_report


def _summary(declaration: PreregDeclaration) -> dict[str, object]:
    return {**declaration.digest_payload, "declaration_sha256": declaration.sha256}


def _freeze_batch(
    conn: psycopg.Connection[tuple], declarations: tuple[PreregDeclaration, ...]
) -> tuple[list[dict[str, object]], bool]:
    """Freeze every missing row atomically; accept only byte-identical retries."""
    existing = {
        (declaration.strategy_id, declaration.strategy_version): load_preregistration(
            conn, declaration.strategy_id, declaration.strategy_version
        )
        for declaration in declarations
    }
    conflicts = [
        declaration
        for declaration in declarations
        if (stored := existing[(declaration.strategy_id, declaration.strategy_version)]) is not None
        and stored.declaration_sha256 != declaration.sha256
    ]
    if conflicts:
        conn.rollback()
        conflict_reports: list[dict[str, object]] = []
        for declaration in conflicts:
            stored = existing[(declaration.strategy_id, declaration.strategy_version)]
            assert stored is not None
            conflict_reports.append(
                {
                    **_summary(declaration),
                    "outcome": "conflicting_declaration_already_frozen",
                    "stored_declaration_sha256": stored.declaration_sha256,
                    "note": "declarations are immutable; different terms require a new strategy_version",
                }
            )
        return conflict_reports, False

    reports: list[dict[str, object]] = []
    try:
        for declaration in declarations:
            stored = existing[(declaration.strategy_id, declaration.strategy_version)]
            if stored is not None:
                reports.append(
                    {
                        **_summary(declaration),
                        "outcome": "already_frozen_identical",
                        "declaration_id": stored.declaration_id,
                    }
                )
                continue
            declaration_id = freeze_preregistration(conn, declaration)
            reports.append({**_summary(declaration), "outcome": "frozen", "declaration_id": declaration_id})
    except PreregDeclarationRefused as error:
        conn.rollback()
        return ([{"outcome": "batch_refused_and_rolled_back", "refusals": list(error.refusals)}], False)
    except psycopg.errors.UniqueViolation:
        # A concurrent identical batch is an idempotent success, but only after
        # this transaction is rolled back and every stored digest is re-read.
        # Any missing or different row is a failed batch, never guessed safe.
        conn.rollback()
        raced = [
            load_preregistration(conn, declaration.strategy_id, declaration.strategy_version)
            for declaration in declarations
        ]
        raced_pairs = tuple(zip(raced, declarations, strict=True))
        if all(
            stored is not None and stored.declaration_sha256 == declaration.sha256
            for stored, declaration in raced_pairs
        ):
            return (
                [
                    {
                        **_summary(declaration),
                        "outcome": "already_frozen_identical",
                        "declaration_id": stored.declaration_id,
                    }
                    for declaration, stored in zip(declarations, raced, strict=True)
                    if stored is not None
                ],
                True,
            )
        return ([{"outcome": "concurrent_batch_conflict_and_rolled_back"}], False)
    conn.commit()
    return reports, True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="print both digest payloads without writing")
    parser.add_argument(
        "--allow-policy-divergence",
        action="store_true",
        help="override the merged-policy guard and record that override in output",
    )
    args = parser.parse_args(argv)
    declarations = build_declarations()
    if args.dry_run:
        policy = policy_version_report()
        for declaration in declarations:
            payload = {**_summary(declaration), **policy, "outcome": "dry_run"}
            sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")
        return 0

    policy = assert_policy_version_merged(allow_divergence=args.allow_policy_divergence)
    with psycopg.connect(settings.database_url) as conn:
        reports, ok = _freeze_batch(conn, declarations)
    stream = sys.stdout if ok else sys.stderr
    for report in reports:
        stream.write(json.dumps({**report, **policy}, sort_keys=True) + "\n")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DECLARED_BY",
    "MIN_FORWARD_CALENDAR_WEEKS",
    "MIN_FORWARD_DECISION_DATES",
    "build_declarations",
    "build_mt1_declaration",
    "build_s8_control_declaration",
    "main",
]
