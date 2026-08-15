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
import math
import sys
from typing import Final

import psycopg

from app.config import settings
from app.services.cost_model import CARRY_UNMODELLED, COST_MODEL_ID, FX_UNMODELLED
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.result_ledger import PreregDeclarationRefused, freeze_preregistration, load_preregistration
from app.services.strategy_mt1_identity import mt1_identity, s8_control_identity
from app.services.strategy_mt1_trial import TRIAL_CONTRACT_VERSION
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION, structural_promotion_refusals
from scripts._prereg_freeze_guard import assert_policy_version_merged, policy_version_report

DECLARED_BY: Final = "scripts/freeze_2437_mt1_declarations.py (#2437)"
UNIVERSE_BASIS: Final = "survivorship_free"

_Z_0975: Final = 1.959963984540054
_Z_08: Final = 0.8416212335729143
_STANDARDISED_EFFECT: Final = 0.5
_POWER_DERIVED_MONTHS: Final = math.ceil(((_Z_0975 + _Z_08) / _STANDARDISED_EFFECT) ** 2)
MIN_FORWARD_DECISION_DATES: Final = max(36, _POWER_DERIVED_MONTHS)
MIN_FORWARD_CALENDAR_WEEKS: Final = math.ceil(MIN_FORWARD_DECISION_DATES * 365.25 / (12 * 7))

_FORWARD_SHADOW_DERIVATION: Final = (
    "Frozen preregistration power floor: standardised paired monthly effect=0.5, alpha=0.05 two-sided, "
    "power=0.8; z_0.975=1.959963984540054 and z_0.8=0.8416212335729143. "
    "n=ceil(((1.959963984540054+0.8416212335729143)/0.5)^2)=32 independent decision months; raised to "
    "36 months to cover three complete calendar years. Calendar duration="
    "ceil(36x365.25/(12x7))=157 weeks. Prospective inference still uses the preregistered paired "
    "moving-block bootstrap; this floor does not assert monthly independence."
)


def _build(*, control: bool) -> PreregDeclaration:
    identity = (
        s8_control_identity(universe=UNIVERSE_BASIS, cost_model_id=COST_MODEL_ID)
        if control
        else mt1_identity(universe=UNIVERSE_BASIS, cost_model_id=COST_MODEL_ID)
    )
    expected = structural_promotion_refusals(
        universe_basis=UNIVERSE_BASIS,
        carry_unmodelled=CARRY_UNMODELLED,
        fx_unmodelled=FX_UNMODELLED,
    )
    return PreregDeclaration(
        strategy_id=identity.strategy_id,
        strategy_version=identity.version,
        contract_version=TRIAL_CONTRACT_VERSION,
        prereg_purpose="falsification_only" if control else "capital_candidate",
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis=UNIVERSE_BASIS,
        declared_carry_unmodelled=CARRY_UNMODELLED,
        declared_fx_unmodelled=FX_UNMODELLED,
        expected_structural_refusals=expected,
        forward_shadow=ForwardShadowFloor(
            min_independent_decision_dates=MIN_FORWARD_DECISION_DATES,
            min_calendar_weeks=MIN_FORWARD_CALENDAR_WEEKS,
            derivation=_FORWARD_SHADOW_DERIVATION,
        ),
        declared_by=DECLARED_BY,
    )


def build_mt1_declaration() -> PreregDeclaration:
    return _build(control=False)


def build_s8_control_declaration() -> PreregDeclaration:
    return _build(control=True)


def build_declarations() -> tuple[PreregDeclaration, PreregDeclaration]:
    return build_mt1_declaration(), build_s8_control_declaration()


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
