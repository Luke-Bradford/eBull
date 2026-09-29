"""#3471 slice 3d — the declaration freeze (spec §9 "Declaration" → "Freeze").

``freeze_trial`` writes, in ONE transaction: the #2599 preregistration row (read back and
asserted equal to the document's ``prereg`` terms), the ``ai_trial_declarations`` document,
both legs' position-manager backstop (``configure_trial_position_managers``) and the genesis
state event ``<none> → active``. The document is ``build_declaration``: a pure function of the
code plus the freeze-time provenance, so review of the code IS review of the terms.

The freeze chooses no capital, profile or policy number — those are the §8 pool decisions,
set beforehand through ``configure_deployment`` / ``configure_execution_policy``. It verifies
their STRUCTURE (both legs deployed, enabled, funded, USD, equal capital, O9 policy parity) and
binds the values the supervisor reviewed through ``config_sha256``.

A dry run executes the same path and rolls back. ⚠ ``apply=True`` STARTS the trial: both jobs
read ``active`` at their next fire. It is a supervisor go-live action (spec §8 wake condition),
never run by the loop.
"""

from __future__ import annotations

import hashlib
import math
import subprocess
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, cast

import psycopg
from psycopg import errors
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from app.services.ai_trial_deadline import TRIAL_MAX_POSITION_AGE_SECONDS
from app.services.ai_trial_decision import decision_json_schema
from app.services.ai_trial_executor import _POLICY_BOOKKEEPING, trial_policy_parity_refusal
from app.services.ai_trial_intent import DECLARATION_CONTRACT_PREFIX, declaration_digest
from app.services.ai_trial_invocation import TRIAL_MODEL_ID
from app.services.ai_trial_jobs import configure_trial_position_managers
from app.services.ai_trial_pack import canonical_sha256
from app.services.ai_trial_policy import AI_TRIAL_POLICY_HASH, PolicyManifest, policy_manifest
from app.services.ai_trial_prompt import PROMPT_TEMPLATE_SHA256, SYSTEM_PROMPT_SHA256
from app.services.ai_trial_readout import COHORT_SESSIONS, MIN_CLUSTERS
from app.services.ai_trial_run import TRIAL_ARM_STRATEGY_ID, TRIAL_STRATEGY_VERSION
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.result_ledger import PreregDeclarationRefused, freeze_preregistration
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION, structural_promotion_refusals
from app.services.trial_register import DeclaredTrial, TrialExactness

REPO_ROOT: Final = Path(__file__).resolve().parents[2]
SPEC_PATH: Final = "docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md"
DECLARATION_KIND: Final = "ai-trial-declaration-v1"
CONTROL_STRATEGY_ID: Final = TRIAL_ARM_STRATEGY_ID + "-control"
DRAW_RULE: Final = "ai_trial_decision.draw_control"
#: Frozen by NAME only (``ai_trial_policy``'s docstring): an edit there mints no version.
HOUSE_FUNCTIONS_BY_NAME: Final = ("indicator_series.atr_series", "market_calendar")

#: §9 "What this trial's statistics are": nothing here is capital-tier evidence.
PREREG_PURPOSE: Final = "falsification_only"
#: The trial's signals carry ``universe = survivor_only``.
UNIVERSE_BASIS: Final = "survivor_only"
#: Long x1 real stock in a USD-only lane (``BrokerStrategyOrder(settlement_type='real')``,
#: ``leverage=1``, ``trial_currency_not_usd``): the ``ranking_ablation_terms`` stamps.
CARRY_UNMODELLED: Final = False
FX_UNMODELLED: Final = False
DEPLOYMENT_CURRENCY: Final = "USD"

#: ``trial_register`` holds this entry verbatim; a test pins the two equal.
EXPECTED_REGISTER_ENTRY: Final = DeclaredTrial(
    trial_id="ai-discretionary-v1",
    description=(
        "#3471 AI-discretionary-v1: the model's daily long picks against a random-draw control leg "
        "on demo, one declared hypothesis (the arm-minus-control pair unit d, §9)."
    ),
    evidence="docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md §9 'Declaration' and "
    "'Freeze'; frozen by scripts/ai_trial_freeze.py (#3471)",
    exactness=TrialExactness.EXACT,
    searches=1,
    declared_for=(TRIAL_ARM_STRATEGY_ID, TRIAL_STRATEGY_VERSION),
)

#: A ``UniqueViolation`` on either of these is a concurrent freeze that won; anything else propagates.
_ROOT_CONSTRAINTS: Final = frozenset(
    {"strategy_preregistration_declaration_one_root", "ai_trial_declarations_one_per_version"}
)
#: The #2599 columns the document's ``prereg`` terms mirror, by column name.
_PREREG_COLUMNS: Final = (
    "prereg_purpose",
    "structural_refusal_policy_version",
    "declared_universe_basis",
    "declared_carry_unmodelled",
    "declared_fx_unmodelled",
    "expected_structural_refusals",
    "min_forward_decision_dates",
    "min_forward_calendar_weeks",
    "forward_shadow_derivation",
)


class TrialFreezeError(RuntimeError):
    """A post-write assertion failed; the transaction rolls back."""


def forward_shadow_floor() -> ForwardShadowFloor:
    return ForwardShadowFloor(
        min_independent_decision_dates=MIN_CLUSTERS,
        min_calendar_weeks=math.ceil(COHORT_SESSIONS / 5),
        derivation=(
            "BY CONSTRUCTION (precedent hunt_door.forward_shadow_floor): a falsification_only declaration "
            "cannot promote, and no power calculation fixes a promotion floor for a demo-tier decision aid. "
            "Dates = spec §9 'Too little data' cluster minimum (MIN_CLUSTERS; a cluster is a decision "
            "session); weeks = ceil(COHORT_SESSIONS / 5), a lower bound on the cohort's calendar span "
            "(holidays only lengthen it)."
        ),
    )


def prereg_terms() -> dict[str, object]:
    """The #2599 terms, keyed by column name, exactly as the row stores them."""
    floor = forward_shadow_floor()
    return {
        "prereg_purpose": PREREG_PURPOSE,
        "structural_refusal_policy_version": STRUCTURAL_REFUSAL_POLICY_VERSION,
        "declared_universe_basis": UNIVERSE_BASIS,
        "declared_carry_unmodelled": CARRY_UNMODELLED,
        "declared_fx_unmodelled": FX_UNMODELLED,
        "expected_structural_refusals": list(
            structural_promotion_refusals(
                universe_basis=UNIVERSE_BASIS, carry_unmodelled=CARRY_UNMODELLED, fx_unmodelled=FX_UNMODELLED
            )
        ),
        "min_forward_decision_dates": floor.min_independent_decision_dates,
        "min_forward_calendar_weeks": floor.min_calendar_weeks,
        "forward_shadow_derivation": floor.derivation,
    }


def build_declaration(
    *, manifest: PolicyManifest, code_git_sha: str, spec_sha256: str, python_version: str
) -> dict[str, Any]:
    """The frozen document (spec §9 "Freeze"). Every value is a JSON string, int, bool, list or
    object, so the JSONB round trip preserves ``declaration_digest``."""
    return {
        "kind": DECLARATION_KIND,
        "strategy_id": TRIAL_ARM_STRATEGY_ID,
        "strategy_version": TRIAL_STRATEGY_VERSION,
        "control_strategy_id": CONTROL_STRATEGY_ID,
        "policy_hash": manifest.digest(),
        "policy_modules": dict(manifest.module_sha256),
        "frozen_constants": dict(manifest.constant_repr),
        "model_id": TRIAL_MODEL_ID,
        "system_prompt_sha256": SYSTEM_PROMPT_SHA256,
        "prompt_template_sha256": PROMPT_TEMPLATE_SHA256,
        "decision_schema_sha256": canonical_sha256(decision_json_schema()),
        "draw_rule": DRAW_RULE,
        "house_functions_by_name": list(HOUSE_FUNCTIONS_BY_NAME),
        "prereg": prereg_terms(),
        "spec_path": SPEC_PATH,
        "spec_sha256": spec_sha256,
        "code_git_sha": code_git_sha,
        "python_version": python_version,
    }


def prereg_declaration(*, doc_sha256: str, declared_by: str) -> PreregDeclaration:
    return PreregDeclaration(
        strategy_id=TRIAL_ARM_STRATEGY_ID,
        strategy_version=TRIAL_STRATEGY_VERSION,
        contract_version=DECLARATION_CONTRACT_PREFIX + doc_sha256,
        prereg_purpose=PREREG_PURPOSE,
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis=UNIVERSE_BASIS,
        declared_carry_unmodelled=CARRY_UNMODELLED,
        declared_fx_unmodelled=FX_UNMODELLED,
        expected_structural_refusals=structural_promotion_refusals(
            universe_basis=UNIVERSE_BASIS, carry_unmodelled=CARRY_UNMODELLED, fx_unmodelled=FX_UNMODELLED
        ),
        forward_shadow=forward_shadow_floor(),
        declared_by=declared_by,
    )


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Provenance:
    code_git_sha: str
    spec_sha256: str
    python_version: str
    refusals: tuple[str, ...]


def read_provenance(repo_root: Path = REPO_ROOT, *, fetch: bool = True) -> Provenance:
    """``worktree_dirty`` / ``code_not_origin_main`` (HEAD must EQUAL a freshly fetched
    ``origin/main``: merged = reviewed, latest = no stale ancestor)."""

    def git(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=repo_root, check=True, capture_output=True, text=True).stdout.strip()

    if fetch:
        git("fetch", "--quiet", "origin", "main")
    head = git("rev-parse", "HEAD")
    refusals = []
    if git("status", "--porcelain"):
        refusals.append("worktree_dirty")
    if head != git("rev-parse", "origin/main"):
        refusals.append("code_not_origin_main")
    return Provenance(
        code_git_sha=head,
        spec_sha256=hashlib.sha256((repo_root / SPEC_PATH).read_bytes()).hexdigest(),
        python_version=sys.version.split()[0],
        refusals=tuple(refusals),
    )


# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------
_CONFIG_SQL: Final = """
    SELECT d.strategy_id, d.deployment_id, d.enabled, d.capital_limit::text, d.currency,
           (to_jsonb(p) - %s::text[]) AS policy
    FROM strategy_deployments d
    LEFT JOIN strategy_execution_policies p ON p.deployment_id = d.deployment_id
    WHERE d.strategy_id IN (%s, %s) AND d.strategy_version = %s AND d.mode = 'paper'
    ORDER BY d.strategy_id
    FOR SHARE OF d
"""


def read_config(conn: psycopg.Connection[Any]) -> dict[str, dict[str, Any]]:
    """Both legs' paper deployment and execution policy (bookkeeping excluded), locked to the
    caller's commit. ``strategy_deployments_unique`` makes each leg at most one row."""
    rows = conn.execute(
        _CONFIG_SQL,
        (_POLICY_BOOKKEEPING, TRIAL_ARM_STRATEGY_ID, CONTROL_STRATEGY_ID, TRIAL_STRATEGY_VERSION),
    ).fetchall()
    return {
        str(row[0]): {
            "deployment_id": int(row[1]),
            "enabled": bool(row[2]),
            "capital_limit": str(row[3]),
            "currency": str(row[4]),
            "policy": row[5],
        }
        for row in rows
    }


def config_refusals(config: dict[str, dict[str, Any]]) -> list[str]:
    """Structure only; the VALUES are the supervisor's §8 decision, bound by ``config_sha256``."""
    legs = [config.get(TRIAL_ARM_STRATEGY_ID), config.get(CONTROL_STRATEGY_ID)]
    if any(leg is None for leg in legs):
        return ["trial_deployment_missing"]
    present = [leg for leg in legs if leg is not None]
    refusals = []
    if not all(leg["enabled"] for leg in present):
        refusals.append("trial_deployment_disabled")
    if not all(Decimal(leg["capital_limit"]) > 0 for leg in present):
        refusals.append("trial_deployment_unfunded")
    if not all(leg["currency"] == DEPLOYMENT_CURRENCY for leg in present):
        refusals.append("trial_deployment_not_usd")
    if len({leg["capital_limit"] for leg in present}) != 1:
        refusals.append("trial_capital_parity")
    return refusals


# ---------------------------------------------------------------------------
# The freeze
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FreezeReport:
    applied: bool
    refusals: tuple[str, ...]
    doc: dict[str, Any]
    doc_sha256: str
    config: dict[str, dict[str, Any]]
    config_sha256: str | None
    declaration_id: int | None = None
    #: On ``already_frozen``: the stored document's sha (the lost-commit retry check).
    existing_doc_sha256: str | None = None


class _Rollback(Exception):
    pass


def _existing_doc_sha256(conn: psycopg.Connection[Any]) -> tuple[bool, str | None]:
    """Any #2599 row for the trial identity, and its document's sha if one was stored. An
    ``ai_trial_declarations`` row cannot exist without one (its FK and ``sql/432``'s bind
    trigger), so the #2599 row alone decides existence."""
    row = conn.execute(
        """
        SELECT p.declaration_id, a.doc_sha256
        FROM strategy_preregistration_declarations p
        LEFT JOIN ai_trial_declarations a ON a.declaration_id = p.declaration_id
        WHERE p.strategy_id = %s AND p.strategy_version = %s
        ORDER BY p.declaration_id DESC
        LIMIT 1
        """,
        (TRIAL_ARM_STRATEGY_ID, TRIAL_STRATEGY_VERSION),
    ).fetchone()
    return (row is not None, None if row is None or row[1] is None else str(row[1]))


def _write(
    conn: psycopg.Connection[Any],
    *,
    doc: dict[str, Any],
    doc_sha256: str,
    declared_by: str,
    wake_evidence: str,
) -> int:
    declaration_id = freeze_preregistration(
        cast(psycopg.Connection[tuple], conn), prereg_declaration(doc_sha256=doc_sha256, declared_by=declared_by)
    )
    stored = conn.execute(
        f"SELECT {', '.join(_PREREG_COLUMNS)} FROM strategy_preregistration_declarations WHERE declaration_id = %s",
        (declaration_id,),
    ).fetchone()
    assert stored is not None
    read_back = dict(zip(_PREREG_COLUMNS, stored, strict=True))
    read_back["expected_structural_refusals"] = list(read_back["expected_structural_refusals"])
    if read_back != doc["prereg"]:
        raise TrialFreezeError(f"#2599 row {declaration_id} does not store the document's prereg terms")
    conn.execute(
        "INSERT INTO ai_trial_declarations (declaration_id, strategy_id, strategy_version, doc_path, doc, doc_sha256) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (
            declaration_id,
            TRIAL_ARM_STRATEGY_ID,
            TRIAL_STRATEGY_VERSION,
            f"generated:ai_trial_freeze.build_declaration@{doc['code_git_sha']}",
            Jsonb(doc),
            doc_sha256,
        ),
    )
    configure_trial_position_managers(conn, updated_by=declared_by)
    managers = conn.execute(
        """
        SELECT m.max_position_age_seconds, m.ratchet_variant_id
        FROM strategy_position_manager_policies m
        JOIN strategy_deployments d ON d.deployment_id = m.deployment_id
        WHERE d.strategy_id IN (%s, %s) AND d.strategy_version = %s AND d.mode = 'paper'
        """,
        (TRIAL_ARM_STRATEGY_ID, CONTROL_STRATEGY_ID, TRIAL_STRATEGY_VERSION),
    ).fetchall()
    if sorted(tuple(row) for row in managers) != [(TRIAL_MAX_POSITION_AGE_SECONDS, None)] * 2:
        raise TrialFreezeError(f"position managers not at the §8 backstop: {managers}")
    conn.execute(
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, NULL, 'active', %s, 'supervisor')",
        (declaration_id, f"declaration frozen at {doc['code_git_sha']}; wake: {wake_evidence}"),
    )
    return declaration_id


def freeze_trial(
    conn: psycopg.Connection[Any],
    *,
    provenance: Provenance,
    apply: bool,
    declared_by: str = "",
    wake_evidence: str = "",
    expect_config_sha256: str | None = None,
) -> FreezeReport:
    """Run the freeze. ``apply=False`` executes the same path and rolls back (advisory: locks are
    released on rollback, and ``apply`` re-runs every check). Refusals are returned, not raised;
    a post-write assertion (``TrialFreezeError``) or an unexpected database error propagates
    after rolling back."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise TrialFreezeError("the freeze requires an idle connection")
    manifest = policy_manifest()
    doc = build_declaration(
        manifest=manifest,
        code_git_sha=provenance.code_git_sha,
        spec_sha256=provenance.spec_sha256,
        python_version=provenance.python_version,
    )
    doc_sha256 = declaration_digest(doc)
    refusals = list(provenance.refusals)
    if manifest.digest() != AI_TRIAL_POLICY_HASH:
        refusals.append("policy_hash_stale")
    if apply and not declared_by.strip():
        refusals.append("declared_by_missing")
    if apply and not wake_evidence.strip():
        refusals.append("wake_evidence_missing")
    config: dict[str, dict[str, Any]] = {}
    config_sha: str | None = None
    existing_sha: str | None = None
    declaration_id: int | None = None
    try:
        with conn.transaction():
            exists, existing_sha = _existing_doc_sha256(conn)
            if exists:
                refusals.append("already_frozen")
            config = read_config(conn)
            config_sha = canonical_sha256(config)
            refusals += config_refusals(config)
            parity = trial_policy_parity_refusal(
                conn, strategy_id=TRIAL_ARM_STRATEGY_ID, strategy_version=TRIAL_STRATEGY_VERSION
            )
            if parity is not None:
                refusals.append(parity)
            if apply and expect_config_sha256 is None:
                refusals.append("config_sha_unconfirmed")
            elif apply and expect_config_sha256 != config_sha:
                refusals.append("config_changed")
            if refusals:
                raise _Rollback
            try:
                declaration_id = _write(
                    conn,
                    doc=doc,
                    doc_sha256=doc_sha256,
                    declared_by=declared_by.strip() or "dry-run",
                    wake_evidence=wake_evidence.strip() or "dry-run",
                )
            except PreregDeclarationRefused as exc:
                refusals += [f"prereg_refused:{code}" for code in exc.refusals]
                raise _Rollback from exc
            if not apply:
                raise _Rollback
    except _Rollback:
        declaration_id = None
    except errors.UniqueViolation as exc:
        if exc.diag.constraint_name not in _ROOT_CONSTRAINTS:
            raise
        refusals.append("already_frozen")
        declaration_id = None
    applied = apply and not refusals and declaration_id is not None
    return FreezeReport(
        applied=applied,
        refusals=tuple(refusals),
        doc=doc,
        doc_sha256=doc_sha256,
        config=config,
        config_sha256=config_sha,
        declaration_id=declaration_id if applied else None,
        existing_doc_sha256=existing_sha,
    )


__all__ = [
    "DECLARATION_KIND",
    "EXPECTED_REGISTER_ENTRY",
    "FreezeReport",
    "Provenance",
    "TrialFreezeError",
    "build_declaration",
    "config_refusals",
    "forward_shadow_floor",
    "freeze_trial",
    "prereg_declaration",
    "prereg_terms",
    "read_config",
    "read_provenance",
]
