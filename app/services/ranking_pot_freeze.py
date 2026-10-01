"""Ranking-pot-v1 declaration freeze (#2842 slice 4a; spec §8 "Declaration").

``freeze_pot`` writes, in ONE transaction: the #2599 preregistration row (read back and asserted equal to the
document's ``prereg`` terms), the ``ranking_pot_declarations`` document and the genesis state event ``<none> →
shadow_only``. The document is ``build_declaration``: a pure function of the code, S₀ and the freeze-time
provenance, so review of the code IS review of the terms.

S₀ (§5.0) is the instrument ids of the latest ``v1.5-balanced`` scores run committed before the freeze. A scores run
is written in one transaction (``scoring.compute_rankings``), so the latest committed run is a whole run.

The freeze chooses no capital and touches no deployment: the pot's executed book is configured at activation
(slice 5, §7.3). ``shadow_only`` places nothing; the shadow and controls are computed.

A dry run executes the same path and rolls back. ``apply=True`` is the supervisor's step (spec §10, after the last
slice), never the loop's.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, cast

import psycopg
from psycopg import errors
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from app.services.ai_trial_freeze import (
    _PREREG_COLUMNS,
    CARRY_UNMODELLED,
    FX_UNMODELLED,
    PREREG_PURPOSE,
    UNIVERSE_BASIS,
    Provenance,
)
from app.services.ai_trial_pack import canonical_sha256
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.ranking_pot import STRATEGY_ID, N
from app.services.ranking_pot_policy import (
    FAMILY,
    HARM_ALPHA,
    LOOK_MONTHS,
    RANKING_POT_POLICY_HASH,
    STRATEGY_VERSION,
    declaration_alpha,
    per_look_alpha,
    policy_manifest_now,
)
from app.services.ranking_pot_sim import K_CONTROLS
from app.services.result_ledger import PreregDeclarationRefused, _lock_trial, freeze_preregistration
from app.services.scoring import _DEFAULT_MODEL_VERSION
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION, structural_promotion_refusals
from app.services.trial_register import DeclaredTrial, TrialExactness

SPEC_PATH: Final = "docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md"
DECLARATION_KIND: Final = "ranking-pot-declaration-v1"
CONTRACT_PREFIX: Final = DECLARATION_KIND + ":"
BUILDER: Final = "ranking_pot_freeze.build_declaration"
SEED_RULE: Final = "ranking_pot_sim.control_seed(declaration doc_sha256, first decided snapshot sha256)"
DRAW_RULE: Final = "ranking_pot_sim.draw_bijection"

#: ``trial_register`` holds this entry verbatim; a test pins the two equal.
EXPECTED_REGISTER_ENTRY: Final = DeclaredTrial(
    trial_id=STRATEGY_ID,
    description=(
        "#2842 ranking-pot-v1: the v1.5-balanced ranking held as a 25-name monthly demo book, against "
        "K = 9,999 order-reassignment controls; one declared hypothesis (the shadow's T, spec §9)."
    ),
    evidence="docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md §8 'Declaration'; frozen by "
    "scripts/ranking_pot_freeze.py (#2842)",
    exactness=TrialExactness.EXACT,
    searches=1,
    declared_for=(STRATEGY_ID, STRATEGY_VERSION),
)

#: A ``UniqueViolation`` on one of these is a concurrent freeze that won; anything else propagates.
_ROOT_CONSTRAINTS: Final = frozenset(
    {
        "strategy_preregistration_declaration_one_root",
        "ranking_pot_declarations_one_per_version",
        "ranking_pot_declarations_family_seq_unique",
    }
)


class PotFreezeError(RuntimeError):
    """A precondition or post-write assertion failed; the transaction rolls back."""


def forward_shadow_floor() -> ForwardShadowFloor:
    return ForwardShadowFloor(
        min_independent_decision_dates=12,
        min_calendar_weeks=52,
        derivation=(
            "BY CONSTRUCTION (precedent ai_trial_freeze.forward_shadow_floor): a falsification_only declaration "
            "cannot promote. Dates = the monthly rebalances up to the first look (spec §9.3: 12 months after the "
            "first rebalance's target session, one decision date per month); weeks = 52, that look's span."
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


def prereg_declaration(*, doc_sha256: str, declared_by: str) -> PreregDeclaration:
    return PreregDeclaration(
        strategy_id=STRATEGY_ID,
        strategy_version=STRATEGY_VERSION,
        contract_version=CONTRACT_PREFIX + doc_sha256,
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


@dataclass(frozen=True)
class S0:
    """§5.0 S₀: the scored ids of one ``v1.5-balanced`` run, ascending."""

    scored_at: datetime
    instrument_ids: tuple[int, ...]

    @property
    def sha256(self) -> str:
        return canonical_sha256(list(self.instrument_ids))


def build_declaration(
    *,
    s0: S0,
    family_seq: int,
    module_sha256: dict[str, str],
    constant_repr: dict[str, str],
    policy_hash: str,
    provenance: Provenance,
) -> dict[str, Any]:
    """The frozen document (§8). Every value is a JSON string, int, bool, list or object, so the JSONB round trip
    preserves its canonical sha256. Fractions are written as ``"p/q"`` strings."""
    return {
        "kind": DECLARATION_KIND,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "family": FAMILY,
        "family_seq": family_seq,
        "policy_hash": policy_hash,
        "policy_modules": dict(module_sha256),
        "frozen_constants": dict(constant_repr),
        "scores_model_version": _DEFAULT_MODEL_VERSION,
        "s0": {
            "scored_at": s0.scored_at.isoformat(),
            "count": len(s0.instrument_ids),
            "sha256": s0.sha256,
            "instrument_ids": list(s0.instrument_ids),
        },
        "terms": {
            "n": N,
            "k_controls": K_CONTROLS,
            "look_months": list(LOOK_MONTHS),
            "declaration_alpha": str(declaration_alpha(family_seq)),
            "per_look_alpha": str(per_look_alpha(family_seq)),
            "harm_alpha": str(HARM_ALPHA),
        },
        "seed_rule": SEED_RULE,
        "draw_rule": DRAW_RULE,
        "prereg": prereg_terms(),
        "spec_path": SPEC_PATH,
        "spec_sha256": provenance.spec_sha256,
        "code_git_sha": provenance.code_git_sha,
        "python_version": provenance.python_version,
    }


# ---------------------------------------------------------------------------
# Reads (inside the freeze transaction)
# ---------------------------------------------------------------------------
def read_s0(conn: psycopg.Connection[Any]) -> S0 | str:
    """The latest committed scores run of the house model, or a refusal code."""
    latest = conn.execute(
        "SELECT max(scored_at) FROM scores WHERE model_version = %s", (_DEFAULT_MODEL_VERSION,)
    ).fetchone()
    if latest is None or latest[0] is None:
        return "s0_no_scores_run"
    rows, distinct = cast(
        tuple[int, int],
        conn.execute(
            "SELECT count(*), count(DISTINCT instrument_id) FROM scores WHERE model_version = %s AND scored_at = %s",
            (_DEFAULT_MODEL_VERSION, latest[0]),
        ).fetchone(),
    )
    if rows != distinct:
        return "s0_duplicate_rows"
    ids = conn.execute(
        "SELECT instrument_id FROM scores WHERE model_version = %s AND scored_at = %s ORDER BY instrument_id",
        (_DEFAULT_MODEL_VERSION, latest[0]),
    ).fetchall()
    return S0(scored_at=latest[0], instrument_ids=tuple(int(r[0]) for r in ids))


def _next_family_seq(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute(
        "SELECT coalesce(max(family_seq), 0) + 1 FROM ranking_pot_declarations WHERE family = %s", (FAMILY,)
    ).fetchone()
    assert row is not None
    return int(row[0])


def _existing_doc_sha256(conn: psycopg.Connection[Any]) -> tuple[bool, str | None]:
    """Any #2599 row for the pot identity, and its document's sha if one was stored."""
    row = conn.execute(
        """
        SELECT p.declaration_id, d.doc_sha256
        FROM strategy_preregistration_declarations p
        LEFT JOIN ranking_pot_declarations d ON d.declaration_id = p.declaration_id
        WHERE p.strategy_id = %s AND p.strategy_version = %s
        ORDER BY p.declaration_id DESC
        LIMIT 1
        """,
        (STRATEGY_ID, STRATEGY_VERSION),
    ).fetchone()
    return (row is not None, None if row is None or row[1] is None else str(row[1]))


# ---------------------------------------------------------------------------
# The freeze
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FreezeReport:
    applied: bool
    refusals: tuple[str, ...]
    doc: dict[str, Any] | None
    doc_sha256: str | None
    declaration_id: int | None = None
    #: On ``already_frozen``: the stored document's sha (the lost-commit retry check).
    existing_doc_sha256: str | None = None


class _Rollback(Exception):
    pass


def _write(conn: psycopg.Connection[Any], *, doc: dict[str, Any], doc_sha256: str, declared_by: str) -> int:
    declaration_id = freeze_preregistration(
        cast(psycopg.Connection[tuple], conn), prereg_declaration(doc_sha256=doc_sha256, declared_by=declared_by)
    )
    stored = conn.execute(
        f"SELECT {', '.join(_PREREG_COLUMNS)} FROM strategy_preregistration_declarations WHERE declaration_id = %s",
        (declaration_id,),
    ).fetchone()
    if stored is None:
        raise PotFreezeError(f"#2599 row {declaration_id} not readable after the freeze")
    read_back = dict(zip(_PREREG_COLUMNS, stored, strict=True))
    read_back["expected_structural_refusals"] = list(read_back["expected_structural_refusals"])
    if read_back != doc["prereg"]:
        raise PotFreezeError(f"#2599 row {declaration_id} does not store the document's prereg terms")
    conn.execute(
        "INSERT INTO ranking_pot_declarations "
        "(declaration_id, strategy_id, strategy_version, family, family_seq, doc_path, doc, doc_sha256) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            declaration_id,
            STRATEGY_ID,
            STRATEGY_VERSION,
            FAMILY,
            doc["family_seq"],
            f"generated:{BUILDER}@{doc['code_git_sha']}",
            Jsonb(doc),
            doc_sha256,
        ),
    )
    stored_doc = conn.execute(
        "SELECT doc FROM ranking_pot_declarations WHERE declaration_id = %s", (declaration_id,)
    ).fetchone()
    if stored_doc is None or canonical_sha256(stored_doc[0]) != doc_sha256:
        raise PotFreezeError("the stored document does not round-trip to its sha256")
    conn.execute(
        "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, NULL, 'shadow_only', %s, 'supervisor')",
        (declaration_id, f"declaration frozen at {doc['code_git_sha']} by {declared_by}"),
    )
    return declaration_id


def freeze_pot(
    conn: psycopg.Connection[Any],
    *,
    provenance: Provenance,
    apply: bool,
    declared_by: str = "",
) -> FreezeReport:
    """Run the freeze. ``apply=False`` executes the same path and rolls back. Refusals are returned, not raised;
    a post-write assertion (``PotFreezeError``) or an unexpected database error propagates after rolling back."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise PotFreezeError("the freeze requires an idle connection")
    manifest = policy_manifest_now()
    refusals = list(provenance.refusals)
    # Recomputed from disk now against the hash computed at import: an edit since import is stale.
    if manifest.digest() != RANKING_POT_POLICY_HASH:
        refusals.append("policy_hash_stale")
    if apply and not declared_by.strip():
        refusals.append("declared_by_missing")
    doc: dict[str, Any] | None = None
    doc_sha256: str | None = None
    existing_sha: str | None = None
    declaration_id: int | None = None
    try:
        with conn.transaction():
            _lock_trial(cast(psycopg.Connection[tuple], conn), STRATEGY_ID, STRATEGY_VERSION)
            exists, existing_sha = _existing_doc_sha256(conn)
            if exists:
                refusals.append("already_frozen")
            s0 = read_s0(conn)
            if isinstance(s0, str):
                refusals.append(s0)
            elif not s0.instrument_ids:
                refusals.append("s0_empty")
            else:
                doc = build_declaration(
                    s0=s0,
                    family_seq=_next_family_seq(conn),
                    module_sha256=manifest.module_sha256,
                    constant_repr=manifest.constant_repr,
                    policy_hash=manifest.digest(),
                    provenance=provenance,
                )
                doc_sha256 = canonical_sha256(doc)
            if refusals or doc is None or doc_sha256 is None:
                raise _Rollback
            try:
                declaration_id = _write(
                    conn, doc=doc, doc_sha256=doc_sha256, declared_by=declared_by.strip() or "dry-run"
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
        declaration_id=declaration_id if applied else None,
        existing_doc_sha256=existing_sha,
    )


__all__ = [
    "CONTRACT_PREFIX",
    "DECLARATION_KIND",
    "EXPECTED_REGISTER_ENTRY",
    "SPEC_PATH",
    "S0",
    "FreezeReport",
    "PotFreezeError",
    "build_declaration",
    "forward_shadow_floor",
    "freeze_pot",
    "prereg_declaration",
    "prereg_terms",
    "read_s0",
]
