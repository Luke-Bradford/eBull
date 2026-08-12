"""#2616 — the re-run gate for the pre-cutoff sealed outcome openers.

Shared by ``scripts/verify_2476_pead_outcomes.py`` and
``scripts/verify_2480_insider_outcomes.py``; declarations are frozen by
``scripts/freeze_2616_precutoff_declarations.py``. Follows C-4's shape in
``scripts/evaluate_2582_schedule13d_outcomes.py`` (#2614).

⚠⚠ WHY A RE-RUN IS THE THING BEING GATED. Both scripts ran once, BEFORE
``TRIAL_REGISTER_CUTOFF``, and #2600's reconstruction charged those looks to the
register. The entries cover the looks already taken; they do not pre-pay for
future ones. An ungated re-run — a corrected window, a re-measurement, a
follow-up arm — would open the sealed population again, charge nothing, and
write no access row: a second look presented as the first, invisible to every
automated check, under-counting ``M`` in the direction that RAISES the Deflated
Sharpe.

⚠⚠ THE REGISTER CHECK IS THREE RULES, AND EACH CLOSES A DIFFERENT DODGE:

1. the named entry must NOT be the original pre-cutoff one — spent entries do
   not pre-pay;
2. it must carry the trial family's prefix — naming an UNRELATED existing
   register entry would satisfy a bare membership check while charging nothing
   new for this family's look;
3. it must be present in ``TRIAL_REGISTER`` — appending a ``DeclaredTrial`` is a
   reviewed PR that bumps the register version, so the charge precedes the look,
   which is the property C-4's gate established for post-cutoff trials.

Because the only current entry carrying each family prefix IS the original, the
three rules together are satisfiable only by an entry that did not exist when
this module shipped.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

from app.services.result_ledger import (
    HoldoutAccess,
    PreregDeclarationRefused,
    load_preregistration,
    require_outcome_access,
)
from app.services.trial_register import TRIAL_REGISTER, TrialRegister


class RerunGateRefusal(RuntimeError):
    """A pre-cutoff opener may not re-open its sealed population as asked."""


@dataclass(frozen=True)
class SealedTrialIdentity:
    """One pre-cutoff opener's frozen identity, declared in the script it gates."""

    strategy_id: str
    strategy_version: str
    #: The preregistration document IS this trial's contract; the digest is
    #: verified on every open, so a rewritten preregistration refuses (the same
    #: posture C-4 takes with its contract JSON).
    prereg_doc: Path
    prereg_sha256: str
    #: The register entry #2600 charged the pre-cutoff look to. Spent.
    original_trial_id: str
    #: Every future re-run entry must carry this prefix — see the module
    #: docstring's rule 2. The original id carries it too, by construction.
    rerun_trial_id_prefix: str
    accessed_by: str

    def __post_init__(self) -> None:
        if not self.original_trial_id.startswith(self.rerun_trial_id_prefix):
            raise ValueError(
                "rerun_trial_id_prefix must be a prefix of original_trial_id — otherwise the family rule "
                "and the spent-entry rule guard different namespaces and rule 2 closes nothing"
            )


@dataclass(frozen=True)
class RerunOutcomeGate:
    """What ``require_outcome_gate`` returns; feeds the provenance re-check."""

    declaration_id: int
    access_id: int
    rerun_trial_id: str
    prereg_digest: str


def verify_preregistration_document(identity: SealedTrialIdentity) -> str:
    """Refuse unless the preregistration document still has its frozen bytes."""

    digest = hashlib.sha256(identity.prereg_doc.read_bytes()).hexdigest()
    if digest != identity.prereg_sha256:
        raise RerunGateRefusal(
            f"frozen preregistration document {identity.prereg_doc} moved: {digest} — a rewritten "
            "preregistration is not a preregistration; a deliberate edit must update the frozen digest in review"
        )
    return digest


def require_rerun_trial_id(
    identity: SealedTrialIdentity, trial_id: str | None, register: TrialRegister = TRIAL_REGISTER
) -> str:
    """The three-rule register check from the module docstring. Pure."""

    if not trial_id:
        raise RerunGateRefusal(
            f"a re-run must name the register entry that charges it (--rerun-trial-id); the original "
            f"{identity.original_trial_id} entry covers only the pre-cutoff look already taken"
        )
    if trial_id == identity.original_trial_id:
        raise RerunGateRefusal(
            f"{identity.original_trial_id} charged the pre-cutoff look and does not pre-pay for this one; "
            "append a new DeclaredTrial to app/services/trial_register.py and name it"
        )
    if not trial_id.startswith(identity.rerun_trial_id_prefix):
        raise RerunGateRefusal(
            f"{trial_id} does not carry this trial family's prefix {identity.rerun_trial_id_prefix!r} — "
            "naming an unrelated register entry would charge nothing new for this look"
        )
    if trial_id not in register.trial_ids:
        raise RerunGateRefusal(
            f"{trial_id} is absent from {register.version}; declare the search in the trial register "
            "before reading outcomes"
        )
    return trial_id


def require_outcome_gate_preconditions(identity: SealedTrialIdentity, trial_id: str | None) -> str:
    """The checks that need no database, so a wrong invocation is refused for free.

    Same split C-4 makes: a refusal that costs a connection is a refusal
    somebody eventually routes around. Returns the verified document digest.
    """

    digest = verify_preregistration_document(identity)
    require_rerun_trial_id(identity, trial_id)
    return digest


def require_outcome_gate(
    conn: psycopg.Connection[Any], identity: SealedTrialIdentity, *, trial_id: str | None
) -> RerunOutcomeGate:
    """Refuse unless the document, the register entry and #2599's declaration all hold.

    ⚠ IT DOES NOT FREEZE THE DECLARATION, AND MUST NOT — a declaration frozen by
    the thing that opens the outcomes declares nothing (#2599 checkpoint 1).
    Freezing is ``scripts/freeze_2616_precutoff_declarations.py``, run separately
    and earlier; this refuses ``preregistration_not_frozen`` until it has been.

    ⚠ THE CALLER OWNS THE COMMIT. ``require_outcome_access`` writes the ``read``
    access row in this transaction and does not commit it. Commit before
    evaluating, so the look stays logged even if the evaluation dies — then
    re-check it with ``verify_outcome_access_provenance``, which also proves the
    commit actually happened.
    """

    digest = require_outcome_gate_preconditions(identity, trial_id)
    assert trial_id is not None  # require_rerun_trial_id refused None above
    frozen = load_preregistration(conn, identity.strategy_id, identity.strategy_version)
    if frozen is None:
        raise PreregDeclarationRefused(identity.strategy_id, identity.strategy_version, ("preregistration_not_frozen",))
    # ⚠ `read`, with a NULL result_version — same reasoning as C-4: these
    # scripts write no result row, so an `evaluate` would stand for a row that
    # never arrives. A `read` is what this is: the sealed side being looked at.
    access_id = require_outcome_access(
        conn,
        HoldoutAccess(
            strategy_id=identity.strategy_id,
            strategy_version=identity.strategy_version,
            result_version=None,
            access_kind="read",
            accessed_by=identity.accessed_by,
            purpose=f"re-open the sealed {identity.original_trial_id} population under register entry "
            f"{trial_id} (#2616)",
        ),
    )
    return RerunOutcomeGate(
        declaration_id=frozen.declaration_id, access_id=access_id, rerun_trial_id=trial_id, prereg_digest=digest
    )


__all__ = [
    "RerunGateRefusal",
    "RerunOutcomeGate",
    "SealedTrialIdentity",
    "require_outcome_gate",
    "require_outcome_gate_preconditions",
    "require_rerun_trial_id",
    "verify_preregistration_document",
]
