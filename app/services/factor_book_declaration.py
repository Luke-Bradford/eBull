"""The #3609 step 2 declaration check that every stage-B step runs before it reads anything.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Registration" (PR #3666). The trial is the
non-claiming ``DeclaredTrial`` ``3609-step2-book-v1`` (slice 4). ``record_holdout_access`` does not enforce a
register row for it, so the run checks its own code against the row: the spec sha256, the construction hash (the
builder, the report and every module they import except ``trial_register.py``), the register-policy hash and the
Python version, each named once in the row's ``evidence``. The construction hash is rooted at the builder, the
report's loader, its assembly and its entry point (``CONSTRUCTION_ROOTS``), so it reaches the verdict and every
diagnostic. The row's payload is pinned by a committed ``declared`` ledger row, so an edited row refuses on every
later attempt.

Until slice 4 merges there is no such row and no report, so every stage-B step refuses here.
"""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import json
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Final

from app.services.factor_panel_artefact import import_closure, sha256_file
from app.services.trial_register import DeclaredTrial, TrialExactness, TrialRegister

TRIAL_ID: Final = "3609-step2-book-v1"
DECLARED_EVENT: Final = "declared"

_REPO_ROOT: Final = Path(__file__).resolve().parents[2]
SPEC_PATH: Final = _REPO_ROOT / "docs" / "research" / "2026-10-06-3609-step2-factor-book.md"
BUILDER_PATH: Final = _REPO_ROOT / "scripts" / "build_3609_factor_panel.py"
REPORT_PATH: Final = _REPO_ROOT / "scripts" / "report_3609_step2.py"
ASSEMBLY_PATH: Final = _REPO_ROOT / "scripts" / "report_3609_step2_assembly.py"
RUN_PATH: Final = _REPO_ROOT / "scripts" / "report_3609_step2_run.py"
INPUTS_PATH: Final = _REPO_ROOT / "scripts" / "report_3609_step2_inputs.py"
#: The construction hash's roots. The loader (``REPORT_PATH``) imports none of the verdict, series or diagnostics
#: modules (they import it), so the assembly, which imports them all, is a root of its own, and so is the report's
#: entry point (``RUN_PATH``), which imports the assembly. The report's input verifiers (``INPUTS_PATH``: the
#: declaration's pins, step 0 and the factor snapshots) are a root until the entry point imports them.
CONSTRUCTION_ROOTS: Final = (BUILDER_PATH, REPORT_PATH, ASSEMBLY_PATH, RUN_PATH, INPUTS_PATH)
TRIAL_REGISTER_PATH: Final = "app/services/trial_register.py"
#: The two top-level assignments the register-policy hash leaves out: they hold the row that holds the hash.
_REGISTER_DATA: Final = frozenset({"TRIAL_REGISTER_VERSION", "TRIAL_REGISTER"})

#: The ``evidence`` labels this check reads, in the form ``label=value``, each exactly once.
SPEC_LABEL: Final = "spec_sha256"
CONSTRUCTION_LABEL: Final = "construction_sha256"
POLICY_LABEL: Final = "register_policy_sha256"
PYTHON_LABEL: Final = "python"


class DeclarationError(RuntimeError):
    """The run's code, spec or register does not match the frozen step 2 declaration."""


def _plain(value: Any) -> Any:
    if isinstance(value, Enum):
        return _plain(value.value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(v) for v in value]
    return value


def canonical_json(value: Any) -> bytes:
    """Spec §"Registration", "Canonical JSON": sorted keys, no whitespace, ASCII, no NaN; enums as values, dates as
    ISO strings, tuples as lists."""
    return json.dumps(_plain(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def payload_sha256(trial: DeclaredTrial) -> str:
    """sha256 of the register row's canonical JSON, the value the committed ``declared`` row pins."""
    return hashlib.sha256(canonical_json(dataclasses.asdict(trial))).hexdigest()


def register_policy_sha256(source: bytes) -> str:
    """sha256 of ``ast.unparse`` of the register module without its two data assignments.

    Pins every class, constant and function the register's validation uses, which the construction hash leaves
    out. Each data assignment must appear exactly once, so a renamed one cannot drop out of the policy silently.
    """
    module = ast.parse(source)
    kept: list[ast.stmt] = []
    removed: list[str] = []
    for node in module.body:
        target = node.target if isinstance(node, ast.AnnAssign) else None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
        if isinstance(target, ast.Name) and target.id in _REGISTER_DATA:
            removed.append(target.id)
        else:
            kept.append(node)
    if sorted(removed) != sorted(_REGISTER_DATA):
        raise DeclarationError(
            f"register data assignments found {sorted(removed)}, expected each of {sorted(_REGISTER_DATA)} once"
        )
    module.body = kept
    return hashlib.sha256(ast.unparse(module).encode()).hexdigest()


def construction_sha256(roots: Sequence[Path], repo: Path) -> str:
    """sha256 of the canonical JSON of the roots' import closure, the trial register excluded (it holds the hash)."""
    missing = [str(root) for root in roots if not root.is_file()]
    if missing:
        raise DeclarationError(f"construction source missing: {missing}")
    return hashlib.sha256(
        canonical_json(import_closure(list(roots), repo, unhashed=frozenset({TRIAL_REGISTER_PATH})))
    ).hexdigest()


@dataclass(frozen=True)
class CodeHashes:
    """What the running checkout is, in the four values the declaration's ``evidence`` names."""

    spec_sha256: str
    construction_sha256: str
    register_policy_sha256: str
    python: str

    @classmethod
    def current(cls) -> CodeHashes:
        if not SPEC_PATH.is_file():
            raise DeclarationError(f"the step 2 spec is not in this checkout: {SPEC_PATH}")
        return cls(
            spec_sha256=sha256_file(SPEC_PATH),
            construction_sha256=construction_sha256(CONSTRUCTION_ROOTS, _REPO_ROOT),
            register_policy_sha256=register_policy_sha256((_REPO_ROOT / TRIAL_REGISTER_PATH).read_bytes()),
            python=f"{sys.version_info.major}.{sys.version_info.minor}",
        )

    def by_label(self) -> dict[str, str]:
        return {
            SPEC_LABEL: self.spec_sha256,
            CONSTRUCTION_LABEL: self.construction_sha256,
            POLICY_LABEL: self.register_policy_sha256,
            PYTHON_LABEL: self.python,
        }


def evidence_value(evidence: str, label: str) -> str:
    """The value of ``label=value`` in ``evidence``; refuses unless the label appears exactly once."""
    found = re.findall(rf"(?:^|[;\s]){re.escape(label)}=([^;\s]+)", evidence)
    if len(found) != 1:
        raise DeclarationError(f"{TRIAL_ID} evidence names {label!r} {len(found)} times; exactly once is required")
    return found[0]


def check_declaration(
    register: TrialRegister, ledger_rows: Sequence[Mapping[str, Any]], current: CodeHashes
) -> DeclaredTrial:
    """The declared step 2 trial, once its row, its pinned payload and this checkout all agree; refuses otherwise."""
    matches = [trial for trial in register.trials if trial.trial_id == TRIAL_ID]
    if len(matches) != 1:
        raise DeclarationError(f"{len(matches)} register rows named {TRIAL_ID}; exactly one is required")
    (trial,) = matches
    if trial.declared_for is not None or trial.exactness is not TrialExactness.EXACT or trial.searches != 1:
        raise DeclarationError(f"{TRIAL_ID} is not a non-claiming exact one-search row")
    declared = [row for row in ledger_rows if row.get("event") == DECLARED_EVENT and row.get("trial_id") == TRIAL_ID]
    if len(declared) != 1:
        raise DeclarationError(f"{len(declared)} 'declared' ledger rows for {TRIAL_ID}; exactly one is required")
    if declared[0].get("payload_sha256") != payload_sha256(trial):
        raise DeclarationError(f"{TRIAL_ID}'s register row differs from the payload its 'declared' row pins")
    for label, value in current.by_label().items():
        if evidence_value(trial.evidence, label) != value:
            raise DeclarationError(f"this checkout's {label} differs from {TRIAL_ID}'s declared value")
    return trial


__all__ = [
    "CONSTRUCTION_LABEL",
    "CONSTRUCTION_ROOTS",
    "DECLARED_EVENT",
    "POLICY_LABEL",
    "PYTHON_LABEL",
    "SPEC_LABEL",
    "TRIAL_ID",
    "CodeHashes",
    "DeclarationError",
    "canonical_json",
    "check_declaration",
    "construction_sha256",
    "evidence_value",
    "payload_sha256",
    "register_policy_sha256",
]
