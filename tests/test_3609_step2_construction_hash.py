"""#3609 step 2 construction hash: its import closure reaches every module the declared run executes.

Spec §"Registration": the construction hash covers "the report, the builder and every imported module". The
report's loader is imported by the verdict, series and diagnostics modules, never the reverse, so a closure rooted
at the loader alone would leave the verdict code out of the freeze.
"""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

from app.services.factor_book_declaration import CONSTRUCTION_ROOTS, TRIAL_REGISTER_PATH
from app.services.factor_panel_artefact import import_closure

REPO = Path(__file__).resolve().parents[1]


def test_the_construction_closure_reaches_every_step2_report_module_and_book_service() -> None:
    closure = import_closure(list(CONSTRUCTION_ROOTS), REPO, unhashed=frozenset({TRIAL_REGISTER_PATH}))
    modules = sorted(
        path.relative_to(REPO).as_posix()
        for pattern in ("scripts/report_3609_step2*.py", "app/services/factor_book*.py")
        for path in REPO.glob(pattern)
    )
    # Non-vacuity: a glob that matched nothing would make the next assertion pass trivially.
    assert "scripts/report_3609_step2_verdict.py" in modules
    missing = [module for module in modules if module not in closure]
    assert not missing, f"not in the construction hash: {missing}"


#: Imports the roots in a clean interpreter and prints the repo-relative file of every ``app``/``scripts`` module
#: loaded, dynamic imports included.
_LOADED = """
import importlib, json, sys
from pathlib import Path
repo = Path(sys.argv[1])
for root in sys.argv[2:]:
    importlib.import_module(root)
files = []
for module in list(sys.modules.values()):
    path = getattr(module, "__file__", None)
    resolved = Path(path).resolve() if path else None
    if resolved and resolved.is_relative_to(repo) and resolved.relative_to(repo).parts[0] in ("app", "scripts"):
        files.append(resolved.relative_to(repo).as_posix())
print(json.dumps(sorted(files)))
"""


def test_every_repo_module_the_roots_load_at_import_is_in_the_construction_closure() -> None:
    """Beyond the globs: what importing the roots actually loads (a module reached only by a dynamic import, or from
    outside the two name patterns) must be hashed too."""
    closure = import_closure(list(CONSTRUCTION_ROOTS), REPO, unhashed=frozenset({TRIAL_REGISTER_PATH}))
    roots = [root.relative_to(REPO).with_suffix("").as_posix().replace("/", ".") for root in CONSTRUCTION_ROOTS]
    out = subprocess.run(
        [sys.executable, "-c", _LOADED, str(REPO), *roots], cwd=REPO, capture_output=True, text=True, check=True
    )
    loaded = json.loads(out.stdout.strip().splitlines()[-1])
    assert "scripts/report_3609_step2_verdict.py" in loaded
    # The register is pinned by the register-policy hash instead (it holds the row that holds this hash).
    missing = [path for path in loaded if path not in closure and path != TRIAL_REGISTER_PATH]
    # The closure does not record package ``__init__`` files; that is safe only while each one holds no code.
    coded = [path for path in missing if not (path.endswith("__init__.py") and _docstring_only(REPO / path))]
    assert not coded, f"loaded by the run but not in the construction hash: {coded}"


def _docstring_only(path: Path) -> bool:
    body = ast.parse(path.read_bytes()).body
    return all(isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) for node in body)
