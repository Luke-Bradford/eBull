"""#3385 — a hunt signal module may import only a pure allowlist (spec "Signals").

A signal sees bars ≤ t through the view it is handed. Anything that could reach data by
another route — a DB driver, a reader, the filesystem, the network — or make the score
irreproducible (randomness) is refused at review time by this test. It is code-review
enforcement, not a sandbox (declared residual).
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Final

from app.services import hunt_harness

#: stdlib pure modules, numpy, and the package itself (the view type lands in slice 3).
_ALLOWED: Final[frozenset[str]] = frozenset(
    {"__future__", "math", "statistics", "dataclasses", "typing", "numpy", "app.services.hunt_signals"}
)


def _import_violations(source: str) -> list[str]:
    violations: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                names = ["app.services.hunt_signals"]
            else:
                names = [node.module or ""]
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"__import__", "open", "eval", "exec"}
        ):
            violations.append(f"calls {node.func.id}()")
            continue
        else:
            continue
        for name in names:
            if not any(name == allowed or name.startswith(f"{allowed}.") for allowed in _ALLOWED):
                violations.append(f"imports {name}")
    return violations


def test_every_signal_module_imports_only_the_allowlist() -> None:
    package = hunt_harness.SIGNAL_PACKAGE_DIR
    assert package.is_dir()
    found = {path.name: _import_violations(path.read_text()) for path in sorted(package.glob("*.py"))}
    assert "__init__.py" in found
    assert {name: bad for name, bad in found.items() if bad} == {}


def test_the_scanner_refuses_io_and_randomness() -> None:
    source = (
        "import math\nimport numpy as np\nfrom typing import Any\nimport random\nimport psycopg\n"
        "from app.services.research_split_corrected_reader import load_ratio_basis\n"
        "from pathlib import Path\nx = open('f')\n"
    )
    assert _import_violations(source) == [
        "imports random",
        "imports psycopg",
        "imports app.services.research_split_corrected_reader",
        "imports pathlib",
        "calls open()",
    ]


def test_the_scanner_admits_a_pure_signal() -> None:
    source = "from __future__ import annotations\nimport math\nimport numpy as np\nfrom . import view\n"
    assert _import_violations(source) == []


def test_the_package_path_is_the_one_the_code_hash_reads() -> None:
    assert Path(hunt_harness.__file__).resolve().parent / "hunt_signals" == hunt_harness.SIGNAL_PACKAGE_DIR
