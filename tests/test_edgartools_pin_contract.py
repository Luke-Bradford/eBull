"""Edgartools version-pin contract test.

Pins the installed ``edgartools`` version against the per-source spec
claims. If a future ``uv sync`` ever bumps ``pyproject.toml:21`` past
the pinned version, this test fails loudly — operator + reviewer see
the drift immediately instead of discovering a parser-bug-class issue
in production (#932 Pydantic validation cliff precedent).

History: caught by API Contract lens BLOCKING in the final committee
(2026-05-24). The per-source spec files at ``docs/etl/sources/`` cite
edgartools-version-specific behaviour (e.g. ``sec_n_port.md:75``
references the 5.30.2 line-number cite); pin drift between
``pyproject.toml`` + spec wording without this gate = silent
regression.

The pinned version below MUST match ``pyproject.toml:21``. Bumping
either side without the other fails the test.
"""

from __future__ import annotations

import re
import tomllib
from importlib.metadata import version
from pathlib import Path

# Single source of truth for the expected pin.
# When intentionally bumping, update BOTH this constant + the
# ``edgartools==...`` line in pyproject.toml in the same commit.
_EXPECTED_EDGARTOOLS_VERSION = "5.30.2"

_PYPROJECT_PATH = Path(__file__).resolve().parent.parent / "pyproject.toml"


def test_pyproject_pins_edgartools_to_expected_version() -> None:
    """pyproject.toml ``edgartools==N.N.N`` matches the constant above.

    Detects the "someone bumped pyproject but forgot the spec files"
    drift class. Per-source specs at ``docs/etl/sources/sec_n_port.md``,
    ``sec_13f_hr.md`` etc cite version-specific behaviour grounded in
    5.30.2; bumping without spec audit = bug.
    """
    body = _PYPROJECT_PATH.read_text()
    data = tomllib.loads(body)
    deps = data.get("project", {}).get("dependencies", [])
    edgartools_pin = next(
        (d for d in deps if d.startswith("edgartools")),
        None,
    )
    assert edgartools_pin is not None, (
        "edgartools must appear in pyproject.toml [project] dependencies. Search pyproject.toml:[project].dependencies."
    )
    match = re.fullmatch(r"edgartools==(\d+\.\d+\.\d+)", edgartools_pin)
    assert match is not None, (
        f"edgartools pin must be exact ``edgartools==N.N.N``, got {edgartools_pin!r}. "
        f"Range pins (~=, >=) defeat the contract — bump intentionally + audit "
        f"per-source specs in lockstep."
    )
    actual = match.group(1)
    assert actual == _EXPECTED_EDGARTOOLS_VERSION, (
        f"edgartools version drifted: pyproject.toml has {actual}, "
        f"test pin expects {_EXPECTED_EDGARTOOLS_VERSION}. "
        f"Per-source specs at docs/etl/sources/sec_n_port.md + sec_13f_hr.md "
        f"+ sec_13dg.md cite version-specific behaviour grounded in the test's "
        f"expected version. Audit those specs + update _EXPECTED_EDGARTOOLS_VERSION "
        f"in lockstep with any pyproject bump."
    )


def test_installed_edgartools_matches_pin() -> None:
    """Runtime check: the actually-installed edgartools matches the pin.

    Catches the rare case where ``uv sync`` was skipped + a stale venv
    has a different version. The test would also catch a build that
    used a different version pin than the committed pyproject.
    """
    installed = version("edgartools")
    assert installed == _EXPECTED_EDGARTOOLS_VERSION, (
        f"Installed edgartools={installed} != pin {_EXPECTED_EDGARTOOLS_VERSION}. "
        f"Run `uv sync` to restore. If the bump is intentional, update "
        f"pyproject.toml + this test + per-source specs in lockstep."
    )
