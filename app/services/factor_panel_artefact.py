"""#3609 step 1: the panel artefact's file mechanics and construction-version hashes.

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` §"Panel artefact, provenance and replay". The
publish protocol is #3360/#3361's: an exclusive directory, every input dumped into ``inputs/`` and read back only
from that copy, files written once and never replaced, the manifest last.

Gzip members are written with ``mtime=0`` and no file name, so the same content gives the same bytes under the
same zlib; replay compares the uncompressed content digest, which does not depend on zlib at all.
"""

from __future__ import annotations

import ast
import gzip
import hashlib
import io
import json
import os
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import IO, Any, Final

from app.services.factor_panel import PanelError

GZIP_LEVEL: Final = 6
#: Packages whose imports the closure follows: the app, and the scripts a report imports helpers from.
FOLLOWED_PACKAGES: Final = ("app", "scripts")


def sha256_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def gz_content_sha256(path: Path) -> str:
    """sha256 of a gzip file's uncompressed content."""
    with gzip.open(path, "rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


class GzLines:
    """A write-once, deterministic gzip text file of JSON lines; ``path`` must not exist."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._raw = path.open("xb")
        self._gz = gzip.GzipFile(filename="", mode="wb", fileobj=self._raw, compresslevel=GZIP_LEVEL, mtime=0)
        self._text: IO[str] = io.TextIOWrapper(self._gz, encoding="utf-8", newline="\n")
        self.count = 0

    def write(self, value: Any) -> None:
        self._text.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
        self.count += 1

    def close(self) -> None:
        self._text.close()  # closes the gzip member, which leaves ``_raw`` open
        self._raw.flush()
        os.fsync(self._raw.fileno())
        self._raw.close()

    def __enter__(self) -> GzLines:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def write_gz_lines(path: Path, values: Iterable[Any]) -> int:
    with GzLines(path) as out:
        for value in values:
            out.write(value)
        return out.count


def read_gz_lines(path: Path) -> Iterator[Any]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            yield json.loads(line)


def write_json_once(path: Path, value: Any) -> None:
    """``value`` as indented, key-sorted JSON; refuses to replace an existing file."""
    with path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _module_files(name: str, repo: Path) -> list[Path]:
    """The files importing ``name`` executes: each package ``__init__`` on its path, then the module itself.

    A name that is not a module (``app.x.y.SomeClass`` from ``from app.x.y import SomeClass``) contributes only
    its packages, which the module name itself also contributes.
    """
    parts = name.split(".")
    out: list[Path] = []
    for i in range(1, len(parts) + 1):
        base = repo.joinpath(*parts[:i])
        if (base / "__init__.py").is_file():
            out.append(base / "__init__.py")
        elif base.with_suffix(".py").is_file():
            out.append(base.with_suffix(".py"))
            break
        else:
            break
    return out


def import_closure(roots: Sequence[Path], repo: Path, *, unhashed: frozenset[str] = frozenset()) -> dict[str, str]:
    """Repo-relative path -> sha256 for each root and every ``app`` or ``scripts`` module it reaches by import,
    transitively.

    Static (every ``import`` statement anywhere in a file, function-level and ``TYPE_CHECKING`` included), so the
    set does not depend on which code path a run took.

    ``unhashed`` names repo-relative files whose content stays out of the result while their imports are still
    followed, so nothing reached only through them drops out. The #3609 trial register is the one use: it records
    these hashes, so hashing it into them would be a fixed point.
    """
    seen: dict[str, str] = {}
    visited: set[str] = set()
    stack = [path.resolve() for path in roots]
    repo = repo.resolve()
    while stack:
        path = stack.pop()
        relative = path.relative_to(repo).as_posix()
        if relative in visited:
            continue
        visited.add(relative)
        data = path.read_bytes()
        if relative not in unhashed:
            seen[relative] = hashlib.sha256(data).hexdigest()
        for node in ast.walk(ast.parse(data, filename=relative)):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    raise PanelError(f"relative import in {relative}: the closure resolves absolute imports only")
                base = node.module or ""
                names = [base, *(f"{base}.{alias.name}" for alias in node.names)]
            else:
                continue
            for name in names:
                if any(name == package or name.startswith(f"{package}.") for package in FOLLOWED_PACKAGES):
                    stack.extend(_module_files(name, repo))
    return dict(sorted(seen.items()))


def construction_versions(characteristics: Sequence[str], spec_sha256: str, sources: dict[str, str]) -> dict[str, str]:
    """One hash per characteristic over its name, the spec's sha256 and every construction source's sha256."""
    out: dict[str, str] = {}
    for name in characteristics:
        payload = {"characteristic": name, "spec_sha256": spec_sha256, "sources": sources}
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        out[name] = hashlib.sha256(encoded).hexdigest()
    return out
