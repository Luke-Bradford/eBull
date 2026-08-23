"""#2342 — template staleness is decided from the template, not a shared file.

Pure-logic: the stamp reader and the clone-time refusal are exercised against a
stub cursor, so this module stays in the fast tier. The mechanism itself (a
DATABASE COMMENT surviving a rebuild and NOT being copied into a clone) was
verified against the 5433 cluster before adoption; see the PR.

⚠ Deliberately avoids the identifiers in ``tests/conftest.py::_DB_SOURCE_MARKERS``
— naming one of them here would auto-apply the ``db`` marker to the whole module
and evict it from the pre-push gate.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tests.fixtures.ebull_test_db import (
    TemplateWorktreeMismatch,
    _assert_template_matches_this_worktree,
    _migration_hash,
    _read_template_stamp,
)

SIBLING = "/Users/lukebradford/Dev/.ebull-ownership"


class _StubCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def __enter__(self) -> _StubCursor:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, *args: object, **kwargs: object) -> None:
        return None

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class _StubConn:
    """Just enough of a connection for the two functions under test."""

    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def cursor(self) -> _StubCursor:
        return _StubCursor(self._row)


def _stamped(migration_hash: str, built_from: str = SIBLING) -> _StubConn:
    payload = json.dumps({"migration_hash": migration_hash, "built_from": built_from})
    return _StubConn((payload,))


def test_a_well_formed_stamp_reads_back_as_hash_and_origin() -> None:
    conn = _stamped("abc123", built_from=SIBLING)
    assert _read_template_stamp(conn) == ("abc123", SIBLING)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "row",
    [
        None,  # no such database
        (None,),  # database exists, never stamped
        ("not json",),  # a comment we did not write
        ('{"migration_hash": "abc"}',),  # our shape, missing a key
        ("[]",),  # valid JSON, wrong type
    ],
    ids=["no-row", "unstamped", "not-json", "missing-key", "wrong-type"],
)
def test_an_unreadable_stamp_is_unknown_and_never_a_match(row: tuple[Any, ...] | None) -> None:
    # Reading "unknown" as "matches" is the failure this ticket is about: it
    # skips the rebuild and runs the suite against another checkout's schema.
    assert _read_template_stamp(_StubConn(row)) is None  # type: ignore[arg-type]


def test_a_matching_stamp_lets_the_clone_proceed() -> None:
    conn = _stamped(_migration_hash())
    assert _assert_template_matches_this_worktree(conn) is None  # type: ignore[arg-type]


def test_a_sibling_worktrees_stamp_refuses_the_clone_and_names_the_sibling() -> None:
    conn = _stamped("deadbeef" * 8, built_from=SIBLING)
    with pytest.raises(TemplateWorktreeMismatch) as excinfo:
        _assert_template_matches_this_worktree(conn)  # type: ignore[arg-type]
    assert SIBLING in str(excinfo.value)


def test_an_unstamped_template_refuses_rather_than_assuming_it_is_ours() -> None:
    with pytest.raises(TemplateWorktreeMismatch) as excinfo:
        _assert_template_matches_this_worktree(_StubConn((None,)))  # type: ignore[arg-type]
    assert "unknown checkout" in str(excinfo.value)


def test_the_mismatch_is_a_distinct_type_so_the_availability_probe_cannot_eat_it() -> None:
    """The probe catches ``Exception`` and turns it into a warning + skip.

    If this mismatch fell into that path the db tier would be silently skipped
    and the run would go green having exercised no database — #2342's defect one
    layer up. `test_db_available` re-raises this type by name, so it must stay a
    distinct class and must NOT be widened to a bare RuntimeError.
    """
    assert issubclass(TemplateWorktreeMismatch, RuntimeError)
    assert TemplateWorktreeMismatch is not RuntimeError

    source = (Path(__file__).resolve().parents[0] / "fixtures" / "ebull_test_db.py").read_text(encoding="utf-8")
    assert "except TemplateWorktreeMismatch:" in source, (
        "test_db_available must re-raise the mismatch instead of skipping"
    )
