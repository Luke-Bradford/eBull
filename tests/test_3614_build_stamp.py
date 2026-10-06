"""#3614 item 2: the build stamp that ``install_build_stamp`` puts in ``PGOPTIONS``."""

from __future__ import annotations

import pytest

from app.db import build_stamp
from app.db.build_stamp import (
    CODE_COMMIT_SETTING,
    CODE_DIRTY_SETTING,
    UV_LOCK_SHA256_SETTING,
    with_build_stamp,
)

SHA = "a" * 40


def test_appends_stamp_and_keeps_unrelated_options() -> None:
    out = with_build_stamp("-c statement_timeout=5000", {CODE_COMMIT_SETTING: SHA})
    assert out == f"-c statement_timeout=5000 -c ebull.code_commit={SHA}"


def test_replaces_an_inherited_stamp_rather_than_appending() -> None:
    inherited = with_build_stamp("", {CODE_COMMIT_SETTING: "b" * 40, CODE_DIRTY_SETTING: "true"})
    out = with_build_stamp(inherited, {CODE_COMMIT_SETTING: SHA})
    assert out == f"-c ebull.code_commit={SHA}"


def test_settings_read_from_the_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    answers = {("rev-parse", "HEAD"): SHA, ("status", "--porcelain", "--untracked-files=no"): ""}
    monkeypatch.setattr(build_stamp, "_git", lambda *args: answers[args])
    stamp = build_stamp.build_stamp_settings()
    assert stamp[CODE_COMMIT_SETTING] == SHA
    assert stamp[CODE_DIRTY_SETTING] == "false"
    assert len(stamp[UV_LOCK_SHA256_SETTING]) == 64


def test_unreadable_git_is_omitted_not_guessed(monkeypatch: pytest.MonkeyPatch) -> None:
    # A failed status is unknown, never "clean"; a non-hex HEAD never reaches PGOPTIONS.
    monkeypatch.setattr(build_stamp, "_git", lambda *args: "not a sha" if args[0] == "rev-parse" else None)
    stamp = build_stamp.build_stamp_settings()
    assert CODE_COMMIT_SETTING not in stamp
    assert CODE_DIRTY_SETTING not in stamp


def test_tracked_edit_is_dirty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(build_stamp, "_git", lambda *args: SHA if args[0] == "rev-parse" else " M app/x.py")
    assert build_stamp.build_stamp_settings()[CODE_DIRTY_SETTING] == "true"


def test_explicit_options_keep_the_inherited_stamp(monkeypatch: pytest.MonkeyPatch) -> None:
    # libpq ignores PGOPTIONS when ``options`` is passed, so connect_job must merge it.
    monkeypatch.setenv("PGOPTIONS", f"-c ebull.code_commit={SHA}")
    assert build_stamp.with_inherited_pgoptions("-c statement_timeout=5") == (
        f"-c ebull.code_commit={SHA} -c statement_timeout=5"
    )


def test_connect_job_passes_the_stamp_with_its_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.jobs import job_connection

    seen: dict[str, object] = {}
    monkeypatch.setenv("PGOPTIONS", f"-c ebull.code_commit={SHA}")
    monkeypatch.setattr(job_connection.psycopg, "connect", lambda *_a, **kw: seen.update(kw))
    token = job_connection.job_statement_timeout_ms.set(7)
    try:
        job_connection.connect_job()
    finally:
        job_connection.job_statement_timeout_ms.reset(token)
    assert seen["options"] == f"-c ebull.code_commit={SHA} -c statement_timeout=7"
