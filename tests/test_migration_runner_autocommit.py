"""Unit test for the ``-- runner: autocommit`` directive (#1208 T0).

Pure parser test against ``_wants_autocommit``. The runner change in
``run_migrations`` is exercised end-to-end by the smoke gate
(``tests/smoke/test_app_boots.py``) which actually applies
``sql/155_postgres_runtime_tuning.sql`` against the dev DB during boot.
"""

from __future__ import annotations

import pytest

from app.db.migrations import (
    AUTOCOMMIT_DIRECTIVE,
    _split_autocommit_statements,
    _wants_autocommit,
)


class TestWantsAutocommit:
    def test_directive_on_line_1_is_detected(self) -> None:
        sql = f"{AUTOCOMMIT_DIRECTIVE}\nALTER SYSTEM SET work_mem = '32MB';\n"
        assert _wants_autocommit(sql) is True

    def test_directive_with_leading_blank_lines_is_detected(self) -> None:
        # lstrip() in the parser allows blank lines before the directive,
        # so an editor that auto-adds a trailing newline at file start is
        # still recognised.
        sql = f"\n\n{AUTOCOMMIT_DIRECTIVE}\nALTER SYSTEM SET work_mem = '32MB';\n"
        assert _wants_autocommit(sql) is True

    def test_directive_on_line_2_is_not_detected(self) -> None:
        # Strict line-1 check (Codex 1b LOW): loose parsing would
        # false-positive on a migration body that contains the directive
        # text.
        sql = f"-- 999: ordinary migration\n{AUTOCOMMIT_DIRECTIVE}\nCREATE TABLE foo (id INT);\n"
        assert _wants_autocommit(sql) is False

    def test_directive_inside_block_body_is_not_detected(self) -> None:
        sql = f"-- 998: comment-only header\nCREATE TABLE foo (\n    note TEXT DEFAULT '{AUTOCOMMIT_DIRECTIVE}'\n);\n"
        assert _wants_autocommit(sql) is False

    def test_empty_file_is_not_detected(self) -> None:
        assert _wants_autocommit("") is False
        assert _wants_autocommit("   \n\n  \n") is False

    def test_directive_must_match_exactly(self) -> None:
        # Trailing comment on the directive line should not be recognised
        # as the directive; the runner expects the canonical form.
        sql = f"{AUTOCOMMIT_DIRECTIVE} -- with trailing comment\nALTER SYSTEM SET work_mem = '32MB';\n"
        assert _wants_autocommit(sql) is False

    @pytest.mark.parametrize(
        "variant",
        [
            "--runner: autocommit",
            "-- runner:autocommit",
            "-- Runner: autocommit",
            "# runner: autocommit",
        ],
    )
    def test_near_variants_rejected(self, variant: str) -> None:
        sql = f"{variant}\nALTER SYSTEM SET work_mem = '32MB';\n"
        assert _wants_autocommit(sql) is False


class TestSplitAutocommitStatements:
    def test_splits_each_statement(self) -> None:
        sql = (
            "ALTER SYSTEM SET work_mem = '32MB';\nALTER SYSTEM SET min_wal_size = '512MB';\nSELECT pg_reload_conf();\n"
        )
        stmts = _split_autocommit_statements(sql)
        assert len(stmts) == 3
        assert stmts[0].startswith("ALTER SYSTEM SET work_mem")
        assert stmts[1].startswith("ALTER SYSTEM SET min_wal_size")
        assert stmts[2].startswith("SELECT pg_reload_conf")

    def test_skips_comment_only_chunks(self) -> None:
        # The migration-155 shape: comment header, blank line, statements.
        sql = (
            "-- runner: autocommit\n"
            "-- 155: Postgres runtime tuning for ...\n"
            "-- ALTER SYSTEM cannot run inside a tx block.\n"
            "\n"
            "ALTER SYSTEM SET max_wal_size = '4GB';\n"
            "SELECT pg_reload_conf();\n"
        )
        stmts = _split_autocommit_statements(sql)
        assert len(stmts) == 2
        assert "ALTER SYSTEM SET max_wal_size" in stmts[0]
        assert "pg_reload_conf" in stmts[1]

    def test_trailing_whitespace_no_extra_statement(self) -> None:
        sql = "SELECT 1;\n\n\n"
        assert _split_autocommit_statements(sql) == ["SELECT 1"]

    def test_inline_trailing_comment_dropped_with_next_fragment(self) -> None:
        # The splitter splits on ``;``. A statement with a trailing
        # inline comment splits into: <statement>; <comment-only chunk>.
        # The comment-only chunk is filtered out; the statement remains.
        sql = "ALTER SYSTEM SET shared_buffers = '2GB';            -- restart required\n"
        stmts = _split_autocommit_statements(sql)
        assert len(stmts) == 1
        assert stmts[0] == "ALTER SYSTEM SET shared_buffers = '2GB'"
