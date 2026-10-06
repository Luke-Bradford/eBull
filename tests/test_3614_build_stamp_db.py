"""#3614 item 2: sql/474's column defaults read the stamp a connection carries in PGOPTIONS.

One DB file for the one new SQL mechanism. The stamp travels the real path: the
environment variable, libpq's connect, the session setting, the column default.
"""

from __future__ import annotations

import psycopg
import pytest

from app.db.build_stamp import CODE_COMMIT_SETTING, CODE_DIRTY_SETTING, UV_LOCK_SHA256_SETTING, with_build_stamp
from tests.fixtures.ebull_test_db import test_database_url

SHA = "c" * 40
LOCK = "d" * 64
_STAMP_COLUMNS = "code_commit, code_dirty, uv_lock_sha256"


def _insert_decision(conn: psycopg.Connection[tuple]) -> tuple:
    row = conn.execute(
        f"""
        INSERT INTO decision_audit (stage, pass_fail, explanation)
        VALUES ('build_stamp_test', 'PASS', 'x')
        RETURNING {_STAMP_COLUMNS}
        """
    ).fetchone()
    assert row is not None
    return row


def test_stamped_connection_writes_its_build_identity(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    stamp = {CODE_COMMIT_SETTING: SHA, CODE_DIRTY_SETTING: "false", UV_LOCK_SHA256_SETTING: LOCK}
    monkeypatch.setenv("PGOPTIONS", with_build_stamp("", stamp))
    with psycopg.connect(test_database_url()) as stamped:
        assert _insert_decision(stamped) == (SHA, False, LOCK)
        instrument_id = 993_614
        stamped.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
            VALUES (%s, 'STMP', 'Stamp', TRUE)
            """,
            (instrument_id,),
        )
        order = stamped.execute(
            f"""
            INSERT INTO orders (instrument_id, action, order_type, status)
            VALUES (%s, 'BUY', 'MARKET', 'submitted')
            RETURNING {_STAMP_COLUMNS}
            """,
            (instrument_id,),
        ).fetchone()
        assert order == (SHA, False, LOCK)
        stamped.rollback()


def test_unstamped_connection_writes_null(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    # The fixture connection carries no stamp: NULL means "not recorded", never a guess.
    assert _insert_decision(ebull_test_conn) == (None, None, None)
