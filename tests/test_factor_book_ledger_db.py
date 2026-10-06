"""#3609 step 2: the committed-access check reads the real access log."""

from __future__ import annotations

import psycopg
import pytest

from app.services.factor_book_ledger import (
    STRATEGY_ID,
    STRATEGY_VERSION,
    StageBAccessError,
    access_purpose,
    require_committed_access,
)
from app.services.result_ledger import HoldoutAccess, record_holdout_access
from tests.fixtures.ebull_test_db import test_database_url

pytestmark = pytest.mark.usefixtures("assume_trial_registered")

RUN = "c" * 32


def _record(conn: psycopg.Connection[tuple], **overrides: str) -> int:
    fields = {
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "access_kind": "evaluate",
        "accessed_by": "test",
        "purpose": access_purpose(RUN),
        "result_version": RUN,
        **overrides,
    }
    with conn.transaction():
        return record_holdout_access(conn, HoldoutAccess(**fields))  # type: ignore[arg-type]


def test_the_runs_evaluate_access_passes(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    access_id = _record(ebull_test_conn)
    require_committed_access(ebull_test_conn, RUN, access_id)


@pytest.mark.parametrize(
    "overrides",
    [
        {"strategy_id": "3609-step1-fidelity"},
        {"strategy_version": "v2"},
        {"result_version": "d" * 32},
        {"access_kind": "read"},
        {"purpose": "#3609 step 2 exploratory look"},
    ],
)
def test_an_access_for_anything_else_does_not_pass(
    ebull_test_conn: psycopg.Connection[tuple], overrides: dict[str, str]
) -> None:
    access_id = _record(ebull_test_conn, **overrides)
    with pytest.raises(StageBAccessError):
        require_committed_access(ebull_test_conn, RUN, access_id)


def test_an_unknown_access_id_does_not_pass(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    access_id = _record(ebull_test_conn)
    with pytest.raises(StageBAccessError):
        require_committed_access(ebull_test_conn, RUN, access_id + 1)


def test_an_uncommitted_access_is_invisible_to_the_gates_connection(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """The publisher checks on its own connection, so the row counts only once the run has committed it."""
    with psycopg.connect(test_database_url()) as other:
        with ebull_test_conn.transaction():
            access_id = record_holdout_access(
                ebull_test_conn,
                HoldoutAccess(
                    strategy_id=STRATEGY_ID,
                    strategy_version=STRATEGY_VERSION,
                    access_kind="evaluate",
                    accessed_by="test",
                    purpose=access_purpose(RUN),
                    result_version=RUN,
                ),
            )
            with pytest.raises(StageBAccessError):
                require_committed_access(other, RUN, access_id)
        require_committed_access(other, RUN, access_id)
