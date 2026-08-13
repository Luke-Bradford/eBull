"""Tests for sole_operator_id() (issue #100).

Verifies the single-operator resolution contract: exactly one row
returns the UUID, zero rows raises NoOperatorError, multiple rows
raises AmbiguousOperatorError.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from app.services.operators import (
    AmbiguousOperatorError,
    NoOperatorError,
    sole_operator_id,
)


def _mock_conn(rows: list[tuple[object, ...]]) -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchall.return_value = rows
    conn.cursor.return_value = cur
    return conn


class TestSoleOperatorId:
    def test_one_operator_returns_uuid(self) -> None:
        op_id = uuid4()
        conn = _mock_conn([(op_id,)])
        assert sole_operator_id(conn) == op_id

    def test_zero_operators_raises(self) -> None:
        conn = _mock_conn([])
        with pytest.raises(NoOperatorError, match="no operator exists"):
            sole_operator_id(conn)

    def test_multiple_operators_raises(self) -> None:
        conn = _mock_conn([(uuid4(),), (uuid4(),)])
        with pytest.raises(AmbiguousOperatorError, match="expected exactly one"):
            sole_operator_id(conn)
