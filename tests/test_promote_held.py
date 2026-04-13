"""Tests for _promote_held_to_tier1 in scheduler."""

from __future__ import annotations

from unittest.mock import MagicMock

from app.workers.scheduler import _promote_held_to_tier1


class TestPromoteHeldToTier1:
    def test_promotes_held_instruments(self) -> None:
        conn = MagicMock()
        result = MagicMock()
        result.rowcount = 3
        conn.execute.return_value = result

        promoted = _promote_held_to_tier1(conn)

        assert promoted == 3
        sql = conn.execute.call_args[0][0]
        assert "UPDATE coverage" in sql
        assert "coverage_tier = 1" in sql
        assert "current_units > 0" in sql
        assert "coverage_tier != 1" in sql

    def test_zero_when_all_already_tier1(self) -> None:
        conn = MagicMock()
        result = MagicMock()
        result.rowcount = 0
        conn.execute.return_value = result

        promoted = _promote_held_to_tier1(conn)

        assert promoted == 0
