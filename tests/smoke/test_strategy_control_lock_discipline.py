"""Structural guards for short strategy control-plane lock scopes."""

from __future__ import annotations

import inspect

from app.api.strategies import update_strategy_sizing


def test_sizing_overview_and_lookup_complete_before_control_lock() -> None:
    source = inspect.getsource(update_strategy_sizing)

    assert source.index("overview = get_strategy_overview(conn)") < source.index("lock_strategy_control(")
    assert "next((item for item in overview.strategies if item.strategy_id == strategy_id), None)" in source
    assert source.index("conn.rollback()") < source.index("lock_strategy_control(")
