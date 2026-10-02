"""#3546 gap B: one failing stage or item never skips the rest of a paper cycle.

Pure: the connection and every stage are mocked, so what is under test is the cycle's
containment and its entry-withholding rule, not the stages themselves (their DB tests live in
``test_strategy_paper_runtime.py``).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from psycopg.pq import TransactionStatus

from app.services import strategy_paper_runtime as runtime
from app.services.strategy_position_manager import PositionManagerResult

_NOW = datetime(2026, 10, 2, 15, 0, tzinfo=UTC)
_RUNTIME = "app.services.strategy_paper_runtime"


def _conn(owned: list[tuple[int, int]]) -> MagicMock:
    conn = MagicMock()
    conn.info.transaction_status = TransactionStatus.IDLE
    conn.execute.return_value.fetchall.return_value = owned
    return conn


def _managed(trade_id: int, state: str = "no_change") -> PositionManagerResult:
    return PositionManagerResult(trade_id, trade_id * 10, state, "ok")  # type: ignore[arg-type]


def _member(signal_id: int) -> SimpleNamespace:
    return SimpleNamespace(ranking_member_id=signal_id * 100, opportunity=SimpleNamespace(signal_id=signal_id))


@pytest.fixture
def stages() -> Iterator[dict[str, MagicMock]]:
    mocks = {
        "reconcile_backlog": MagicMock(return_value=(object(), object())),
        "refresh_strategy_health": MagicMock(return_value=0),
        "manage_owned_position": MagicMock(side_effect=lambda _c, **kw: _managed(kw["strategy_trade_id"])),
        "_load_ranked_opportunities": MagicMock(return_value=[]),
        "persist_ranking_batch": MagicMock(return_value=[_member(1), _member(2)]),
        "execute_fired_paper_signal": MagicMock(),
    }
    with patch.multiple(_RUNTIME, **mocks):  # type: ignore[arg-type]
        yield mocks


def _run(conn: MagicMock, **kwargs: Any) -> runtime.StrategyPaperCycleResult:
    return runtime.run_strategy_paper_cycle(conn, broker=MagicMock(), now=_NOW, strategy_versions=["v1"], **kwargs)


def test_a_raising_position_does_not_skip_the_positions_after_it(stages: dict[str, MagicMock]) -> None:
    def manage(_conn: object, **kw: Any) -> PositionManagerResult:
        if kw["strategy_trade_id"] == 1:
            raise RuntimeError("unmodelled broker response")
        return _managed(kw["strategy_trade_id"], "applied")

    stages["manage_owned_position"].side_effect = manage
    result = _run(_conn([(1, 10), (2, 20), (3, 30)]))

    assert [c.kwargs["strategy_trade_id"] for c in stages["manage_owned_position"].call_args_list] == [1, 2, 3]
    assert result.managed_positions == 2
    assert result.management == {"applied": 2}
    assert result.errors == {"manage": 1}
    # A management failure is not an entry input: entries still run.
    assert result.evaluated_signals == 2


def test_a_raising_signal_does_not_skip_the_signals_after_it(stages: dict[str, MagicMock]) -> None:
    stages["execute_fired_paper_signal"].side_effect = [RuntimeError("poison"), MagicMock()]
    result = _run(_conn([]))

    assert [c.kwargs["signal_id"] for c in stages["execute_fired_paper_signal"].call_args_list] == [1, 2]
    assert result.evaluated_signals == 1
    assert result.errors == {"execute": 1}


@pytest.mark.parametrize("stage", ["reconcile_backlog", "refresh_strategy_health"])
def test_a_failed_entry_input_withholds_entries_but_never_management(stages: dict[str, MagicMock], stage: str) -> None:
    stages[stage].side_effect = RuntimeError("down")
    result = _run(_conn([(1, 10)]))

    assert result.managed_positions == 1
    stages["persist_ranking_batch"].assert_not_called()
    stages["execute_fired_paper_signal"].assert_not_called()
    assert result.evaluated_signals == 0
    assert result.errors == {"reconcile" if stage == "reconcile_backlog" else "health": 1}
    if stage == "refresh_strategy_health":
        assert result.active_health_blocks is None
    else:
        assert result.active_health_blocks == 0


def test_a_ranking_failure_fails_closed_and_keeps_the_rest(stages: dict[str, MagicMock]) -> None:
    stages["_load_ranked_opportunities"].side_effect = RuntimeError("duplicate economic identity")
    result = _run(_conn([(1, 10)]))

    stages["execute_fired_paper_signal"].assert_not_called()
    assert (result.managed_positions, result.evaluated_signals) == (1, 0)
    assert result.errors == {"ranking": 1}


def test_a_contained_failure_leaves_the_connection_idle(stages: dict[str, MagicMock]) -> None:
    conn = _conn([(1, 10), (2, 20)])

    def manage(_conn: object, **kw: Any) -> PositionManagerResult:
        if kw["strategy_trade_id"] == 1:
            conn.info.transaction_status = TransactionStatus.INERROR
            raise RuntimeError("aborted transaction")
        return _managed(kw["strategy_trade_id"])

    conn.rollback.side_effect = lambda: setattr(conn.info, "transaction_status", TransactionStatus.IDLE)
    stages["manage_owned_position"].side_effect = manage
    result = _run(conn)

    conn.rollback.assert_called_once()
    assert result.management == {"no_change": 1}


def test_an_unpinned_cycle_gives_each_item_a_fresh_instant(stages: dict[str, MagicMock]) -> None:
    instants = iter([_NOW, datetime(2026, 10, 2, 15, 0, 7, tzinfo=UTC), datetime(2026, 10, 2, 15, 0, 9, tzinfo=UTC)])

    class _Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:  # type: ignore[override]
            return next(instants)

    with patch(f"{_RUNTIME}.datetime", _Clock):
        runtime.run_strategy_paper_cycle(_conn([]), broker=MagicMock(), strategy_versions=["v1"])

    sent = [c.kwargs["now"] for c in stages["execute_fired_paper_signal"].call_args_list]
    assert sent == [datetime(2026, 10, 2, 15, 0, 7, tzinfo=UTC), datetime(2026, 10, 2, 15, 0, 9, tzinfo=UTC)]
