from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services import momentum_book_executor_job as mj
from tests.test_momentum_book_job_db import _seed

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("marked_daily_loss_neutral")]

Conn = psycopg.Connection[Any]
NOW = datetime(2026, 10, 15, 15, 30, tzinfo=UTC)


def test_nothing_is_formed_before_activation(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)  # no momentum-30 deployment
    refresh = MagicMock()
    result = mj.run_momentum_book(conn, broker=MagicMock(), refresh_market=refresh, clock=lambda: NOW)
    assert (result.session_open, result.activated) == (True, False)
    assert result.note == "not_activated"
    assert conn.execute("SELECT count(*) FROM momentum_book_formations").fetchone() == (0,)
    refresh.assert_not_called()


def test_outside_the_entry_window_does_nothing(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    early = datetime(2026, 10, 15, 14, 0, tzinfo=UTC)
    result = mj.run_momentum_book(conn, broker=MagicMock(), refresh_market=MagicMock(), clock=lambda: early)
    assert not result.session_open
    assert conn.execute("SELECT count(*) FROM momentum_book_formations").fetchone() == (0,)


def test_entries_wait_while_a_rebalance_exit_is_still_closing(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)  # an enabled deployment and one held trade on the weakest name, which leaves the book
    refresh = MagicMock()
    result = mj.run_momentum_book(conn, broker=MagicMock(), refresh_market=refresh, clock=lambda: NOW)
    assert result.formation_id is not None
    assert result.exits_pending == 1
    assert result.entries == 0
    refresh.assert_not_called()
    assert "entries_wait_for_exits=1" in result.note
    # No entry was decided: nothing consumed while the exit's capital is still reserved.
    decided = conn.execute(
        """
        SELECT count(*) FROM strategy_funding_decisions fd
        JOIN momentum_book_entries me ON me.signal_id = fd.signal_id
        """
    ).fetchone()
    assert decided == (0,)
    assert mj.due_entries(conn, today=NOW.date()) != []
