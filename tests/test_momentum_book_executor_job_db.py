from datetime import UTC, date, datetime
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


def _close_in_flight(conn: Conn, status: str) -> None:
    """The seeded held trade's close has been submitted: `closing`, or `reconcile_required` when its outcome was
    uncertain."""
    conn.execute("UPDATE strategy_trades SET status = %s WHERE status NOT IN ('closed', 'failed')", (status,))
    conn.commit()


@pytest.mark.parametrize("status", ["closing", "reconcile_required"])
def test_entries_wait_while_a_rebalance_exit_is_still_closing(ebull_test_conn: Conn, status: str) -> None:
    conn = ebull_test_conn
    _seed(conn)  # an enabled deployment and one held trade on the weakest name, which leaves the book
    _close_in_flight(conn, status)
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


def test_an_exit_that_could_not_be_submitted_does_not_block_entries(ebull_test_conn: Conn) -> None:
    """The seeded held trade has no owned broker position: the exit is reported `position_unowned`, logged, and the
    entries still run (review on #3828: an unsubmittable exit must not consume the month's entries)."""
    conn = ebull_test_conn
    _seed(conn)
    conn.execute("UPDATE strategy_trades SET status = 'open' WHERE status NOT IN ('closed', 'failed')")
    conn.commit()
    refresh = MagicMock()
    result = mj.run_momentum_book(conn, broker=MagicMock(), refresh_market=refresh, clock=lambda: NOW)
    assert result.exits == {"position_unowned": 1}
    assert result.exits_pending == 0
    refresh.assert_called_once()
    # Every entry was decided (here refused: the fixture deployment has no execution policy), none left waiting.
    assert result.entries > 0
    assert mj.due_entries(conn, today=NOW.date()) == []


def test_only_the_latest_formations_exits_are_retried(ebull_test_conn: Conn) -> None:
    """An older formation's exit is not retried once a newer formation exists: the newer book may hold that name."""
    conn = ebull_test_conn
    _seed(conn)
    conn.execute("UPDATE strategy_trades SET status = 'open' WHERE status NOT IN ('closed', 'failed')")
    conn.commit()
    first = mj.run_momentum_book(conn, broker=MagicMock(), refresh_market=MagicMock(), clock=lambda: NOW)
    assert first.exits == {"position_unowned": 1}
    # Positive case first, so the empty result below is not vacuous: the September formation's exit is due.
    assert len(conn.execute(mj._EXITS_DUE_SQL, {"today": date(2026, 11, 2), "version": "v1"}).fetchall()) == 1
    # October's formation (September's was formed on 2026-10-15, so the month is distinct).
    conn.execute(
        """
        INSERT INTO momentum_book_formations (strategy_version, formation_month, target_session, candidates, book, held)
        VALUES ('v1', '2026-10-01', '2026-11-02', 0, '[]'::jsonb, '[]'::jsonb)
        """
    )
    conn.commit()
    later = conn.execute(mj._EXITS_DUE_SQL, {"today": date(2026, 11, 2), "version": "v1"}).fetchall()
    assert later == []


def test_catch_up_entries_wait_while_an_exit_is_still_closing_overnight(ebull_test_conn: Conn) -> None:
    """Catch-up entries on a later session wait for their formation's exit still in flight (Codex ckpt-2): the
    pending-exit gate is keyed on today's entries, not on the formation's own session."""
    conn = ebull_test_conn
    _seed(conn)
    conn.execute("UPDATE strategy_trades SET status = 'open' WHERE status NOT IN ('closed', 'failed')")
    conn.commit()
    first = mj.run_momentum_book(conn, broker=MagicMock(), refresh_market=MagicMock(), clock=lambda: NOW)
    assert first.entries > 0  # every entry refused (the fixture deployment has no execution policy)
    _close_in_flight(conn, "closing")
    nxt = datetime(2026, 10, 16, 15, 30, tzinfo=UTC)
    refresh = MagicMock()
    result = mj.run_momentum_book(conn, broker=MagicMock(), refresh_market=refresh, clock=lambda: nxt)
    caught_up = mj.due_entries(conn, today=nxt.date())
    assert len(caught_up) == first.entries  # emitted, and left undecided
    assert (result.exits_pending, result.entries) == (1, 0)
    refresh.assert_not_called()
