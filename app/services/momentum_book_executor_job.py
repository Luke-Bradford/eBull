"""Momentum book (#3822) — the hourly job body: form the month's book, close what left it, then submit entries.

Each fire on a NYSE session from ``ENTRY_TIME_UTC``, once the ``momentum-30`` paper deployment is enabled:

1. ``momentum_book_job.form_book`` forms month t's book if it is due (idempotent).
2. **Exits first.** Every held trade the formation ordered out is closed through the position manager
   (``manage_owned_position(close_reason="strategy_exit")``), which owns the broker close and its reconciliation.
3. **Entries** wait until every exit of the formation has closed (a closing trade still reserves its capital), then
   each undecided entry signal goes through ``momentum_book_executor.execute_momentum_signal``.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerProvider
from app.services import momentum_book as mb
from app.services.momentum_book_executor import execute_momentum_signal
from app.services.momentum_book_job import form_book
from app.services.strategy_paper_executor import _session_is_open
from app.services.strategy_position_manager import manage_owned_position

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

_NEW_YORK: Final = ZoneInfo("America/New_York")
#: Exits and entries from 15:00 UTC: inside the regular session in both EDT and EST.
ENTRY_TIME_UTC: Final = time(15, 0)

_DUE_SQL: Final = """
    SELECT me.signal_id
    FROM momentum_book_entries me
    JOIN momentum_book_formations f ON f.formation_id = me.formation_id
    WHERE f.target_session = %s
      AND NOT EXISTS (SELECT 1 FROM strategy_funding_decisions fd WHERE fd.signal_id = me.signal_id)
    ORDER BY me.formation_id, me.rank
"""

_EXITS_DUE_SQL: Final = """
    SELECT x.strategy_trade_id, own.broker_position_id
    FROM momentum_book_exits x
    JOIN momentum_book_formations f ON f.formation_id = x.formation_id
    JOIN strategy_trades t ON t.strategy_trade_id = x.strategy_trade_id
    LEFT JOIN strategy_position_ownership own
      ON own.strategy_trade_id = x.strategy_trade_id AND own.status = 'active'
    WHERE f.target_session <= %s AND t.status = 'open'
    ORDER BY x.strategy_trade_id
"""


_ACTIVATED_SQL: Final = """
    SELECT 1 FROM strategy_deployments
    WHERE strategy_id = %s AND strategy_version = %s AND mode = 'paper' AND enabled
"""

# Exits of the formation targeting today whose close is IN FLIGHT. Only a submitted close (`closing`) waits: an exit
# that could not be submitted (no owned position, or a broker refusal that leaves the trade `open`) would otherwise
# block every entry of the session, and the session's entries expire with it (review on #3828). Those are logged.
_EXITS_PENDING_SQL: Final = """
    SELECT count(*)
    FROM momentum_book_exits x
    JOIN momentum_book_formations f ON f.formation_id = x.formation_id
    JOIN strategy_trades t ON t.strategy_trade_id = x.strategy_trade_id
    WHERE f.target_session = %s AND t.status = 'closing'
"""


def due_entries(conn: Conn, *, today: date) -> list[int]:
    """Undecided entry signals of the formation targeting ``today``, best rank first."""
    return [int(r[0]) for r in conn.execute(_DUE_SQL, (today,)).fetchall()]


@dataclass(frozen=True)
class MomentumJobResult:
    session_open: bool
    formation_id: int | None = None
    exits: Mapping[str, int] = field(default_factory=dict)
    verdicts: Mapping[str, int] = field(default_factory=dict)
    errors: int = 0
    activated: bool = True
    #: Exits still closing; entries wait for them.
    exits_pending: int = 0

    @property
    def entries(self) -> int:
        return sum(self.verdicts.values())

    @property
    def note(self) -> str:
        if not self.session_open:
            return "outside_entry_window"
        if not self.activated:
            return "not_activated"
        ex = " ".join(f"exit_{k}={v}" for k, v in sorted(self.exits.items()))
        en = " ".join(f"{k}={v}" for k, v in sorted(self.verdicts.items()))
        err = f" errors={self.errors}" if self.errors else ""
        wait = f" entries_wait_for_exits={self.exits_pending}" if self.exits_pending else ""
        return f"formation={self.formation_id} {ex} entries={self.entries} {en}".strip() + wait + err


def run_momentum_book(
    conn: Conn,
    *,
    broker: BrokerProvider,
    refresh_market: Callable[[], object],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> MomentumJobResult:
    observed = clock().astimezone(UTC)
    if not _session_is_open(observed) or observed.time() < ENTRY_TIME_UTC:
        return MomentumJobResult(session_open=False)
    today = observed.astimezone(_NEW_YORK).date()
    # Nothing is formed before activation: a formation is immutable, and its entries would all refuse
    # `paper_deployment_missing` and be consumed for the month (Codex ckpt-2 P1).
    activated = conn.execute(_ACTIVATED_SQL, (mb.STRATEGY_ID, mb.STRATEGY_VERSION)).fetchone()
    conn.commit()
    if activated is None:
        return MomentumJobResult(session_open=True, activated=False)
    formation = form_book(conn, session=today)
    conn.commit()

    exits: Counter[str] = Counter()
    errors = 0
    for trade_id, position_id in conn.execute(_EXITS_DUE_SQL, (today,)).fetchall():
        conn.commit()
        if position_id is None:
            logger.warning("momentum book: exit of trade %s has no owned broker position; not closed", trade_id)
            exits["position_unowned"] += 1
            continue
        try:
            outcome = manage_owned_position(
                conn,
                broker=broker,
                strategy_trade_id=int(trade_id),
                broker_position_id=int(position_id),
                close_reason="strategy_exit",
                now=clock(),
            )
        except Exception:
            logger.exception("momentum book: closing trade %s raised; continuing", trade_id)
            if conn.info.transaction_status != TransactionStatus.IDLE:
                conn.rollback()
            errors += 1
            continue
        exits[outcome.state] += 1
    conn.commit()

    verdicts: Counter[str] = Counter()
    # Entries wait for every exit this formation ordered to close: a closing trade still reserves its capital,
    # so an entry decided now would refuse for headroom and be consumed (Codex ckpt-2 P1). A later fire retries.
    pending = conn.execute(_EXITS_PENDING_SQL, (today,)).fetchone()
    conn.commit()
    if pending is not None and int(pending[0]) > 0:
        return MomentumJobResult(
            session_open=True,
            formation_id=None if formation is None else formation.formation_id,
            exits=dict(exits),
            errors=errors,
            exits_pending=int(pending[0]),
        )
    signal_ids = due_entries(conn, today=today)
    conn.commit()
    if signal_ids:
        refresh_market()
    for signal_id in signal_ids:
        try:
            result = execute_momentum_signal(conn, broker=broker, signal_id=signal_id, now=clock())
        except Exception:
            logger.exception("momentum book: entry signal %s raised; continuing", signal_id)
            if conn.info.transaction_status != TransactionStatus.IDLE:
                conn.rollback()
            errors += 1
            continue
        verdicts[result.verdict] += 1
    return MomentumJobResult(
        session_open=True,
        formation_id=None if formation is None else formation.formation_id,
        exits=dict(exits),
        verdicts=dict(verdicts),
        errors=errors,
    )


__all__ = ["ENTRY_TIME_UTC", "MomentumJobResult", "due_entries", "run_momentum_book"]
