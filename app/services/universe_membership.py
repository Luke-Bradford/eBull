"""
Universe-membership reconcile (#2290).

Maintains ``instrument_universe_membership`` — the append-only record of
which instruments were members of the tradable universe on which dates.
``sync_universe`` previously overwrote that transition in place, leaving
"was instrument X tradable on date D?" unanswerable for every past D.

See ``sql/271_instrument_universe_membership.sql`` for why the close date
comes from this table's own ``last_confirmed_on`` and never from
``instruments.last_seen_at``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MembershipReconcileStats:
    """Outcome of one :func:`reconcile_universe_membership` pass."""

    confirmed: int
    imported: int
    listed: int
    relisted: int
    reopened_same_day: int
    closed: int


def reconcile_universe_membership(
    conn: psycopg.Connection[Any],
) -> MembershipReconcileStats:
    """Bring ``instrument_universe_membership`` in line with
    ``instruments.is_tradable`` (#2290).

    ⚠⚠ **Ordering is load-bearing: this MUST run after the deactivation
    UPDATE inside ``sync_universe``'s transaction, and its correctness has
    no other guard.** The function reads presence from
    ``instruments.is_tradable``, and that column only equals "returned by
    this sync's provider feed" once both halves of the sync have run: the
    upsert sets it TRUE for every record the provider returned, and the
    deactivation clears it for every instrument the provider omitted. The
    provider hardcodes ``is_tradable=True``
    (``app/providers/implementations/etoro.py``), so the upsert is never
    itself a deactivation path. Called standalone against a database whose
    ``is_tradable`` is stale, pass 1 would happily confirm membership for
    instruments the feed stopped returning weeks ago — and the record it
    writes would be indistinguishable from a true one.

    Being a pure function of current table state (the same shape as
    ``reconcile_symbol_history``) is what makes it testable without a
    provider, and lets it self-heal drift introduced outside the sync path.

    Four passes, ordered:

    1. **Confirm** — tradable instruments with an open row get
       ``last_confirmed_on = CURRENT_DATE``. Rows already confirmed today
       are skipped, so a second sync on the same day does not rewrite the
       whole universe.
    2. **Same-day reopen** — a tradable instrument with no open row but a
       row closed TODAY is a provider flip-flop (present, absent, present
       within one day), not a relisting: there is no day on which it was
       absent. The closed row reopens, exactly as
       ``reconcile_symbol_history`` pass 2 undoes a same-day symbol flip.
       Opening a second row instead would assert a membership gap that did
       not happen, and under the inclusive-range EXCLUDE constraint it
       would also collide with the row closed today.
    3. **Open** — tradable instruments with no open row and nothing closed
       today. ``source_event`` records how the row landed: ``relisting``
       when any prior row exists for the instrument, ``listing`` when the
       instrument itself first appeared today, else ``imported`` (already
       tradable when this table was created, so its true membership start
       predates the record and is truncated here).
    4. **Close** — untradable instruments with an open row are closed at
       ``effective_to = last_confirmed_on``: the last date the provider
       actually returned them, never the date we noticed they were gone.

    The passes act on disjoint row sets (1-3 tradable, 4 untradable), so
    the order between them is not itself load-bearing; it is fixed only so
    the counts read in a sensible sequence.

    Caller owns the transaction (``sync_universe`` wraps the whole sync;
    tests commit explicitly).
    """
    # Explicit tuple_row: the caller's connection may carry any row factory
    # (sync_universe's is unparameterised), and the int(r[0]) / r[0] fetches
    # below must not depend on it — same reason as reconcile_symbol_history.
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        # Pass 1 — confirm today's presence on the open row.
        cur.execute(
            """
            UPDATE instrument_universe_membership m
            SET last_confirmed_on = CURRENT_DATE
            FROM instruments i
            WHERE i.instrument_id = m.instrument_id
              AND i.is_tradable
              AND m.effective_to IS NULL
              AND m.last_confirmed_on < CURRENT_DATE
            """,
        )
        confirmed = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

        # Pass 2 — same-day flip-flop undo: reopen the row closed today
        # rather than opening a second one across a gap that never existed.
        cur.execute(
            """
            UPDATE instrument_universe_membership m
            SET effective_to = NULL,
                last_confirmed_on = CURRENT_DATE
            FROM instruments i
            WHERE i.instrument_id = m.instrument_id
              AND i.is_tradable
              AND m.effective_to = CURRENT_DATE
              AND NOT EXISTS (
                  SELECT 1
                  FROM instrument_universe_membership o
                  WHERE o.instrument_id = m.instrument_id
                    AND o.effective_to IS NULL
              )
            """,
        )
        reopened = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

        # Pass 3 — open a membership row for anything tradable without one.
        #
        # NOT EXISTS(open row) is re-evaluated here rather than reused from
        # pass 2 because pass 2 has just closed the gap for the flip-flop
        # rows; a candidate list captured before it would re-open them.
        cur.execute(
            """
            INSERT INTO instrument_universe_membership (
                instrument_id, effective_from, effective_to,
                last_confirmed_on, source_event
            )
            SELECT i.instrument_id,
                   CURRENT_DATE,
                   NULL,
                   CURRENT_DATE,
                   CASE
                       WHEN EXISTS (
                           SELECT 1
                           FROM instrument_universe_membership p
                           WHERE p.instrument_id = i.instrument_id
                       ) THEN 'relisting'
                       WHEN i.first_seen_at::date = CURRENT_DATE THEN 'listing'
                       ELSE 'imported'
                   END
            FROM instruments i
            WHERE i.is_tradable
              AND NOT EXISTS (
                  SELECT 1
                  FROM instrument_universe_membership o
                  WHERE o.instrument_id = i.instrument_id
                    AND o.effective_to IS NULL
              )
            RETURNING source_event
            """,
        )
        opened = [str(r[0]) for r in cur.fetchall()]

        # Pass 4 — close at the last CONFIRMED date, not today.
        cur.execute(
            """
            UPDATE instrument_universe_membership m
            SET effective_to = m.last_confirmed_on
            FROM instruments i
            WHERE i.instrument_id = m.instrument_id
              AND NOT i.is_tradable
              AND m.effective_to IS NULL
            """,
        )
        closed = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    return MembershipReconcileStats(
        confirmed=confirmed,
        imported=opened.count("imported"),
        listed=opened.count("listing"),
        relisted=opened.count("relisting"),
        reopened_same_day=reopened,
        closed=closed,
    )


__all__ = ["MembershipReconcileStats", "reconcile_universe_membership"]
