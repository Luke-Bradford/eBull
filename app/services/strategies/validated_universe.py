"""The §4.0 validated universe — US stocks ex-ETF.

Parent spec: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md``
§4.0 (decided 2026-08-05, #2289). Refs #2240, #2288, #2289.

    *"The validated universe is ``asset_class='us_equity' AND
    instrument_type_id = 5``."*

One definition, one place. Every §4 strategy is validated on it, §5's gates run
over it, and §7's allocation reads it — so three call sites re-deriving the same
two predicates is three chances to drift.

⚠ THIS IS A *NECESSARY, NOT SUFFICIENT* CUT, and §4.0 says so at length: eToro's
``Stocks`` type is a provider classification, not a security master, so ADRs,
US-listed foreign private issuers, REITs, BDCs, preferreds, units and SPAC
remnants all carry it. The name here is "validated universe", never "US common
stocks" — that would be the wrong-population claim §10 of the parent is about.

⚠ ``is_tradable`` IS A LOOK-AHEAD FILTER AND IS USED ANYWAY.
It is *today's* listing state, so selecting on it excludes companies that were
listed at the decision date and are not now — the survivorship bias §2.1
describes. It is used because §4.0's decision was measured on exactly this
predicate, and because the honest disclosure already exists: every row produced
from these instruments is labelled ``universe = 'survivor_only'`` (#2288). The
fix is a point-in-time membership record (#2290) plus the delisted corpus
(#2284), not a different ``WHERE`` clause. **Do not quietly drop the filter to
make a number look better** — that would widen the population without changing
the label, which is worse than the bias.

⚠ NO ORDER GATE LIVES HERE. §4.0's allocation invariant 2 puts the hard
pre-trade rule in ``execution_guard`` — *"A ledger label is observability; this
needs enforcement"* — and that is phase 7. This module answers "who is in
scope"; it does not stop anything.
"""

from __future__ import annotations

from typing import Any, Final

import psycopg

#: Names the definition below for records that freeze this universe (#2621).
#: ⚠ Bump when the DEFINITION changes (the predicates, not this module's
#: comments) — a frozen ``strategy_result_universe`` record carries the version
#: its ids were produced under, and the promotion transition refuses versions it
#: does not recognise rather than re-interpreting them. Same shape as
#: ``TRIAL_REGISTER_VERSION``.
VALIDATED_UNIVERSE_RULE_VERSION: Final = "validated-universe-us-stocks-v1"

#: The ``etoro_instrument_types.description`` of the one type in scope.
#: ⚠ Resolved through the table rather than written as ``instrument_type_id = 5``.
#: §4.0: *"``instruments.instrument_type_id`` carries no foreign key … the
#: universe definition therefore rests on an unconstrained integer maintained by
#: the provider sync … the universe query must assert the type-id lookup
#: resolves, not assume it."* A literal 5 asserts nothing; a lookup that finds
#: no row raises, which is the assertion.
STOCKS_TYPE_DESCRIPTION = "Stocks"

#: §4.0's other half. On ``exchanges``, NOT on ``instruments`` — measured and
#: written down in the spec because the intuitive join is the wrong one.
US_EQUITY_ASSET_CLASS = "us_equity"

_RESOLVE_TYPE_SQL = """
    SELECT instrument_type_id
    FROM etoro_instrument_types
    WHERE description = %(description)s
"""

_UNIVERSE_SQL = """
    SELECT i.instrument_id
    FROM instruments i
    JOIN exchanges e ON e.exchange_id = i.exchange
    WHERE i.is_tradable
      AND e.asset_class = %(asset_class)s
      AND i.instrument_type_id = %(instrument_type_id)s
    ORDER BY i.instrument_id
"""


def resolve_stocks_type_id(conn: psycopg.Connection[Any]) -> int:
    """The ``Stocks`` type id, or raise.

    Raises when the description resolves to no row or to more than one. Both are
    the same failure — the universe definition has lost its anchor — and both
    are silent under a hardcoded integer, which is why §4.0 asked for the
    assertion.
    """
    rows = conn.execute(_RESOLVE_TYPE_SQL, {"description": STOCKS_TYPE_DESCRIPTION}).fetchall()
    if len(rows) != 1:
        raise RuntimeError(
            f"etoro_instrument_types.description = {STOCKS_TYPE_DESCRIPTION!r} resolved to {len(rows)} rows, "
            "expected exactly 1 — the §4.0 validated-universe definition has no anchor"
        )
    return int(rows[0][0])


def load_validated_universe(conn: psycopg.Connection[Any]) -> tuple[int, ...]:
    """Every ``instrument_id`` in the §4.0 validated universe, ascending.

    ⚠ Returns ids, not a count. A caller that wants the size takes ``len`` of
    this; a caller that wants a census joins against it. Handing back a number
    invites it being written down, and this population moves with every
    ``sync_universe`` run.
    """
    instrument_type_id = resolve_stocks_type_id(conn)
    rows = conn.execute(
        _UNIVERSE_SQL,
        {"asset_class": US_EQUITY_ASSET_CLASS, "instrument_type_id": instrument_type_id},
    ).fetchall()
    return tuple(int(row[0]) for row in rows)


__all__ = [
    "STOCKS_TYPE_DESCRIPTION",
    "US_EQUITY_ASSET_CLASS",
    "VALIDATED_UNIVERSE_RULE_VERSION",
    "load_validated_universe",
    "resolve_stocks_type_id",
]
