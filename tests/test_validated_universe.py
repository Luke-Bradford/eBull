"""The §4.0 validated universe — the scope every promotion gate is judged against (#2605).

``check_promotable`` refuses ``instrument_outside_validated_universe`` by
subtracting ``validated_universe_ids`` from the evaluated set
(``app/services/strategy_result.py:850``), and ``run_backtest`` — the sole
writer of ``strategy_results_store`` — fills that set from
``load_validated_universe``. So the refusal is only as strong as this module's
``WHERE`` clause, and until #2605 that clause had no test at all —
`grep -rln load_validated_universe tests/` returned nothing. Widening it (a
dropped ``asset_class`` predicate, a dropped ``is_tradable``) would silently
admit non-US names to every gate while every existing test stayed green.

⚠ SCOPE: these pin the LOADER, which is the scope definition. They do not pin
the end-to-end path, because it does not currently exist —
``promote_strategy`` never calls ``check_promotable`` and cannot re-derive the
universe check from a stored result. That gap is #2621, and the test proving a
non-US result cannot be promoted belongs there, not here.

⚠ These are DB-backed on purpose. The predicates ARE the SQL; asserting on the
query string would import the constant it validates, which the prevention log
calls a tautology. One integration test per mechanism, per the test-quality
skill — the exclusion cases share a single seeded fixture.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.strategies.validated_universe import (
    STOCKS_TYPE_DESCRIPTION,
    load_validated_universe,
    resolve_stocks_type_id,
)
from tests.fixtures.ebull_test_db import (
    STOCKS_TYPE_ID,
    ebull_test_conn,  # noqa: F401 — fixture re-export
)

#: eToro's own ids, so the fixture cannot accidentally agree with a hardcoded 5.
#: ⚠ Imported, not redeclared (#2859) — `tests/fixtures/ebull_test_db` is the
#: single source for the §4.0 anchor id.
_STOCKS_TYPE_ID = STOCKS_TYPE_ID
_ETF_TYPE_ID = 6

#: One instrument per exclusion reason, plus the two that must survive.
#: ⚠ TWO in-universe rows, and the lower id is inserted LAST. One row makes the
#: ordering assertion vacuous — a list of length 1 is sorted under any ORDER BY.
_IN_UNIVERSE_LOWER = 90260500
_IN_UNIVERSE = 90260501
_ETF_SAME_VENUE = 90260502
_NON_US_VENUE = 90260503
_UNTRADABLE = 90260504
_UNKNOWN_ASSET_CLASS = 90260505


def _seed(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO etoro_instrument_types (instrument_type_id, description)
        VALUES (%(stocks)s, %(stocks_desc)s), (%(etf)s, 'ETF')
        ON CONFLICT (instrument_type_id) DO NOTHING
        """,
        {"stocks": _STOCKS_TYPE_ID, "stocks_desc": STOCKS_TYPE_DESCRIPTION, "etf": _ETF_TYPE_ID},
    )
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES ('vu_us', 'validated us venue', 'US', 'us_equity'),
               ('vu_uk', 'validated uk venue', 'GB', 'uk_equity'),
               ('vu_unknown', 'unclassified venue', NULL, 'unknown')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
    )
    conn.execute(
        """
        INSERT INTO instruments
            (instrument_id, symbol, company_name, exchange, currency, is_tradable, instrument_type_id)
        VALUES
            (%(in_universe)s, 'VUIN', 'US stock, tradable', 'vu_us', 'USD', TRUE, %(stocks)s),
            (%(etf)s, 'VUETF', 'US ETF, same venue', 'vu_us', 'USD', TRUE, %(etf_type)s),
            (%(non_us)s, 'VUUK', 'UK stock', 'vu_uk', 'GBP', TRUE, %(stocks)s),
            (%(untradable)s, 'VUDEAD', 'US stock, delisted', 'vu_us', 'USD', FALSE, %(stocks)s),
            (%(unknown)s, 'VUUNK', 'unclassified venue stock', 'vu_unknown', 'USD', TRUE, %(stocks)s),
            (%(in_universe_lower)s, 'VUIN2', 'US stock, lower id', 'vu_us', 'USD', TRUE, %(stocks)s)
        """,
        {
            "in_universe": _IN_UNIVERSE,
            "in_universe_lower": _IN_UNIVERSE_LOWER,
            "etf": _ETF_SAME_VENUE,
            "non_us": _NON_US_VENUE,
            "untradable": _UNTRADABLE,
            "unknown": _UNKNOWN_ASSET_CLASS,
            "stocks": _STOCKS_TYPE_ID,
            "etf_type": _ETF_TYPE_ID,
        },
    )
    conn.commit()


def test_universe_admits_us_stocks_and_excludes_every_other_axis(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """All four exclusions asserted individually, not as one count.

    A single ``len(universe) == 1`` would pass if the ETF were dropped and the
    UK name admitted, so each id is checked by name. The settled decision
    (#2605) is *US listing venue AND Stocks type AND tradable* — three
    predicates, three ways to be silently widened.
    """
    _seed(ebull_test_conn)
    universe = set(load_validated_universe(ebull_test_conn))

    assert _IN_UNIVERSE in universe
    assert _IN_UNIVERSE_LOWER in universe
    assert _ETF_SAME_VENUE not in universe, "instrument_type_id must exclude ETFs — §4.0's homogeneity cut"
    assert _NON_US_VENUE not in universe, "exchanges.asset_class must exclude non-US venues — #2605"
    assert _UNTRADABLE not in universe, "is_tradable must still bind — see the module's look-ahead warning"
    assert _UNKNOWN_ASSET_CLASS not in universe, (
        "an unclassified venue must be REFUSED, not defaulted in — the cut is an allowlist"
    )


def test_universe_is_returned_ascending(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The docstring promises ascending ids, and callers index into it.

    ``backtest_run`` builds a frozenset from this, but ``strategy_signal_scan``
    and the verify scripts iterate it, so a change of ORDER BY would reorder
    work without failing anything else.
    """
    _seed(ebull_test_conn)
    universe = load_validated_universe(ebull_test_conn)

    assert len(universe) > 1, "a one-element universe makes the ordering assertion vacuous"
    assert list(universe) == sorted(universe)


def test_stocks_type_id_raises_when_the_lookup_does_not_resolve(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """§4.0 asked for an assertion, not a hardcoded 5.

    ``instruments.instrument_type_id`` carries no foreign key, so the universe
    definition rests on an unconstrained provider-maintained integer. An empty
    lookup must raise rather than yield a universe of nobody — which would read
    downstream as "no instrument is outside the validated universe".
    """
    _seed(ebull_test_conn)
    ebull_test_conn.execute(
        "DELETE FROM etoro_instrument_types WHERE description = %(description)s",
        {"description": STOCKS_TYPE_DESCRIPTION},
    )

    with pytest.raises(RuntimeError, match="resolved to 0 rows"):
        resolve_stocks_type_id(ebull_test_conn)


def test_stocks_type_id_raises_when_the_description_is_ambiguous(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Two rows is the same failure as none — the anchor is gone either way.

    Silent under a hardcoded integer, and silent under a ``LIMIT 1`` too, which
    is why the loader counts rows instead of taking the first.
    """
    _seed(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO etoro_instrument_types (instrument_type_id, description)
        VALUES (%(duplicate_id)s, %(description)s)
        """,
        {"duplicate_id": 9_260_599, "description": STOCKS_TYPE_DESCRIPTION},
    )

    with pytest.raises(RuntimeError, match="resolved to 2 rows"):
        resolve_stocks_type_id(ebull_test_conn)
