"""§8.5 guard AUM integration tests and §8.6 Test 3 guard-delta.

Tests `_load_sector_exposure` directly — see spec §6.1 and §8.5
for why this is the correct test surface (GuardResult has no
total_aum field, AUM is a local inside evaluate_recommendation).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from app.services.execution_guard import _load_sector_exposure
from app.services.portfolio import _load_mirror_equity
from tests.fixtures.copy_mirrors import (
    _GUARD_INSTRUMENT_ID,
    _GUARD_INSTRUMENT_SECTOR,
    _NOW,
    mirror_aum_fixture,
)
from tests.test_operator_setup_race import (
    _assert_test_db,
    _test_database_url,
    _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable — skipping real-DB guard AUM test",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        with c.cursor() as cur:
            cur.execute(
                "TRUNCATE copy_mirror_positions, copy_mirrors, "
                "copy_traders, quotes, scores, positions, "
                "cash_ledger, instruments RESTART IDENTITY CASCADE"
            )
        c.commit()
        yield c
        c.rollback()


def _seed_ebull_position_and_cash(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    sector: str,
    units: float,
    cost_basis: float,
    quote_last: float | None,
    cash: float,
) -> None:
    """Add one eBull position + one cash ledger row on top of the
    mirror_aum_fixture base. Instrument must not collide with
    _GUARD_INSTRUMENT_ID — §8.5 scenarios use a separate
    instrument for the eBull position so the sector-numerator
    test has a distinct id.
    """
    # NOTE — schema references: sql/001_init.sql:1-13 (instruments),
    # sql/001_init.sql:159-168 (positions), sql/021_positions_source.sql
    # (positions.source: 'ebull' | 'broker_sync'), sql/001_init.sql:170-177
    # (cash_ledger event_type / amount / currency / note, no 'reason' or
    # 'recorded_at'), sql/002_market_data_features.sql (quotes). Do NOT
    # use 'tier', 'created_at', 'updated_at' on instruments — they do
    # not exist. Do NOT use 'broker' as a positions.source — the CHECK
    # constraint rejects it.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name,
                                     sector, is_tradable)
            VALUES (%(iid)s, 'EBULL', 'eBull Position',
                    %(sector)s, TRUE)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
            {"iid": instrument_id, "sector": sector},
        )
        cur.execute(
            """
            INSERT INTO positions (instrument_id, current_units,
                                   cost_basis, avg_cost, open_date,
                                   source, updated_at)
            VALUES (%(iid)s, %(units)s, %(cb)s,
                    %(cb)s / NULLIF(%(units)s, 0),
                    %(today)s, 'broker_sync', %(now)s)
            """,
            {
                "iid": instrument_id,
                "units": units,
                "cb": cost_basis,
                "today": _NOW.date(),
                "now": _NOW,
            },
        )
        if quote_last is not None:
            cur.execute(
                """
                INSERT INTO quotes (instrument_id, last, bid, ask,
                                    quoted_at)
                VALUES (%(iid)s, %(last)s, %(last)s, %(last)s, %(now)s)
                ON CONFLICT (instrument_id) DO UPDATE
                  SET last = EXCLUDED.last,
                      bid  = EXCLUDED.bid,
                      ask  = EXCLUDED.ask,
                      quoted_at = EXCLUDED.quoted_at
                """,
                {"iid": instrument_id, "last": quote_last, "now": _NOW},
            )
        cur.execute(
            """
            INSERT INTO cash_ledger (event_type, amount, currency)
            VALUES ('deposit', %(amt)s, 'GBP')
            """,
            {"amt": cash},
        )


def test_empty_baseline_no_mirrors(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.5: with no copy_mirrors rows at all, guard AUM is the
    pre-PR contract: positions_mv + cash.
    """
    # Seed ONLY the instruments row for the guard query + one
    # eBull position + cash.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name,
                                     sector, is_tradable)
            VALUES (%(iid)s, 'AUMTEST', 'guard instrument',
                    %(sector)s, TRUE)
            """,
            {
                "iid": _GUARD_INSTRUMENT_ID,
                "sector": _GUARD_INSTRUMENT_SECTOR,
            },
        )
    _seed_ebull_position_and_cash(
        conn,
        instrument_id=770001,
        sector="healthcare",
        units=10.0,
        cost_basis=200.0,
        quote_last=25.0,  # mv = 250.0
        cash=100.0,
    )
    conn.commit()

    found, sector, pct, total_aum = _load_sector_exposure(conn, _GUARD_INSTRUMENT_ID, cash=100.0)
    assert found is True
    assert sector == _GUARD_INSTRUMENT_SECTOR
    assert total_aum == pytest.approx(250.0 + 100.0, abs=1e-6)


def test_active_mirror_adds_to_denominator(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.5: active mirror's equity is added to total_aum on top of
    positions + cash. Sector numerator is untouched.
    """
    mirror_aum_fixture(conn)
    _seed_ebull_position_and_cash(
        conn,
        instrument_id=770001,
        sector="healthcare",  # DIFFERENT sector from guard instrument
        units=10.0,
        cost_basis=200.0,
        quote_last=25.0,  # mv = 250.0
        cash=100.0,
    )
    conn.commit()

    expected_mirror = _load_mirror_equity(conn)
    assert expected_mirror == pytest.approx(1550.0, abs=1e-6)

    found, sector, pct, total_aum = _load_sector_exposure(conn, _GUARD_INSTRUMENT_ID, cash=100.0)
    assert found is True
    assert sector == _GUARD_INSTRUMENT_SECTOR
    assert total_aum == pytest.approx(250.0 + 100.0 + 1550.0, abs=1e-6)


def test_closed_mirror_contributes_nothing(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.5: flipping active=FALSE on all mirrors returns total_aum
    to the baseline (positions + cash).
    """
    mirror_aum_fixture(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    _seed_ebull_position_and_cash(
        conn,
        instrument_id=770001,
        sector="healthcare",
        units=10.0,
        cost_basis=200.0,
        quote_last=25.0,
        cash=100.0,
    )
    conn.commit()

    found, _, _, total_aum = _load_sector_exposure(conn, _GUARD_INSTRUMENT_ID, cash=100.0)
    assert found is True
    assert total_aum == pytest.approx(250.0 + 100.0, abs=1e-6)


def test_sector_numerator_unchanged_by_mirror(
    conn: psycopg.Connection[Any],
) -> None:
    """§4 / §8.5: mirrors expand the denominator only. Query under
    an instrument whose sector != the mirror's sector; mirror
    contributes to total_aum but NOT to current_sector_pct.
    """
    mirror_aum_fixture(conn)
    # Add one eBull position in `healthcare` (different from
    # _GUARD_INSTRUMENT_SECTOR='technology') so the sector split
    # is visible.
    _seed_ebull_position_and_cash(
        conn,
        instrument_id=770001,
        sector="healthcare",
        units=10.0,
        cost_basis=200.0,
        quote_last=25.0,  # mv = 250.0 in healthcare
        cash=100.0,
    )
    conn.commit()

    # Query FOR a healthcare instrument → sector numerator should
    # cover only the 770001 position (which the query itself
    # excludes via instrument_id != iid, so numerator = 0).
    # The denominator still includes the mirror.
    found_hc, sector_hc, pct_hc, aum_hc = _load_sector_exposure(conn, 770001, cash=100.0)
    assert found_hc is True
    assert sector_hc == "healthcare"
    # Expected: the sole healthcare position is the iid being
    # queried, so the numerator is 0 (the query EXCLUDES the
    # queried instrument to avoid counting itself). Denominator
    # is mirror equity only (250 is being excluded).
    assert pct_hc == pytest.approx(0.0, abs=1e-6)
    # Denominator = positions - self + cash + mirror_equity
    #             = (250 - 250) + 100 + 1550 = 1650
    assert aum_hc == pytest.approx(100.0 + 1550.0, abs=1e-6)


def test_guard_delta_matches_mirror_equity(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.6 Test 3: the additive delta on the guard path equals
    _load_mirror_equity(conn). Symmetry with §8.6 Tests 1 and 2
    so the per-call-site delta contract is visible.
    """
    mirror_aum_fixture(conn)
    _seed_ebull_position_and_cash(
        conn,
        instrument_id=770001,
        sector="healthcare",
        units=10.0,
        cost_basis=200.0,
        quote_last=25.0,
        cash=100.0,
    )
    conn.commit()

    expected_mirror_contribution = _load_mirror_equity(conn)
    _, _, _, with_mirror = _load_sector_exposure(conn, _GUARD_INSTRUMENT_ID, cash=100.0)

    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    conn.commit()

    _, _, _, without_mirror = _load_sector_exposure(conn, _GUARD_INSTRUMENT_ID, cash=100.0)

    assert (with_mirror - without_mirror) == pytest.approx(expected_mirror_contribution, abs=1e-6)
