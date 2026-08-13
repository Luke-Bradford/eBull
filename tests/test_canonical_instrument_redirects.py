"""Tests for the canonical-instrument-redirect populate service (#819).

The service walks every ``%.RTH`` variant in ``instruments`` and binds
its ``canonical_instrument_id`` to the matching base instrument. The
match rule:

  * Variant symbol ends in ``.RTH`` (case-insensitive).
  * Base symbol == variant minus suffix.
  * Base lives on a DIFFERENT exchange (the .RTH variant lives on
    eToro's operational-duplicate exchange).
  * Single base, OR exactly one with ``is_primary_listing=TRUE``.

Tests cover the happy path, ambiguity skip, no-base skip, and
idempotency.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.canonical_instrument_redirects import populate_canonical_redirects
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


def _ensure_exchange(
    conn: psycopg.Connection[tuple],
    exchange_id: str,
    asset_class: str = "us_equity",
) -> None:
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES (%s, %s, 'US', %s)
        ON CONFLICT (exchange_id) DO NOTHING
        """,
        (exchange_id, f"Exchange {exchange_id}", asset_class),
    )


def _insert_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
    exchange: str,
    is_primary_listing: bool = True,
    asset_class: str = "us_equity",
) -> None:
    _ensure_exchange(conn, exchange, asset_class=asset_class)
    conn.execute(
        """
        INSERT INTO instruments
            (instrument_id, symbol, company_name, exchange, currency,
             is_tradable, is_primary_listing)
        VALUES (%s, %s, %s, %s, 'USD', TRUE, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co", exchange, is_primary_listing),
    )


def test_happy_path_single_base_per_variant(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """One ``.RTH`` variant on exchange '33', one base on exchange '4'.
    Populate sets the variant's canonical_instrument_id to the base."""
    conn = ebull_test_conn
    _insert_instrument(conn, iid=8190001, symbol="AAPL", exchange="4")
    _insert_instrument(conn, iid=8190002, symbol="AAPL.RTH", exchange="33")
    conn.commit()

    stats = populate_canonical_redirects(conn)

    assert stats.variants_scanned == 1
    assert stats.redirects_set == 1
    assert stats.redirects_already_correct == 0
    row = conn.execute(
        "SELECT canonical_instrument_id FROM instruments WHERE instrument_id = %s",
        (8190002,),
    ).fetchone()
    assert row is not None and row[0] == 8190001


def test_idempotent_second_run_reports_already_correct(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A second populate run on the same DB writes nothing and reports
    the existing bindings under ``redirects_already_correct``."""
    conn = ebull_test_conn
    _insert_instrument(conn, iid=8190101, symbol="MSFT", exchange="4")
    _insert_instrument(conn, iid=8190102, symbol="MSFT.RTH", exchange="33")
    conn.commit()

    populate_canonical_redirects(conn)
    stats2 = populate_canonical_redirects(conn)

    assert stats2.redirects_set == 0
    assert stats2.redirects_already_correct == 1


def test_no_base_skipped(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A ``.RTH`` variant with no matching base ticker is skipped."""
    conn = ebull_test_conn
    _insert_instrument(conn, iid=8190201, symbol="DELISTED.RTH", exchange="33")
    conn.commit()

    stats = populate_canonical_redirects(conn)

    assert stats.redirects_set == 0
    assert stats.redirects_skipped_no_base == 1
    row = conn.execute(
        "SELECT canonical_instrument_id FROM instruments WHERE instrument_id = %s",
        (8190201,),
    ).fetchone()
    assert row is not None and row[0] is None


def test_ambiguous_bases_resolved_via_primary_listing(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Two bases for the same symbol on different exchanges; exactly
    one is_primary_listing=TRUE wins. Without is_primary_listing the
    populate would skip as ambiguous."""
    conn = ebull_test_conn
    _insert_instrument(conn, iid=8190301, symbol="DUP", exchange="4", is_primary_listing=True)
    _insert_instrument(conn, iid=8190302, symbol="DUP", exchange="5", is_primary_listing=False)
    _insert_instrument(conn, iid=8190303, symbol="DUP.RTH", exchange="33")
    conn.commit()

    stats = populate_canonical_redirects(conn)

    assert stats.redirects_set == 1
    row = conn.execute(
        "SELECT canonical_instrument_id FROM instruments WHERE instrument_id = %s",
        (8190303,),
    ).fetchone()
    assert row is not None and row[0] == 8190301


def test_ambiguous_when_two_primary_listings_skipped(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Two bases both is_primary_listing=TRUE — skip rather than pick
    silently. Operator must hand-bind."""
    conn = ebull_test_conn
    _insert_instrument(conn, iid=8190401, symbol="MULTI", exchange="4", is_primary_listing=True)
    _insert_instrument(conn, iid=8190402, symbol="MULTI", exchange="5", is_primary_listing=True)
    _insert_instrument(conn, iid=8190403, symbol="MULTI.RTH", exchange="33")
    conn.commit()

    stats = populate_canonical_redirects(conn)

    assert stats.redirects_skipped_ambiguous == 1
    row = conn.execute(
        "SELECT canonical_instrument_id FROM instruments WHERE instrument_id = %s",
        (8190403,),
    ).fetchone()
    assert row is not None and row[0] is None


def test_non_equity_base_excluded_from_match(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Codex round 1: a crypto / non-us_equity row with the same
    stripped symbol must NOT be eligible as a canonical base."""
    conn = ebull_test_conn
    # Crypto ``AAPL`` on a crypto-asset-class exchange — must NOT
    # match. Tracker.
    _insert_instrument(
        conn,
        iid=8190601,
        symbol="AAPL",
        exchange="cryptoex",
        asset_class="crypto",
    )
    _insert_instrument(conn, iid=8190602, symbol="AAPL.RTH", exchange="33")
    conn.commit()

    stats = populate_canonical_redirects(conn)

    assert stats.redirects_set == 0
    assert stats.redirects_skipped_no_base == 1
    row = conn.execute(
        "SELECT canonical_instrument_id FROM instruments WHERE instrument_id = %s",
        (8190602,),
    ).fetchone()
    assert row is not None and row[0] is None


def test_existing_manual_binding_preserved(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Codex round 1: when a variant already has a non-NULL
    canonical_instrument_id (operator hand-binding or stale auto run),
    the script preserves it rather than overwriting."""
    conn = ebull_test_conn
    _insert_instrument(conn, iid=8190701, symbol="ZZZ", exchange="4")
    _insert_instrument(conn, iid=8190702, symbol="ZZZ", exchange="5")
    _insert_instrument(conn, iid=8190703, symbol="ZZZ.RTH", exchange="33")
    # Make BOTH bases is_primary_listing=FALSE so the rule would
    # default to ambiguous-skip on auto-run. Then operator hand-binds
    # to one of them — should be preserved.
    conn.execute(
        "UPDATE instruments SET is_primary_listing = FALSE WHERE instrument_id IN (%s, %s)",
        (8190701, 8190702),
    )
    conn.execute(
        "UPDATE instruments SET canonical_instrument_id = %s WHERE instrument_id = %s",
        (8190702, 8190703),
    )
    conn.commit()

    stats = populate_canonical_redirects(conn)

    # The auto rule would now ambiguous-skip (both bases TIE on
    # is_primary_listing=FALSE) so the only run-path is the
    # ambiguous skip. The operator binding stays intact.
    assert stats.redirects_set == 0
    row = conn.execute(
        "SELECT canonical_instrument_id FROM instruments WHERE instrument_id = %s",
        (8190703,),
    ).fetchone()
    assert row is not None and row[0] == 8190702

    # Now flip one base to is_primary_listing=TRUE so the rule resolves
    # — but only ``8190701`` becomes the auto target. The existing
    # binding at ``8190702`` must still be preserved.
    conn.execute(
        "UPDATE instruments SET is_primary_listing = TRUE WHERE instrument_id = %s",
        (8190701,),
    )
    conn.commit()

    stats2 = populate_canonical_redirects(conn)
    assert stats2.redirects_skipped_already_set_differently == 1
    row = conn.execute(
        "SELECT canonical_instrument_id FROM instruments WHERE instrument_id = %s",
        (8190703,),
    ).fetchone()
    assert row is not None and row[0] == 8190702


def test_check_constraint_blocks_self_loop(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The CHECK constraint must reject a self-loop UPDATE — guards
    against a future caller that accidentally points a row at itself
    and would otherwise drive the FE redirect into an infinite loop."""
    conn = ebull_test_conn
    _insert_instrument(conn, iid=8190501, symbol="LOOP", exchange="4")
    conn.commit()

    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            "UPDATE instruments SET canonical_instrument_id = %s WHERE instrument_id = %s",
            (8190501, 8190501),
        )
        conn.commit()
    # PR #1121 NITPICK fix: after the CheckViolation the tx is in an
    # aborted state. Rollback explicitly so fixture teardown's
    # TRUNCATE doesn't trip the aborted-tx guard.
    conn.rollback()
