"""Post-migration-156 functional regression for the share-count views.

Migration 156 drops + re-creates `share_count_history` /
`instrument_dilution_summary` / `instrument_share_count_latest`
verbatim from sql/052 (the views can't survive `DROP TABLE
financial_facts_raw` and must be re-installed after the partitioned
parent's swap-rename). This test seeds facts + verifies the
re-installed views produce expected rows — a behavioural anchor in
case the inline view bodies in 156 drift from sql/052.
"""

from __future__ import annotations

from datetime import date

import psycopg


def _seed_instrument(conn: psycopg.Connection[tuple], *, instrument_id: int) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, f"T{instrument_id}", f"Test {instrument_id}"),
    )
    conn.commit()


def _seed_outstanding(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    period_end: date,
    shares: int,
    concept: str = "EntityCommonStockSharesOutstanding",
    accession_number: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO financial_facts_raw (
            instrument_id, taxonomy, concept, unit, period_end, val,
            accession_number, form_type, filed_date
        ) VALUES (%s, 'dei', %s, 'shares', %s, %s, %s, '10-K', %s)
        """,
        (
            instrument_id,
            concept,
            period_end,
            shares,
            accession_number or f"acc-{period_end.isoformat()}",
            period_end,
        ),
    )
    conn.commit()


def test_views_exist_after_migration_156(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """All three views are re-installed by migration 156."""
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT viewname FROM pg_views "
            " WHERE schemaname = 'public' AND viewname IN ("
            "   'share_count_history', "
            "   'instrument_dilution_summary', "
            "   'instrument_share_count_latest') "
            " ORDER BY viewname"
        )
        names = [r[0] for r in cur.fetchall()]
    assert names == [
        "instrument_dilution_summary",
        "instrument_share_count_latest",
        "share_count_history",
    ]


def test_share_count_history_returns_seeded_rows(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    iid = 30001
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    for q_end in (
        date(2024, 3, 31),
        date(2024, 6, 30),
        date(2024, 9, 30),
        date(2024, 12, 31),
        date(2025, 3, 31),
    ):
        _seed_outstanding(
            ebull_test_conn,
            instrument_id=iid,
            period_end=q_end,
            shares=1_000_000_000 + q_end.toordinal(),
        )

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT period_end, shares_outstanding FROM share_count_history "
            " WHERE instrument_id = %s ORDER BY period_end",
            (iid,),
        )
        rows = cur.fetchall()
    assert len(rows) == 5
    # shares_outstanding picked from DEI section (the seeded concept).
    assert all(int(r[1]) > 0 for r in rows)


def test_instrument_share_count_latest_picks_newest(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    iid = 30002
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    _seed_outstanding(
        ebull_test_conn,
        instrument_id=iid,
        period_end=date(2024, 12, 31),
        shares=1_000_000_000,
    )
    _seed_outstanding(
        ebull_test_conn,
        instrument_id=iid,
        period_end=date(2025, 3, 31),
        shares=1_100_000_000,
    )

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT latest_shares, as_of_date, source_taxonomy "
            "  FROM instrument_share_count_latest "
            " WHERE instrument_id = %s",
            (iid,),
        )
        row = cur.fetchone()
    assert row is not None
    assert int(row[0]) == 1_100_000_000
    assert row[1] == date(2025, 3, 31)
    assert row[2] == "dei"
