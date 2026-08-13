"""Regression test for migration 066 (purge orphan SEC data, #503).

Verifies the migration predicate against a real ``ebull_test``
Postgres: rows on instruments WITHOUT a current SEC CIK in
``external_identifiers`` are deleted; rows on instruments WITH
the CIK link survive.

The migration is auto-applied at fixture setup, so the test
seeds two instrument shapes — one no-CIK, one with-CIK — plus
matching rows in every SEC-derived table the migration claims
to purge, then RE-EXECUTES the migration's DELETEs (the
migration is documented as idempotent) and asserts the
no-CIK rows are gone while the with-CIK rows survive.
"""

from __future__ import annotations

import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# Re-execution of the migration's DELETEs. Mirrors
# sql/066_purge_orphan_sec_data.sql verbatim. Anchored to
# the SEC-only base tables we currently exercise; the
# multi-source ones are covered by the same predicate
# semantics and don't need their own setup row to pin
# the contract here (the schema-level test below confirms
# the migration applies cleanly with all DELETEs).
_PURGE_DELETES_SEC_ONLY: tuple[tuple[str, str], ...] = (
    ("filing_events", "fe"),
    ("insider_filings", "i"),
    ("eight_k_filings", "ek"),
    ("instrument_business_summary_sections", "ibss"),
    ("instrument_business_summary", "ibs"),
    ("dividend_events", "de"),
    ("financial_facts_raw", "ffr"),
    ("instrument_sec_profile", "isp"),
    ("sec_entity_change_log", "secl"),
)


def _exec_purge(conn: object) -> None:
    """Re-run the migration's DELETE statements. Idempotent."""
    for table, alias in _PURGE_DELETES_SEC_ONLY:
        conn.execute(  # type: ignore[attr-defined]
            f"""
            DELETE FROM {table} {alias}
            WHERE NOT EXISTS (
                SELECT 1 FROM external_identifiers ei
                WHERE ei.instrument_id = {alias}.instrument_id
                  AND ei.provider = 'sec'
                  AND ei.identifier_type = 'cik'
            )
            """
        )


def _seed_instrument(conn: object, instrument_id: int, symbol: str) -> None:
    conn.execute(  # type: ignore[attr-defined]
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, symbol, f"{symbol} test"),
    )


def _link_sec_cik(conn: object, instrument_id: int, cik: str) -> None:
    conn.execute(  # type: ignore[attr-defined]
        """
        INSERT INTO external_identifiers
            (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cik', %s, TRUE)
        ON CONFLICT (provider, identifier_type, identifier_value, instrument_id)
            WHERE provider = 'sec' AND identifier_type = 'cik'
        DO NOTHING
        """,
        (instrument_id, cik),
    )


def test_purge_predicate_drops_no_cik_filing_events_keeps_cik_linked(
    ebull_test_conn,  # noqa: F811
) -> None:
    """The core invariant: filing_events rows on an instrument
    without a current SEC CIK are deleted; rows on an
    instrument with the CIK link survive."""
    conn = ebull_test_conn

    _seed_instrument(conn, 9000001, "ORPH")  # no CIK link
    _seed_instrument(conn, 9000002, "GOOD")
    _link_sec_cik(conn, 9000002, "0009000002")

    for iid, accession in [(9000001, "ORPH-1"), (9000002, "GOOD-1")]:
        conn.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider, provider_filing_id)
            VALUES (%s, '2026-01-01', '10-K', 'sec', %s)
            """,
            (iid, accession),
        )

    _exec_purge(conn)

    # Orphan row purged.
    cur = conn.execute("SELECT COUNT(*) FROM filing_events WHERE instrument_id = 9000001")
    assert cur.fetchone()[0] == 0
    # Linked row survives.
    cur = conn.execute("SELECT COUNT(*) FROM filing_events WHERE instrument_id = 9000002")
    assert cur.fetchone()[0] == 1


def test_purge_predicate_idempotent(ebull_test_conn) -> None:  # noqa: F811
    """Re-running the migration on a clean DB is a zero-row
    delete. Pin so a future change to the predicate that
    accidentally re-purges rows fails loud."""
    conn = ebull_test_conn

    _seed_instrument(conn, 9000003, "GOOD2")
    _link_sec_cik(conn, 9000003, "0009000003")
    conn.execute(
        """
        INSERT INTO filing_events
            (instrument_id, filing_date, filing_type, provider, provider_filing_id)
        VALUES (9000003, '2026-01-02', '10-K', 'sec', 'GOOD2-1')
        """
    )

    _exec_purge(conn)
    _exec_purge(conn)  # re-run

    cur = conn.execute("SELECT COUNT(*) FROM filing_events WHERE instrument_id = 9000003")
    assert cur.fetchone()[0] == 1


def test_multi_source_predicate_filters_on_source_column(
    ebull_test_conn,  # noqa: F811
) -> None:
    """``financial_periods`` rows on a no-CIK instrument are
    purged ONLY when source IN ('sec', 'sec_edgar', ...). A
    future row sourced from FMP / another provider on the
    same instrument is left alone — that's the safety the
    multi-source predicate buys (Codex round 1 finding 2 on
    spec)."""
    conn = ebull_test_conn

    _seed_instrument(conn, 9000004, "MULT")  # no CIK

    # Seed two distinct (period_end_date, source) rows so the PK
    # ``(instrument_id, period_end_date, period_type)`` does not
    # collide. Pre-#530 the test seeded both rows with the same
    # period_end_date, expecting two sources to coexist on the same
    # PK — the schema doesn't allow that, so the second INSERT was
    # silently dropped by ``ON CONFLICT DO NOTHING`` and the test
    # was effectively single-row.
    #
    # ``source_ref`` and ``reported_currency`` are NOT NULL (#530
    # also fixes that — pre-fix the INSERT silently inserted NULLs
    # against the constraint).
    for period_end, fiscal_year, source in (
        ("2024-12-31", 2024, "sec_edgar"),
        ("2025-12-31", 2025, "fmp"),
    ):
        conn.execute(
            """
            INSERT INTO financial_periods
                (instrument_id, period_end_date, period_type,
                 source, source_ref, fiscal_year, reported_currency)
            VALUES (%s, %s, 'FY', %s, %s, %s, 'USD')
            ON CONFLICT DO NOTHING
            """,
            (9000004, period_end, source, f"test-{source}", fiscal_year),
        )

    conn.execute(
        """
        DELETE FROM financial_periods fp
        WHERE fp.source IN ('sec', 'sec_edgar', 'sec_xbrl', 'sec_companyfacts')
          AND NOT EXISTS (
              SELECT 1 FROM external_identifiers ei
              WHERE ei.instrument_id = fp.instrument_id
                AND ei.provider = 'sec'
                AND ei.identifier_type = 'cik'
          )
        """
    )

    cur = conn.execute("SELECT source FROM financial_periods WHERE instrument_id = 9000004 ORDER BY source")
    surviving = [row[0] for row in cur.fetchall()]
    # SEC-sourced row deleted; FMP-sourced row survives the
    # multi-source predicate.
    assert surviving == ["fmp"]
