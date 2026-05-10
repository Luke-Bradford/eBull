"""Tests for the CIK coverage audit (#1067).

The audit reports the us_equity tradable cohort, how many have a
primary SEC CIK in ``external_identifiers``, and a categorised
sample of the unmapped rows (``suffix_variants`` vs ``other``).
"""

from __future__ import annotations

import psycopg

from app.services.cik_coverage_audit import compute_cik_gap_report
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


def _seed_universe(conn: psycopg.Connection[tuple]) -> None:
    # us_equity exchange.
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES ('audit_us', 'audit us eq', 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
    )
    # crypto exchange — out of cohort.
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES ('audit_crypto', 'audit crypto', 'US', 'crypto')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
    )
    # Mapped row: AAPL with CIK.
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (90671001, 'AAPL', 'Apple Inc', 'audit_us', 'USD', TRUE)
        """,
    )
    conn.execute(
        """
        INSERT INTO external_identifiers
          (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (90671001, 'sec', 'cik', '0000320193', TRUE)
        ON CONFLICT (provider, identifier_type, identifier_value, instrument_id)
            WHERE provider = 'sec' AND identifier_type = 'cik'
        DO NOTHING
        """,
    )
    # Unmapped suffix variant: AAPL.RTH (no CIK).
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (90671002, 'AAPL.RTH', 'Apple Inc (RTH)', 'audit_us', 'USD', TRUE)
        """,
    )
    # Unmapped other: ETF.
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (90671003, 'AAXJ', 'iShares MSCI Asia ETF', 'audit_us', 'USD', TRUE)
        """,
    )
    # Crypto — out of cohort, should not affect counts.
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (90671004, 'BTC', 'Bitcoin', 'audit_crypto', 'USD', TRUE)
        """,
    )
    conn.commit()


def test_gap_report_counts_us_equity_only(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Cohort scoped to us_equity asset_class — crypto must not
    inflate the cohort count or the unmapped count.

    Cohort = 3 (AAPL + AAPL.RTH + AAXJ). Mapped = 1 (AAPL).
    Unmapped = 2 (AAPL.RTH suffix + AAXJ other).
    """
    conn = ebull_test_conn
    _seed_universe(conn)

    report = compute_cik_gap_report(conn)

    assert report.cohort_total == 3
    assert report.mapped == 1
    assert report.unmapped == 2
    assert report.unmapped_suffix_variants == 1  # AAPL.RTH
    assert report.unmapped_other == 1  # AAXJ


def test_sample_orders_other_before_suffix_variants(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Sample prioritises ``other`` over ``suffix_variant`` so the
    operator sees the genuine gaps first when scanning the response."""
    conn = ebull_test_conn
    _seed_universe(conn)

    report = compute_cik_gap_report(conn, sample_limit=10)

    # Two unmapped rows; both fit in the sample.
    assert len(report.sample) == 2
    assert report.sample[0].category == "other"
    assert report.sample[0].symbol == "AAXJ"
    assert report.sample[1].category == "suffix_variant"
    assert report.sample[1].symbol == "AAPL.RTH"


def test_demoted_historical_cik_does_not_count_as_mapped(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Codex pre-push round 1: ``is_primary = TRUE`` is load-bearing.
    An instrument with only a demoted historical CIK row must count
    as UNMAPPED, not mapped — otherwise a stale-CIK-only row would
    falsely inflate the coverage figure."""
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES ('audit_us2', 'audit us eq 2', 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
    )
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (90671099, 'STALECIK', 'Stale Inc', 'audit_us2', 'USD', TRUE)
        """,
    )
    # Only a demoted historical CIK exists for this instrument.
    conn.execute(
        """
        INSERT INTO external_identifiers
          (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (90671099, 'sec', 'cik', '0009999999', FALSE)
        ON CONFLICT (provider, identifier_type, identifier_value, instrument_id)
            WHERE provider = 'sec' AND identifier_type = 'cik'
        DO NOTHING
        """,
    )
    conn.commit()

    report = compute_cik_gap_report(conn)

    # Should appear in the unmapped count + sample (no primary).
    assert any(s.symbol == "STALECIK" for s in report.sample)
    assert report.unmapped >= 1


def test_sample_limit_caps_payload(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Sample limit bounds the per-row payload regardless of the
    aggregate unmapped count."""
    conn = ebull_test_conn
    _seed_universe(conn)
    # Add 5 more suffix-variant unmapped rows so the aggregate
    # exceeds sample_limit=2.
    for i in range(5):
        conn.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
            VALUES (%s, %s, 'extra', 'audit_us', 'USD', TRUE)
            """,
            (90671010 + i, f"EXTRA{i}.RTH"),
        )
    conn.commit()

    report = compute_cik_gap_report(conn, sample_limit=2)

    assert report.unmapped == 7  # AAPL.RTH + AAXJ + 5 EXTRA.RTH
    assert len(report.sample) == 2
