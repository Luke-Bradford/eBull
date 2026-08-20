"""Peer-comparison data layer (#1751; SIC re-key #2023): DB integration —
factor formulas, SIC-cohort medians, the YoY consecutive-year guard, and that
the cohort key is SEC SIC (not eToro sector). Pure ranking + level-resolution
tests live in test_peer_comparison_ranking.py (DB-free, fast tier)."""

from __future__ import annotations

import psycopg
import pytest

from app.services.peer_comparison import compute_peer_comparison, is_factor_thin
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 (fixture re-export)

__all__ = ["ebull_test_conn"]


# financial_periods_ttm is a VIEW summing the latest 4 quarters (sql/032:209);
# stock items (equity/assets/debt) come from the most recent quarter. So a
# "complete TTM" is seeded by inserting 4 quarterly rows.
_Q_ENDS = ("2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31")
_Q_TYPES = ("Q1", "Q2", "Q3", "Q4")


def _seed_quarters(
    conn: psycopg.Connection[tuple],
    iid: int,
    *,
    revenue_q: float,
    op_income_q: float,
    net_income_q: float,
    equity: float,
    total_assets: float,
    long_term_debt: float = 0,
    short_term_debt: float = 0,
) -> None:
    """Insert 4 quarters → a complete TTM. Flow items sum; stock items are read
    from the latest quarter (2024-12-31), so set them only there."""
    for i, (end, qt) in enumerate(zip(_Q_ENDS, _Q_TYPES, strict=True)):
        latest = end == "2024-12-31"
        conn.execute(
            """
            INSERT INTO financial_periods
              (instrument_id, period_end_date, period_type, fiscal_year, source, source_ref,
               reported_currency, revenue, operating_income, net_income,
               total_assets, shareholders_equity, long_term_debt, short_term_debt,
               superseded_at, normalization_status)
            VALUES (%s, %s, %s, 2024, 'test', %s, 'USD', %s, %s, %s, %s, %s, %s, %s, NULL, 'normalized')
            """,
            (
                iid,
                end,
                qt,
                f"q{iid}{i}",
                revenue_q,
                op_income_q,
                net_income_q,
                total_assets if latest else None,
                equity if latest else None,
                long_term_debt if latest else None,
                short_term_debt if latest else None,
            ),
        )


def _seed_sic(conn: psycopg.Connection[tuple], iid: int, sic: str | None, desc: str | None) -> None:
    """One instrument_sec_profile row (sic3/sic2 are generated STORED cols)."""
    conn.execute(
        """
        INSERT INTO instrument_sec_profile (instrument_id, cik, sic, sic_description)
        VALUES (%s, %s, %s, %s)
        """,
        (iid, iid, sic, desc),
    )


def _seed(conn: psycopg.Connection[tuple]) -> None:
    # eToro `sector` is set to DISAGREE with SIC on purpose, to prove the cohort
    # keys off SEC SIC (#2023), not eToro sector:
    #   9003 = same SIC, DIFFERENT eToro sector → still a peer (SIC wins).
    #   9004 = SAME eToro sector, different SIC   → NOT a peer (SIC wins).
    #   9006 = NULL eToro sector, same SIC        → a peer (the +1012 gain).
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, sector, is_tradable)
        VALUES
          (9001, 'PCSELF', 'Self Co',   '99', TRUE),
          (9002, 'PCNEAR', 'Near Peer', '99', TRUE),
          (9003, 'PCFAR',  'Far Peer',  '88', TRUE),
          (9004, 'PCOTHER','Other SIC', '99', TRUE),
          (9005, 'PCNOSEC','No SIC',    NULL, TRUE),
          (9006, 'PCGAIN', 'Gained',    NULL, TRUE)
        """
    )
    # SIC profile — 9001/9002/9003/9006 share SIC 3571; 9004 is 2834 (diff sic2);
    # 9005 has no SIC (target-None path + cohort-excluded).
    _seed_sic(conn, 9001, "3571", "Electronic Computers")
    _seed_sic(conn, 9002, "3571", "Electronic Computers")
    _seed_sic(conn, 9003, "3571", "Electronic Computers")
    _seed_sic(conn, 9004, "2834", "Pharmaceutical Preparations")
    _seed_sic(conn, 9005, None, None)
    _seed_sic(conn, 9006, "3571", "Electronic Computers")
    # self: ttm rev 1000, op 100, ni 80; equity 800, ta 1000, no debt.
    _seed_quarters(conn, 9001, revenue_q=250, op_income_q=25, net_income_q=20, equity=800, total_assets=1000)
    # near peer (ta 1100): ttm rev 500, op 50, ni 25; equity 250.
    _seed_quarters(
        conn,
        9002,
        revenue_q=125,
        op_income_q=12.5,
        net_income_q=6.25,
        equity=250,
        total_assets=1100,
        long_term_debt=50,
        short_term_debt=50,
    )
    # far peer (ta 9e8): same SIC, far by size — last in peer order, still a peer.
    _seed_quarters(conn, 9003, revenue_q=100, op_income_q=5, net_income_q=2.5, equity=100, total_assets=9.0e8)
    # other SIC — same eToro sector as self, must NOT appear as a peer.
    _seed_quarters(conn, 9004, revenue_q=225, op_income_q=22.5, net_income_q=17.5, equity=700, total_assets=1050)
    # no-SIC — target-None path; excluded from cohorts.
    _seed_quarters(conn, 9005, revenue_q=225, op_income_q=22.5, net_income_q=17.5, equity=700, total_assets=1050)
    # gained name (ta 1050, nearest): NULL eToro sector but same SIC → a peer.
    _seed_quarters(conn, 9006, revenue_q=225, op_income_q=22.5, net_income_q=17.5, equity=700, total_assets=1050)
    # FY revenue rows for YoY. self: consecutive (366d) → computable.
    # near peer: 5-year gap → guard rejects → None.
    conn.execute(
        """
        INSERT INTO financial_periods
          (instrument_id, period_end_date, period_type, fiscal_year, source, source_ref,
           reported_currency, revenue, superseded_at, normalization_status)
        VALUES
          (9001, DATE '2024-12-31', 'FY', 2024, 'test', 'fy1', 'USD', 1100, NULL, 'normalized'),
          (9001, DATE '2023-12-31', 'FY', 2023, 'test', 'fy2', 'USD', 1000, NULL, 'normalized'),
          (9002, DATE '2024-12-31', 'FY', 2024, 'test', 'fy3', 'USD',  600, NULL, 'normalized'),
          (9002, DATE '2019-12-31', 'FY', 2019, 'test', 'fy4', 'USD',  500, NULL, 'normalized')
        """
    )
    conn.commit()


@pytest.mark.db
def test_compute_peer_comparison_db(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    _seed(ebull_test_conn)
    result = compute_peer_comparison(ebull_test_conn, instrument_id=9001)
    assert result is not None
    assert result.cohort_sic == "3571"
    assert result.cohort_sic_label == "Electronic Computers"
    # 4 same-SIC members (9001/9002/9003/9006) < MIN_COHORT (8) at every level →
    # walk widens to SIC-2, marker 0 (thin fallback). Level selection per
    # granularity is covered exhaustively by the pure resolve_sic_level test.
    assert result.cohort_sic_level == 0
    assert result.cohort_member_count == 4  # 9001/9002/9003/9006; 9004 (diff SIC) + 9005 (no SIC) excluded

    sf = result.self_factors
    assert sf["operating_margin"] == pytest.approx(0.1)  # 100/1000
    assert sf["net_margin"] == pytest.approx(0.08)  # 80/1000
    assert sf["roe"] == pytest.approx(0.1)  # 80/800
    assert sf["debt_equity_ratio"] == pytest.approx(0.0)  # no debt
    assert sf["revenue_growth_yoy"] == pytest.approx(0.1)  # (1100-1000)/1000, consecutive FY
    assert sf["pe_ratio"] is None  # no price → instrument_valuation excludes it

    # peer order by size proximity to self (ta 1000): 9006 (1050) < 9002 (1100)
    # < 9003 (9e8). Proves SIC drives the cohort: 9006 (null eToro sector) and
    # 9003 (eToro sector '88') are peers; 9004 (same eToro sector '99', SIC 2834)
    # and 9005 (no SIC) are NOT.
    assert [p.instrument_id for p in result.peers] == [9006, 9002, 9003]
    near = result.peers[0]
    assert near.factors["revenue_growth_yoy"] is None  # 9006 has no FY rows → None

    # median of revenue_growth_yoy: only self has a value → median 0.1, n=1.
    assert result.medians["revenue_growth_yoy"].n == 1
    assert result.medians["revenue_growth_yoy"].median == pytest.approx(0.1)

    # Thin-factor policy (#1836; #2023). Coverage branch in isolation (flag off):
    # pe_ratio n=0 → thin; revenue_growth_yoy n=1/4 (25%) clears the 20% cut →
    # NOT thin; roe full coverage → NOT thin. Guards the mapping from regressing.
    mc = result.cohort_member_count
    assert result.medians["pe_ratio"].n == 0
    assert is_factor_thin("pe_ratio", result.medians["pe_ratio"].n, mc) is True
    assert is_factor_thin("revenue_growth_yoy", result.medians["revenue_growth_yoy"].n, mc) is False
    assert is_factor_thin("roe", result.medians["roe"].n, mc) is False
    # But this fixture is a below-threshold fallback cohort (level 0): the API
    # passes cohort_is_fallback → EVERY factor greys, coverage notwithstanding.
    assert is_factor_thin("roe", result.medians["roe"].n, mc, cohort_is_fallback=True) is True


@pytest.mark.db
def test_compute_peer_comparison_none_paths(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    _seed(ebull_test_conn)
    # No SIC classification → None.
    assert compute_peer_comparison(ebull_test_conn, instrument_id=9005) is None
    # Unknown instrument → None.
    assert compute_peer_comparison(ebull_test_conn, instrument_id=999999) is None
