"""Fair-value band IO layer (#2009): DB integration.

Two BLOCKING regression guards over the two-pass compute:

1. ``test_two_pass_cohort_excludes_dual_class_member`` — the §4.3 curated-oracle
   anti-join keeps a dual-class member's P/S and P/B OUT of the pass-2 medians
   while keeping its P/E IN (spec §9, #1662). The oracle predicate lives on
   ``external_identifiers`` joined to ``instrument_class_shares_outstanding`` on
   ``source_cik = lpad(identifier_value,10,'0')`` (sql/201:46-56), NOT on the
   class table directly.

2. ``test_single_name_run_anchors_to_materialized_cohort`` — a single-name
   cascade run anchors to ``max(as_of_date)`` in ``fair_value_cohort_members``
   (``resolve_cohort_members_as_of_date``), NOT the price max, so a post-
   materialize price advance does not silently drop every peer into a peerless
   ``thin_cohort`` band (A7 fix I1).

Pure-policy tests live in ``test_fair_value_band_policy.py`` (fast tier).
"""

from __future__ import annotations

import datetime as dt
from statistics import median

import psycopg
import pytest

from app.services.fair_value_band import (
    MIN_PEERS,
    materialize_cohort_members,
    peer_pct_for,
    refresh_fair_value_band_batch,
    resolve_batch_as_of_date,
    resolve_cohort_members_as_of_date,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 (fixture re-export)

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.db  # auto-applied via conftest; explicit for clarity

_AS_OF = dt.date(2024, 12, 31)
_Q_ENDS = ("2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31")
_Q_TYPES = ("Q1", "Q2", "Q3", "Q4")
_SIC = "3571"  # electronic computers — a single shared SIC-4 cohort
_CLOSE = 100.0
_SHARES = 1000.0


def _clear_band_tables(conn: psycopg.Connection[tuple]) -> None:
    """The fair_value_* tables carry no FK to instruments, so the fixture's
    ``TRUNCATE instruments CASCADE`` never reaches them — clear explicitly for a
    hermetic per-test state (test-local per-worker DB)."""
    for t in ("fair_value_cohort_members", "fair_value_band_current", "fair_value_band_observations"):
        conn.execute(f"DELETE FROM {t}")  # noqa: S608 — fixed identifier, no interpolation of input


def _seed_member(
    conn: psycopg.Connection[tuple],
    iid: int,
    *,
    ps: float,
    pb: float,
    pe: float | None,
    total_assets: float,
    cik: str,
    ev: dict[str, float | None] | None = None,
    margin: float = 0.1,
    prior_revenue_ttm: float | None = None,
) -> None:
    """Seed one instrument so its as-of multiples are exactly (ps, pb, pe).

    With close=100 and shares=1000: revenue_ttm = close*shares/ps ;
    shareholders_equity = close*shares/pb ; eps_diluted_ttm = close/pe. Flow
    items (revenue, net_income, eps_diluted) are present in all 4 quarters
    (strict-TTM requires COUNT=4); stock items (equity, shares, total_assets)
    are read from the latest quarter only. ``pe=None`` seeds a non-positive
    eps_diluted_ttm so the name has NO P/E cohort row (still P/S + P/B).

    ``ev`` (#2021) optionally seeds the EV/EBITDA inputs: flow keys
    ``op``/``da``/``interest`` land in all 4 quarters (strict-TTM), stock keys
    ``ltd``/``std``/``cash`` in the latest quarter only. None keys stay NULL —
    letting a test exercise the strict/coherence gates. Omitted entirely ->
    every EV column NULL -> no ev_ebitda cohort row (the pre-#2021 shape).

    v4 (#2032): ``margin`` fixes net_income_q = revenue_q * margin (the
    net-margin companion); ``prior_revenue_ttm`` seeds 4 ADDITIONAL 2023
    quarters (rn 5-8) carrying prior_revenue_ttm/4 each, so rev_growth_yoy =
    (revenue_ttm - prior_revenue_ttm)/prior_revenue_ttm with the adjacent
    2023-12-31 -> 2024-03-31 window gap (91d, inside [1,120]). Omitted ->
    growth companion NULL (the pre-v4 shape).
    """
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
        "VALUES (%s, %s, %s, 'USD', TRUE)",
        (iid, f"FVB{iid}", f"FVB Co {iid}"),
    )
    conn.execute(
        "INSERT INTO instrument_sec_profile (instrument_id, cik, sic) VALUES (%s, %s, %s)",
        (iid, cik, _SIC),
    )
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, close) VALUES (%s, %s, %s)",
        (iid, _AS_OF, _CLOSE),
    )

    revenue_ttm = _CLOSE * _SHARES / ps
    equity = _CLOSE * _SHARES / pb
    eps_ttm = (_CLOSE / pe) if pe is not None else -4.0  # <=0 -> no P/E cohort row
    revenue_q = revenue_ttm / 4.0
    net_income_q = revenue_q * margin  # positive -> select_multiples sees profit
    eps_q = eps_ttm / 4.0
    ev = ev or {}

    if prior_revenue_ttm is not None:
        for end, qt in zip(("2023-03-31", "2023-06-30", "2023-09-30", "2023-12-31"), _Q_TYPES, strict=True):
            conn.execute(
                """
                INSERT INTO financial_periods
                  (instrument_id, period_end_date, period_type, fiscal_year, source, source_ref,
                   reported_currency, revenue, superseded_at, normalization_status)
                VALUES (%s, %s, %s, 2023, 'test', %s, 'USD', %s, NULL, 'normalized')
                """,
                (iid, end, qt, f"fvb{iid}{qt}p", prior_revenue_ttm / 4.0),
            )

    def _q(key: str) -> float | None:
        v = ev.get(key)
        return None if v is None else v / 4.0

    for end, qt in zip(_Q_ENDS, _Q_TYPES, strict=True):
        latest = end == "2024-12-31"
        conn.execute(
            """
            INSERT INTO financial_periods
              (instrument_id, period_end_date, period_type, fiscal_year, source, source_ref,
               reported_currency, revenue, net_income, eps_diluted,
               operating_income, depreciation_amort, interest_expense,
               total_assets, shareholders_equity, shares_outstanding,
               long_term_debt, short_term_debt, cash,
               superseded_at, normalization_status)
            VALUES (%s, %s, %s, 2024, 'test', %s, 'USD', %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, NULL, 'normalized')
            """,
            (
                iid,
                end,
                qt,
                f"fvb{iid}{qt}",
                revenue_q,
                net_income_q,
                eps_q,
                _q("op"),
                _q("da"),
                _q("interest"),
                total_assets if latest else None,
                equity if latest else None,
                _SHARES if latest else None,
                ev.get("ltd") if latest else None,
                ev.get("std") if latest else None,
                ev.get("cash") if latest else None,
            ),
        )


def _seed_dual_class(
    conn: psycopg.Connection[tuple],
    iid: int,
    *,
    ps: float,
    pb: float,
    pe: float,
    ev: dict[str, float | None] | None = None,
) -> None:
    """A curated dual-class member: seed like a normal member, then attach the
    §4.3 oracle — a primary SEC-CIK ``external_identifiers`` row whose
    ``lpad(identifier_value,10,'0')`` matches an ``instrument_class_shares_
    outstanding.source_cik`` (the predicate the materialize dual_class CTE runs)."""
    _seed_member(conn, iid, ps=ps, pb=pb, pe=pe, total_assets=1.0e9, cik="9999999999", ev=ev)
    cik10 = "0000320193"
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (%s, 'sec', 'cik', %s, TRUE)",
        (iid, cik10),
    )
    conn.execute(
        """
        INSERT INTO instrument_class_shares_outstanding
          (instrument_id, period_end, shares, class_member, source_cik, source_adsh,
           source_form_type, source_fsds_qtr, source_filed_at, resolution_method, parser_version)
        VALUES (%s, %s, %s, 'CommonClassA', %s, '0000320193-24-000001', '10-K', '2024q4',
                %s, 'curated', 'fsds_class_shares_v1')
        """,
        (iid, _AS_OF, _SHARES, cik10, _AS_OF),
    )


# 8 single-class peers: P/S 10..17, P/B all 2.0; 8001-8007 profitable (P/E 20..26),
# 8008 loss-making (no P/E cohort row). One dual-class member (8009) with rich
# multiples that MUST be excluded from the P/S & P/B medians but kept for P/E.
_TARGET = 8000
_SINGLE = {
    8001: (10.0, 2.0, 20.0),
    8002: (11.0, 2.0, 21.0),
    8003: (12.0, 2.0, 22.0),
    8004: (13.0, 2.0, 23.0),
    8005: (14.0, 2.0, 24.0),
    8006: (15.0, 2.0, 25.0),
    8007: (16.0, 2.0, 26.0),
    8008: (17.0, 2.0, None),  # loss-maker: in P/S + P/B, not P/E
}
_DUAL = 8009
_DUAL_MULTS = (100.0, 50.0, 100.0)  # ps, pb, pe — off-distribution so leakage is detectable


def _seed_cohort(conn: psycopg.Connection[tuple]) -> None:
    _clear_band_tables(conn)
    # Target: profitable single-class name in the same SIC-4.
    _seed_member(conn, _TARGET, ps=8.0, pb=1.5, pe=18.0, total_assets=5.0e9, cik="1000000000")
    for iid, (ps, pb, pe) in _SINGLE.items():
        _seed_member(conn, iid, ps=ps, pb=pb, pe=pe, total_assets=1.0e9, cik=str(1_000_000_000 + iid))
    _seed_dual_class(conn, _DUAL, ps=_DUAL_MULTS[0], pb=_DUAL_MULTS[1], pe=_DUAL_MULTS[2])
    conn.commit()


def test_two_pass_cohort_excludes_dual_class_member(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    """BLOCKING (spec §9): the curated-oracle anti-join keeps the dual-class
    member's P/S and P/B out of the pass-2 medians but keeps its P/E in."""
    conn = ebull_test_conn
    _seed_cohort(conn)

    materialize_cohort_members(conn, _AS_OF)

    # 1. The dual-class member is flagged suppressed on ALL of its cohort rows.
    rows = conn.execute(
        "SELECT multiple, dual_class_suppressed FROM fair_value_cohort_members "
        "WHERE instrument_id = %s AND as_of_date = %s ORDER BY multiple",
        (_DUAL, _AS_OF),
    ).fetchall()
    assert rows == [("pb", True), ("pe", True), ("ps", True)]

    # No single-class peer is suppressed (LEFT JOIN dual_class -> NULL).
    suppressed_row = conn.execute(
        "SELECT count(*) FROM fair_value_cohort_members "
        "WHERE as_of_date = %s AND dual_class_suppressed AND instrument_id <> %s",
        (_AS_OF, _DUAL),
    ).fetchone()
    assert suppressed_row is not None and suppressed_row[0] == 0

    # 2. P/S median: dual member (ps=100) excluded -> median over the 8
    #    single-class members {10..17} = 13.5; cohort_n counts 8 (not 9).
    #    (revenue/equity are full-precision numeric -> ps/pb are exact.)
    ps_pct, ps_meta = peer_pct_for(conn, _TARGET, _SIC, 5.0e9, "ps", _AS_OF)
    expected_ps = median([v[0] for v in _SINGLE.values()])
    assert ps_pct.p50 is not None and ps_pct.p75 is not None
    assert ps_meta["cohort_n"] == len(_SINGLE)  # 8, dual excluded from the member set
    assert ps_pct.p50 == pytest.approx(expected_ps)
    assert ps_pct.p50 == pytest.approx(13.5)
    assert ps_pct.p75 < _DUAL_MULTS[0]  # the ps=100 dual value never reaches the tail

    # 3. P/B median: dual member (pb=50) excluded -> all single-class pb=2.0.
    pb_pct, pb_meta = peer_pct_for(conn, _TARGET, _SIC, 5.0e9, "pb", _AS_OF)
    assert pb_pct.p50 is not None and pb_pct.p75 is not None
    assert pb_meta["cohort_n"] == len(_SINGLE)
    assert pb_pct.p50 == pytest.approx(2.0)
    assert pb_pct.p75 < _DUAL_MULTS[1]

    # 4. P/E: dual member (pe=100) IS included. Members = 8001..8007 (pe 20..26)
    #    + dual (pe 100) = 8. median of [20,21,22,23,24,25,26,100] = 23.5, which
    #    is STRICTLY ABOVE the 23.0 it would be if the dual member were dropped —
    #    so ~23.5 proves inclusion, not just a coincidental match. (eps_diluted is
    #    numeric(12,4) so the reconstructed P/E drifts ~1e-3; the 23.5-vs-23.0
    #    discriminator is >> that drift, so a loose abs tolerance is exact enough.)
    pe_pct, pe_meta = peer_pct_for(conn, _TARGET, _SIC, 5.0e9, "pe", _AS_OF)
    assert pe_pct.p50 is not None
    assert pe_meta["cohort_n"] == 8  # 7 profitable single-class + dual
    assert pe_pct.p50 == pytest.approx(23.5, abs=0.05)  # dual (pe=100) pulls the p50 up
    assert abs(pe_pct.p50 - 23.0) > 0.25  # NOT the 23.0 of the dual-excluded 7-member set


def test_single_name_run_anchors_to_materialized_cohort(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    """A7 fix I1: a single-name cascade run anchors to the materialized cohort
    ``max(as_of_date)``, NOT the price max — a post-materialize price advance
    must not drop every peer into a peerless ``thin_cohort`` band."""
    conn = ebull_test_conn
    _seed_cohort(conn)

    # Full materialize at D=_AS_OF, then advance price_daily past D so the price
    # anchor and the cohort anchor DIVERGE.
    materialize_cohort_members(conn, _AS_OF)
    conn.commit()
    newer = _AS_OF + dt.timedelta(days=3)
    for iid in (_TARGET, *_SINGLE, _DUAL):
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, close) VALUES (%s, %s, %s)",
            (iid, newer, _CLOSE),
        )
    conn.commit()

    # Precondition: the two anchors now differ (guards the test itself).
    assert resolve_batch_as_of_date(conn) == newer
    assert resolve_cohort_members_as_of_date(conn) == _AS_OF

    result = refresh_fair_value_band_batch(conn, [_TARGET])
    conn.commit()
    assert result == {"written": 1, "statused": 0, "failed": 0}

    row = conn.execute(
        "SELECT as_of_date, reason, base_value FROM fair_value_band_current WHERE instrument_id = %s",
        (_TARGET,),
    ).fetchone()
    assert row is not None
    as_of_date, reason, base_value = row
    # Anchored to the materialized cohort date (D), NOT the advanced price date.
    assert as_of_date == _AS_OF
    # Peers were found (>= MIN_PEERS) -> a real band, not peerless thin_cohort.
    assert reason == "ok"
    assert base_value is not None

    # Sanity: the peer read at the cohort anchor still clears MIN_PEERS.
    _, ps_meta = peer_pct_for(conn, _TARGET, _SIC, 5.0e9, "ps", _AS_OF)
    assert ps_meta["cohort_n"] >= MIN_PEERS


# --- #2021 EV/EBITDA (fvb_v3): pass-1 arm + gates + anti-join ---
# One integration test for the genuinely-new SQL mechanism (test-quality skill):
# the ev_ebitda materialize arm with its strict/coherence gates, plus the
# keep_dual=False routing through the existing anti-join.

_EV_TARGET_ID = 8199  # never seeded — peer_pct_for just needs a non-member id
_EV_BASE = {"ltd": 0.0, "std": 0.0, "cash": 0.0}  # EV == close*shares == 100_000
# ebitda = 100_000 / mult, all divisible by 4 so quarterly flows are exact.
_EV_SINGLE = {
    8101: 8.0,
    8102: 10.0,
    8103: 16.0,
    8104: 20.0,
    8105: 25.0,
    8106: 40.0,
    8107: 50.0,
    8108: 80.0,
}
_EV_DUAL = 8109  # ev mult 100 — off-distribution so leakage is detectable
_EV_GATED_CASH = 8110  # cash NULL -> ev row must not materialize
_EV_GATED_DEBT = 8111  # debt-both-NULL + positive interest -> incoherent, no ev row


def _ev_inputs(mult: float, **overrides: float | None) -> dict[str, float | None]:
    ebitda = _CLOSE * _SHARES / mult
    out: dict[str, float | None] = {"op": ebitda * 0.8, "da": ebitda * 0.2, **_EV_BASE}
    out.update(overrides)
    return out


def test_two_pass_ev_ebitda_arm_gates_and_anti_join(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    """BLOCKING (#2021 spec §3.6): the ev_ebitda pass-1 arm materializes exact
    multiples for gate-passing members only (strict D&A + cash-present +
    debt/interest coherence), and the dual-class member's ev row is excluded
    from the pass-2 median via the existing keep_dual=False anti-join."""
    conn = ebull_test_conn
    _clear_band_tables(conn)
    for iid, mult in _EV_SINGLE.items():
        _seed_member(
            conn, iid, ps=10.0, pb=2.0, pe=20.0, total_assets=1.0e9, cik=str(1_000_000_000 + iid), ev=_ev_inputs(mult)
        )
    _seed_dual_class(conn, _EV_DUAL, ps=10.0, pb=2.0, pe=20.0, ev=_ev_inputs(100.0))
    _seed_member(
        conn,
        _EV_GATED_CASH,
        ps=10.0,
        pb=2.0,
        pe=20.0,
        total_assets=1.0e9,
        cik=str(1_000_000_000 + _EV_GATED_CASH),
        ev=_ev_inputs(10.0, cash=None),
    )
    _seed_member(
        conn,
        _EV_GATED_DEBT,
        ps=10.0,
        pb=2.0,
        pe=20.0,
        total_assets=1.0e9,
        cik=str(1_000_000_000 + _EV_GATED_DEBT),
        ev=_ev_inputs(10.0, ltd=None, std=None, interest=4_000.0),
    )
    conn.commit()

    materialize_cohort_members(conn, _AS_OF)

    # 1. Gate-passing members materialize with the EXACT multiple (EV=100_000).
    row = conn.execute(
        "SELECT mult_value FROM fair_value_cohort_members "
        "WHERE as_of_date = %s AND multiple = 'ev_ebitda' AND instrument_id = %s",
        (_AS_OF, 8101),
    ).fetchone()
    assert row is not None and float(row[0]) == pytest.approx(8.0)

    # 2. Gated members have NO ev row but DO have their ps row — proving the
    #    gates bind the ev arm only, not the whole name.
    for gated in (_EV_GATED_CASH, _EV_GATED_DEBT):
        counts = conn.execute(
            "SELECT count(*) FILTER (WHERE multiple = 'ev_ebitda'), "
            "       count(*) FILTER (WHERE multiple = 'ps') "
            "FROM fair_value_cohort_members WHERE as_of_date = %s AND instrument_id = %s",
            (_AS_OF, gated),
        ).fetchone()
        assert counts is not None and (counts[0], counts[1]) == (0, 1), gated

    # 3. The dual-class member's ev row exists flagged, and the pass-2 median
    #    excludes it (keep_dual=False for every cap-based multiple): cohort = the
    #    8 single-class members, median of {8,10,16,20,25,40,50,80} = 22.5.
    dual_row = conn.execute(
        "SELECT dual_class_suppressed FROM fair_value_cohort_members "
        "WHERE as_of_date = %s AND multiple = 'ev_ebitda' AND instrument_id = %s",
        (_AS_OF, _EV_DUAL),
    ).fetchone()
    assert dual_row is not None and dual_row[0] is True
    ev_pct, ev_meta = peer_pct_for(conn, _EV_TARGET_ID, _SIC, 1.0e9, "ev_ebitda", _AS_OF)
    assert ev_meta["cohort_n"] == len(_EV_SINGLE)  # dual + gated never in the member set
    assert ev_pct.p50 is not None and ev_pct.p50 == pytest.approx(22.5)
    assert ev_pct.p75 is not None and ev_pct.p75 < 100.0  # dual's 100x never reaches the tail


# --- #2032 (fvb_v4): companion-variable peer screen, end-to-end ---
# One integration test for the genuinely-new SQL mechanism (test-quality skill):
# the pass-1 companion columns + the pass-2 screened walk + the real _TARGET_SQL
# prior-TTM lateral (dict-row key + projection in the same diff — prevention
# log #2021), covering the held path, the target-companion-missing fallback,
# and the no-screened-cohort fallback.

_SCR_TARGET = 8300  # margin 0.10, growth 0.25 — screen holds at tier 0 / SIC-4
_SCR_NO_COMP = 8320  # no prior quarters -> growth NULL -> target_companion_missing
_SCR_UNMATCHED = 8330  # margin 0.60 vs near peers' 0.10 -> no_screened_cohort
# 8 near peers (margin 0.10, growth 0.25) + 4 far peers (margin 0.60, growth
# 3.0, off-distribution ps=100) the screen must exclude at every tier.
_SCR_NEAR = {8301: 10.0, 8302: 11.0, 8303: 12.0, 8304: 13.0, 8305: 14.0, 8306: 15.0, 8307: 16.0, 8308: 17.0}
_SCR_FAR = (8311, 8312, 8313, 8314)


def _seed_screen_cohort(conn: psycopg.Connection[tuple]) -> None:
    _clear_band_tables(conn)
    _seed_member(
        conn,
        _SCR_TARGET,
        ps=8.0,
        pb=1.5,
        pe=18.0,
        total_assets=5.0e9,
        cik=str(1_000_000_000 + _SCR_TARGET),
        margin=0.10,
        prior_revenue_ttm=(_CLOSE * _SHARES / 8.0) / 1.25,
    )
    _seed_member(
        conn,
        _SCR_NO_COMP,
        ps=8.0,
        pb=1.5,
        pe=18.0,
        total_assets=5.0e9,
        cik=str(1_000_000_000 + _SCR_NO_COMP),
        margin=0.10,
    )
    _seed_member(
        conn,
        _SCR_UNMATCHED,
        ps=8.0,
        pb=1.5,
        pe=18.0,
        total_assets=5.0e9,
        cik=str(1_000_000_000 + _SCR_UNMATCHED),
        margin=0.60,
        prior_revenue_ttm=(_CLOSE * _SHARES / 8.0) / 1.25,
    )
    for iid, ps in _SCR_NEAR.items():
        _seed_member(
            conn,
            iid,
            ps=ps,
            pb=2.0,
            pe=20.0,
            total_assets=1.0e9,
            cik=str(1_000_000_000 + iid),
            margin=0.10,
            prior_revenue_ttm=(_CLOSE * _SHARES / ps) / 1.25,
        )
    for iid in _SCR_FAR:
        # total_assets matches the targets' 5e9 so the UNSCREENED size-refine
        # keeps the far peers (nearest-8 by |ln assets|) — the screened/held
        # path must exclude them by COMPANION distance, not by size.
        _seed_member(
            conn,
            iid,
            ps=100.0,
            pb=2.0,
            pe=20.0,
            total_assets=5.0e9,
            cik=str(1_000_000_000 + iid),
            margin=0.60,
            prior_revenue_ttm=(_CLOSE * _SHARES / 100.0) / 4.0,
        )
    conn.commit()


def test_companion_screen_end_to_end(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _seed_screen_cohort(conn)

    materialize_cohort_members(conn, _AS_OF)
    conn.commit()

    # 1. Pass-1 wrote the companion columns (near peer: margin .1, growth .25).
    row = conn.execute(
        "SELECT net_margin, rev_growth_yoy, roe FROM fair_value_cohort_members "
        "WHERE as_of_date = %s AND multiple = 'ps' AND instrument_id = %s",
        (_AS_OF, 8301),
    ).fetchone()
    assert row is not None
    assert float(row[0]) == pytest.approx(0.10, abs=1e-6)
    assert float(row[1]) == pytest.approx(0.25, abs=1e-6)
    assert row[2] is not None  # roe = ni_ttm/equity, positive here
    # No-prior name: growth NULL, margin still present.
    row = conn.execute(
        "SELECT net_margin, rev_growth_yoy FROM fair_value_cohort_members "
        "WHERE as_of_date = %s AND multiple = 'ps' AND instrument_id = %s",
        (_AS_OF, _SCR_NO_COMP),
    ).fetchone()
    assert row is not None and row[0] is not None and row[1] is None

    # 2. HELD: the batch path (real _TARGET_SQL incl. the prior-TTM lateral ->
    #    companion_vars -> screened peer walk) screens the 4 far peers out at
    #    tier 0 / SIC-4 with full provenance in basis_json.
    result = refresh_fair_value_band_batch(conn, [_SCR_TARGET])
    conn.commit()
    assert result == {"written": 1, "statused": 0, "failed": 0}
    basis = conn.execute(
        "SELECT basis_json FROM fair_value_band_current WHERE instrument_id = %s",
        (_SCR_TARGET,),
    ).fetchone()
    assert basis is not None
    ps_entry = basis[0]["multiples"]["ps"]
    assert ps_entry["cohort_screened"] is True
    assert ps_entry["screen"] == {"sic_level": 4, "width_tier": 0, "survivors_n": len(_SCR_NEAR)}
    assert ps_entry["sic_level"] == 4
    # Screened p50 = median of the 8 near multiples {10..17} = 13.5; the far
    # 100x members never reach the percentiles.
    assert ps_entry["peer"]["p50"] == pytest.approx(13.5, abs=0.01)
    assert ps_entry["peer"]["p75"] < 100.0
    # pe leg is never screened — no screen keys (spec §4.2).
    pe_entry = basis[0]["multiples"]["pe"]
    assert "cohort_screened" not in pe_entry and "screen" not in pe_entry

    # 3. FALLBACK target_companion_missing: growth NULL on the target -> the
    #    unscreened cohort (far peers INCLUDED: 12 members, p75 pulled above the
    #    near-only tail) + the flag.
    result = refresh_fair_value_band_batch(conn, [_SCR_NO_COMP])
    conn.commit()
    assert result == {"written": 1, "statused": 0, "failed": 0}
    basis = conn.execute(
        "SELECT basis_json FROM fair_value_band_current WHERE instrument_id = %s",
        (_SCR_NO_COMP,),
    ).fetchone()
    assert basis is not None
    ps_entry = basis[0]["multiples"]["ps"]
    assert ps_entry["cohort_screened"] is False
    assert ps_entry["screen"] == {"reason": "target_companion_missing"}
    assert ps_entry["peer"]["p75"] > 17.0  # far 100x members present unscreened

    # 4. FALLBACK no_screened_cohort: companions present but margin 0.60 matches
    #    neither the 8 near (0.10) nor enough far peers at any tier.
    result = refresh_fair_value_band_batch(conn, [_SCR_UNMATCHED])
    conn.commit()
    assert result == {"written": 1, "statused": 0, "failed": 0}
    basis = conn.execute(
        "SELECT basis_json FROM fair_value_band_current WHERE instrument_id = %s",
        (_SCR_UNMATCHED,),
    ).fetchone()
    assert basis is not None
    ps_entry = basis[0]["multiples"]["ps"]
    assert ps_entry["cohort_screened"] is False
    assert ps_entry["screen"] == {"reason": "no_screened_cohort"}


def test_companion_screen_pre_v4_cohort_annotates_companions_missing(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:  # noqa: F811
    """Codex ckpt-2 P2 (deploy->backfill window): a single-name cascade anchored
    to a cohort materialized BEFORE the sql/228 backfill (companion columns all
    NULL) must annotate the fallback as cohort_companions_missing — never the
    misleading no_screened_cohort — while producing the identical unscreened
    (pre-v4) peer stats."""
    conn = ebull_test_conn
    _seed_screen_cohort(conn)
    materialize_cohort_members(conn, _AS_OF)
    # Simulate the pre-v4 member set: strip every companion column.
    conn.execute("UPDATE fair_value_cohort_members SET net_margin = NULL, rev_growth_yoy = NULL, roe = NULL")
    conn.commit()

    result = refresh_fair_value_band_batch(conn, [_SCR_TARGET])
    conn.commit()
    assert result == {"written": 1, "statused": 0, "failed": 0}
    basis = conn.execute(
        "SELECT basis_json FROM fair_value_band_current WHERE instrument_id = %s",
        (_SCR_TARGET,),
    ).fetchone()
    assert basis is not None
    ps_entry = basis[0]["multiples"]["ps"]
    assert ps_entry["cohort_screened"] is False
    assert ps_entry["screen"] == {"reason": "cohort_companions_missing"}
    # Unscreened stats: all 14 sibling members (near 8 + far 4 + 2 co-targets)
    # feed the walk; the far 100x members reach the tail unscreened.
    assert ps_entry["peer"]["p75"] > 17.0
