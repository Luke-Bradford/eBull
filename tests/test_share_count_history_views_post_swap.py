"""Functional regression for the share-count views.

Migration 156 drops + re-creates `share_count_history` /
`instrument_dilution_summary` / `instrument_share_count_latest`
verbatim from sql/052 (the views can't survive `DROP TABLE
financial_facts_raw` and must be re-installed after the partitioned
parent's swap-rename). This test seeds facts + verifies the
re-installed views produce expected rows — a behavioural anchor in
case the inline view bodies in 156 drift from sql/052.

Migration 259 (#2232) is the CURRENT owner of all three bodies: the
point-in-time share-count columns select POSITIVE values only, so a
filer's zero-valued undimensioned tag can no longer mask a usable
figure. `TestPositiveOnlyShareCount` pins that behaviour and the
deliberate asymmetry that leaves the FLOW columns taking zeros.
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
    taxonomy: str = "dei",
    accession_number: str | None = None,
    filed_date: date | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO financial_facts_raw (
            instrument_id, taxonomy, concept, unit, period_end, val,
            accession_number, form_type, filed_date
        ) VALUES (%s, %s, %s, 'shares', %s, %s, %s, '10-K', %s)
        """,
        (
            instrument_id,
            taxonomy,
            concept,
            period_end,
            shares,
            accession_number or f"acc-{taxonomy}-{period_end.isoformat()}-{concept}",
            filed_date or period_end,
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


class TestPositiveOnlyShareCount:
    """Migration 259 (#2232) — the point-in-time share count is selected from
    POSITIVE values only.

    A filer whose share classes are tagged dimensionally leaves the
    undimensioned line at zero, and companyfacts strips the dimensional facts
    (sec-edgar skill §7.17), so a literal ``0`` is what lands in
    ``financial_facts_raw``. Verified against SEC EDGAR for Chime
    (CIK 0001795586): ``us-gaap:CommonStockSharesOutstanding`` is 0 at
    2025-12-31 and 66,950,736 at 2024-12-31.
    """

    def test_history_never_emits_a_non_positive_stock_count(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """The single enforcement point. Both dependent views restate a ``> 0``
        predicate, but neither can be probed independently — once the base
        view's FILTERs are positive-only, ``shares_outstanding`` is NULL or
        positive and nothing downstream can observe the difference. This is the
        invariant they lean on, so it is pinned here directly."""
        iid = 30015
        _seed_instrument(ebull_test_conn, instrument_id=iid)
        _seed_outstanding(ebull_test_conn, instrument_id=iid, period_end=date(2025, 12, 31), shares=0)
        _seed_outstanding(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 12, 31),
            shares=0,
            concept="CommonStockSharesOutstanding",
            taxonomy="us-gaap",
        )

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT shares_outstanding_dei, shares_outstanding_gaap, shares_outstanding "
                "  FROM share_count_history WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row == (None, None, None)

    def test_newer_zero_does_not_mask_older_positive(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """The CHYM / LIFE / GLOO shape: a zero at the newest period must not
        win ``period_end DESC`` over a usable earlier figure."""
        iid = 30010
        _seed_instrument(ebull_test_conn, instrument_id=iid)
        _seed_outstanding(ebull_test_conn, instrument_id=iid, period_end=date(2024, 12, 31), shares=66_950_736)
        _seed_outstanding(ebull_test_conn, instrument_id=iid, period_end=date(2025, 12, 31), shares=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT latest_shares, as_of_date, source_taxonomy "
                "  FROM instrument_share_count_latest WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert int(row[0]) == 66_950_736
        assert row[1] == date(2024, 12, 31)
        assert row[2] == "dei"

    def test_zero_dei_does_not_mask_positive_gaap_in_same_period(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """The VIPS shape: the DEI-preferred COALESCE must skip a zero DEI and
        fall through to the us-gaap figure at the SAME period, rather than
        returning the zero because zero is not NULL."""
        iid = 30011
        _seed_instrument(ebull_test_conn, instrument_id=iid)
        _seed_outstanding(ebull_test_conn, instrument_id=iid, period_end=date(2025, 12, 31), shares=0)
        _seed_outstanding(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 12, 31),
            shares=111_665_972,
            concept="CommonStockSharesOutstanding",
            taxonomy="us-gaap",
        )

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT shares_outstanding_dei, shares_outstanding_gaap, shares_outstanding "
                "  FROM share_count_history WHERE instrument_id = %s",
                (iid,),
            )
            hist = cur.fetchone()
            cur.execute(
                "SELECT latest_shares, source_taxonomy FROM instrument_share_count_latest  WHERE instrument_id = %s",
                (iid,),
            )
            latest = cur.fetchone()
        assert hist is not None
        assert hist[0] is None  # the zero DEI value is not offered at all
        assert int(hist[1]) == 111_665_972
        assert int(hist[2]) == 111_665_972
        assert latest is not None
        assert int(latest[0]) == 111_665_972
        assert latest[1] == "us-gaap"

    def test_zero_only_instrument_yields_no_denominator_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """The CWAN / LLYVA shape: nothing positive on file means the
        instrument drops out of the denominator view entirely, rather than
        surfacing a zero the consumer has to reject."""
        iid = 30012
        _seed_instrument(ebull_test_conn, instrument_id=iid)
        _seed_outstanding(ebull_test_conn, instrument_id=iid, period_end=date(2025, 3, 31), shares=0)
        _seed_outstanding(ebull_test_conn, instrument_id=iid, period_end=date(2025, 12, 31), shares=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM instrument_share_count_latest WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert int(row[0]) == 0

    def test_zero_latest_does_not_render_as_total_buyback(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """``instrument_dilution_summary`` fed a zero in the latest slot
        reported exactly -100% YoY and posture ``buyback_heavy`` — a scoring
        input that read "they repurchased every share" off a tagging artefact.
        """
        iid = 30013
        _seed_instrument(ebull_test_conn, instrument_id=iid)
        for q_end in (
            date(2024, 3, 31),
            date(2024, 6, 30),
            date(2024, 9, 30),
            date(2024, 12, 31),
            date(2025, 3, 31),
        ):
            _seed_outstanding(ebull_test_conn, instrument_id=iid, period_end=q_end, shares=16_379_906)
        _seed_outstanding(ebull_test_conn, instrument_id=iid, period_end=date(2025, 6, 30), shares=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT latest_shares, latest_as_of, net_dilution_pct_yoy, dilution_posture "
                "  FROM instrument_dilution_summary WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert int(row[0]) == 16_379_906
        assert row[1] == date(2025, 3, 31)
        assert row[3] == "stable"
        assert row[2] is None or abs(float(row[2])) < 0.01

    def test_flow_columns_still_take_zeros(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """The deliberate asymmetry: zero is a meaningful value for a FLOW (no
        issuance this period), so only the point-in-time STOCK columns are
        positive-only. A blanket ``val > 0`` on the CTE would silently turn a
        real "issued nothing" quarter into a gap."""
        iid = 30014
        _seed_instrument(ebull_test_conn, instrument_id=iid)
        _seed_outstanding(ebull_test_conn, instrument_id=iid, period_end=date(2025, 3, 31), shares=5_000_000)
        _seed_outstanding(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 3, 31),
            shares=0,
            concept="StockIssuedDuringPeriodSharesNewIssues",
        )

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT shares_outstanding, shares_issued_new FROM share_count_history  WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert int(row[0]) == 5_000_000
        assert row[1] is not None
        assert int(row[1]) == 0


class TestShareCountFiledDate:
    """#2411 / sql/273 — ``shares_outstanding_filed_date`` is the filed date OF THE
    COUNT, not of whatever else was filed in the same period.

    ``latest_filed_date`` is ``MAX(filed_date)`` across all five grouped concepts, so
    it can only overstate the count's freshness. Two consumers now bound the
    ``share_count_filed`` input on this value, and a bound built on the MAX fails open —
    it was reading 16 of 4,665 dev rows too fresh, 7 of them across the 183-day line.
    """

    def test_a_later_flow_fact_does_not_freshen_the_count(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """The HUIZ shape: the count is filed once, then a flow fact for the same
        period is re-tagged years later. ``latest_filed_date`` follows the flow;
        the count's own date must not."""
        iid = 30021
        _seed_instrument(ebull_test_conn, instrument_id=iid)
        _seed_outstanding(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2019, 12, 31),
            shares=483_310_373,
            filed_date=date(2020, 4, 24),
        )
        _seed_outstanding(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2019, 12, 31),
            shares=1_000,
            concept="StockIssuedDuringPeriodSharesNewIssues",
            filed_date=date(2024, 4, 19),
        )

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT shares_outstanding, latest_filed_date, shares_outstanding_filed_date"
                "  FROM share_count_history WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert int(row[0]) == 483_310_373
        # latest_filed_date is KEPT as MAX-over-everything — it answers a different
        # question and has other readers.
        assert row[1] == date(2024, 4, 19)
        assert row[2] == date(2020, 4, 24)

    def test_filed_date_follows_the_arm_the_value_came_from(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """The VIPS shape again, asked of the date: when a zero DEI makes the
        COALESCE fall through to us-gaap, the reported filing must be the us-gaap
        one. A date taken from the DEI arm would describe a value nobody used."""
        iid = 30022
        _seed_instrument(ebull_test_conn, instrument_id=iid)
        _seed_outstanding(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 12, 31),
            shares=0,
            filed_date=date(2026, 6, 1),
        )
        _seed_outstanding(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2025, 12, 31),
            shares=111_665_972,
            concept="CommonStockSharesOutstanding",
            taxonomy="us-gaap",
            filed_date=date(2026, 2, 13),
        )

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT shares_outstanding, shares_outstanding_filed_date"
                "  FROM share_count_history WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert int(row[0]) == 111_665_972
        assert row[1] == date(2026, 2, 13)

    def test_dei_preference_holds_for_the_date_too(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Both arms positive: the value takes DEI, so the date must take DEI —
        even when the us-gaap fact was filed later. Value and date have to name
        the same fact or the pair is incoherent."""
        iid = 30023
        _seed_instrument(ebull_test_conn, instrument_id=iid)
        _seed_outstanding(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2026, 3, 31),
            shares=500_000,
            filed_date=date(2026, 5, 1),
        )
        _seed_outstanding(
            ebull_test_conn,
            instrument_id=iid,
            period_end=date(2026, 3, 31),
            shares=499_000,
            concept="CommonStockSharesOutstanding",
            taxonomy="us-gaap",
            filed_date=date(2026, 7, 30),
        )

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT shares_outstanding, shares_outstanding_filed_date"
                "  FROM share_count_history WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert int(row[0]) == 500_000
        assert row[1] == date(2026, 5, 1)
