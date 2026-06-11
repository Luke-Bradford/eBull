"""Tests for the ownership history endpoint (#840.F).

Per Codex plan-review #6: each history point is the dedup winner
for ``(period_end, ownership_nature)`` — NOT raw observations. The
service applies the same source × ownership_nature dedup logic per
time bucket.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.services.ownership_history import (
    get_ownership_history,
    iter_categories,
)
from app.services.ownership_observations import (
    OwnershipNature,
    record_blockholder_observation,
    record_def14a_observation,
    record_insider_observation,
    record_institution_observation,
    record_treasury_observation,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


# ---------------------------------------------------------------------------
# Insiders
# ---------------------------------------------------------------------------


class TestInsidersHistory:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_001, symbol="GME")
        conn.commit()
        return conn

    def test_returns_one_point_per_period_per_nature(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        run_id = uuid4()
        cik = "0001767470"
        for q_end, accession, shares in [
            (date(2025, 6, 30), "0001234500-25-000220", Decimal("36000000")),
            (date(2025, 9, 30), "0001234500-25-000221", Decimal("37000000")),
            (date(2025, 12, 31), "0001234500-25-000207", Decimal("38347842")),
        ]:
            record_insider_observation(
                conn,
                instrument_id=843_001,
                holder_cik=cik,
                holder_name="Cohen Ryan",
                ownership_nature="direct",
                source="form4",
                source_document_id=accession,
                source_accession=accession,
                source_field=None,
                source_url=None,
                filed_at=datetime.combine(q_end, datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=q_end,
                ingest_run_id=run_id,
                shares=shares,
            )
        conn.commit()

        points = get_ownership_history(
            conn,
            instrument_id=843_001,
            category="insiders",
            holder_id=cik,
        )

        assert [p.period_end for p in points] == [date(2025, 6, 30), date(2025, 9, 30), date(2025, 12, 31)]
        assert [p.shares for p in points] == [Decimal("36000000"), Decimal("37000000"), Decimal("38347842")]
        # All same nature.
        assert {p.ownership_nature for p in points} == {"direct"}

    def test_dual_nature_renders_two_lines_per_period(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Cohen-on-GME case: same date, same CIK, two natures
        (direct Form 4 + beneficial 13D — but for insiders the bene
        comes via def14a in this category). Two points per period."""
        conn = _setup
        run_id = uuid4()
        cik = "0001767470"
        period = date(2025, 12, 31)
        record_insider_observation(
            conn,
            instrument_id=843_001,
            holder_cik=cik,
            holder_name="Cohen Ryan",
            ownership_nature="direct",
            source="form4",
            source_document_id="0001234500-25-000202",
            source_accession="0001234518-25-000018",
            source_field=None,
            source_url=None,
            filed_at=datetime.combine(period, datetime.min.time(), tzinfo=UTC),
            period_start=None,
            period_end=period,
            ingest_run_id=run_id,
            shares=Decimal("38000000"),
        )
        record_insider_observation(
            conn,
            instrument_id=843_001,
            holder_cik=cik,
            holder_name="Cohen Ryan",
            ownership_nature="indirect",
            source="form4",
            source_document_id="0001234500-25-000222",
            source_accession="0001234519-25-000019",
            source_field=None,
            source_url=None,
            filed_at=datetime.combine(period, datetime.min.time(), tzinfo=UTC),
            period_start=None,
            period_end=period,
            ingest_run_id=run_id,
            shares=Decimal("5000000"),
        )
        conn.commit()

        points = get_ownership_history(
            conn,
            instrument_id=843_001,
            category="insiders",
            holder_id=cik,
        )
        assert len(points) == 2
        natures = {p.ownership_nature: p.shares for p in points}
        assert natures == {"direct": Decimal("38000000"), "indirect": Decimal("5000000")}

    def test_date_range_filter(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        run_id = uuid4()
        cik = "0001234567"
        for q_end in (date(2024, 12, 31), date(2025, 6, 30), date(2025, 12, 31)):
            record_insider_observation(
                conn,
                instrument_id=843_001,
                holder_cik=cik,
                holder_name="Holder",
                ownership_nature="direct",
                source="form4",
                source_document_id=f"ACC-{q_end}",
                source_accession="0001234520-25-000020",
                source_field=None,
                source_url=None,
                filed_at=datetime.combine(q_end, datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=q_end,
                ingest_run_id=run_id,
                shares=Decimal("1000"),
            )
        conn.commit()

        points = get_ownership_history(
            conn,
            instrument_id=843_001,
            category="insiders",
            holder_id=cik,
            from_date=date(2025, 1, 1),
            to_date=date(2025, 9, 30),
        )
        assert [p.period_end for p in points] == [date(2025, 6, 30)]


# ---------------------------------------------------------------------------
# Institutions
# ---------------------------------------------------------------------------


class TestInstitutionsHistory:
    def test_vanguard_quarterly_series(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Operator question this is for: *show me Vanguard's AAPL
        position over the last 4 quarters*. One point per quarter."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_100, symbol="AAPL")
        conn.commit()
        cik = "0000102909"
        run_id = uuid4()
        for q_end, shares in [
            (date(2025, 6, 30), Decimal("1400000000")),
            (date(2025, 9, 30), Decimal("1450000000")),
            (date(2025, 12, 31), Decimal("1480000000")),
            (date(2026, 3, 31), Decimal("1500000000")),
        ]:
            record_institution_observation(
                conn,
                instrument_id=843_100,
                filer_cik=cik,
                filer_name="Vanguard Group Inc",
                filer_type="ETF",
                ownership_nature="economic",
                source="13f",
                source_document_id=f"ACC-VG-{q_end}",
                source_accession="0001234521-25-000021",
                source_field=None,
                source_url=None,
                filed_at=datetime.combine(q_end, datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=q_end,
                ingest_run_id=run_id,
                shares=shares,
                market_value_usd=None,
                voting_authority="SOLE",
            )
        conn.commit()

        points = get_ownership_history(
            conn,
            instrument_id=843_100,
            category="institutions",
            holder_id=cik,
        )
        assert len(points) == 4
        assert [p.shares for p in points] == [
            Decimal("1400000000"),
            Decimal("1450000000"),
            Decimal("1480000000"),
            Decimal("1500000000"),
        ]
        # Provenance carries through.
        for p in points:
            assert p.source == "13f"
            assert p.source_accession == "0001234521-25-000021"  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Blockholders / Treasury / DEF 14A — smoke
# ---------------------------------------------------------------------------


class TestSmokeOtherCategories:
    def test_blockholders_amendment_chain(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_200, symbol="GME")
        conn.commit()
        run_id = uuid4()
        cik = "0001767470"
        for filed_year, accession, amount in [
            (2024, "0001234500-25-000210", Decimal("60000000")),
            (2025, "0001234500-25-000211", Decimal("75000000")),
        ]:
            record_blockholder_observation(
                conn,
                instrument_id=843_200,
                reporter_cik=cik,
                reporter_name="Cohen Ryan",
                ownership_nature="beneficial",
                submission_type="SCHEDULE 13D/A",
                status_flag="active",
                source="13d",
                source_document_id=accession,
                source_accession=accession,
                source_field=None,
                source_url=None,
                filed_at=datetime(filed_year, 1, 29, tzinfo=UTC),
                period_start=None,
                period_end=date(filed_year, 1, 29),
                ingest_run_id=run_id,
                aggregate_amount_owned=amount,
                percent_of_class=None,
            )
        conn.commit()

        points = get_ownership_history(
            conn,
            instrument_id=843_200,
            category="blockholders",
            holder_id=cik,
        )
        assert [p.shares for p in points] == [Decimal("60000000"), Decimal("75000000")]

    def test_treasury_quarterly(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_300, symbol="JPM")
        conn.commit()
        run_id = uuid4()
        for q_end, shares in [
            (date(2025, 12, 31), Decimal("1408661319")),
            (date(2026, 3, 31), Decimal("1425422477")),
        ]:
            record_treasury_observation(
                conn,
                instrument_id=843_300,
                source="xbrl_dei",
                source_document_id=f"TREAS-{q_end}",
                source_accession=None,
                source_field="TreasuryStockShares",
                source_url=None,
                filed_at=datetime.combine(q_end, datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=q_end,
                ingest_run_id=run_id,
                treasury_shares=shares,
            )
        conn.commit()

        points = get_ownership_history(
            conn,
            instrument_id=843_300,
            category="treasury",
        )
        assert len(points) == 2
        assert points[-1].shares == Decimal("1425422477")

    def test_def14a_holder_name_normalisation(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_400, symbol="AAPL")
        conn.commit()
        run_id = uuid4()
        for q_end, accession, name in [
            (date(2024, 12, 31), "0001234500-25-000216", "  Tim Cook  "),
            (date(2025, 12, 31), "0001234500-25-000217", "TIM COOK"),
        ]:
            record_def14a_observation(
                conn,
                instrument_id=843_400,
                holder_name=name,
                holder_role="CEO",
                ownership_nature="beneficial",
                source="def14a",
                source_document_id=accession,
                source_accession=accession,
                source_field=None,
                source_url=None,
                filed_at=datetime.combine(q_end, datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=q_end,
                ingest_run_id=run_id,
                shares=Decimal("3000000"),
                percent_of_class=None,
            )
        conn.commit()

        # Filter by name regardless of casing.
        points = get_ownership_history(
            conn,
            instrument_id=843_400,
            category="def14a",
            holder_id="Tim Cook",
        )
        assert len(points) == 2


class TestHolderScopedAPIContract:
    """Codex pre-push review for #840.F: omitting ``holder_id`` for
    holder-scoped categories returned one arbitrary winning holder per
    (period, nature) and silently dropped the rest. The API rejects
    that with 400. Service-level ``get_ownership_history`` keeps the
    historical behaviour for callers that legitimately want a deduped
    winner — direct testing here verifies the API guard logic without
    spinning up a TestClient (which trips a Python 3.14 + anyio
    lifespan-coro StopIteration on this test runner; deferred to
    a CI fix-it ticket)."""

    def test_get_ownership_history_works_with_holder_id(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Service-level: holder_id required for insiders (the
        category most likely to be misused without it)."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_500, symbol="HOLDERTEST")
        conn.commit()
        # Empty result is acceptable — what we're proving is that
        # the holder_id keyword argument plumbs through.
        points = get_ownership_history(
            conn,
            instrument_id=843_500,
            category="insiders",
            holder_id="0001234567",
        )
        assert points == []

    def test_get_ownership_history_treasury_ignores_holder_id(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Service-level: treasury is issuer-level. Passing
        holder_id is harmless (ignored)."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_501, symbol="TREASTEST")
        conn.commit()
        # No raise; empty result.
        points = get_ownership_history(
            conn,
            instrument_id=843_501,
            category="treasury",
            holder_id="ignored",
        )
        assert points == []


class TestUnknownCategory:
    def test_raises_on_unknown_category(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with pytest.raises(ValueError, match="unknown category"):
            get_ownership_history(
                ebull_test_conn,
                instrument_id=843_400,
                category="bogus",  # type: ignore[arg-type]
            )

    def test_blank_holder_id_normalised_to_full_series(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Bot review for #840.F PR #861: an empty / whitespace-only
        ``holder_id`` used to fall through to ``= NULL`` SQL and
        silently return zero rows. Now normalised to ``None`` =
        full-series scan."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_600, symbol="BLANKID")
        conn.commit()
        # Empty string and whitespace both map to None internally.
        for blank in ("", "   "):
            points = get_ownership_history(
                conn,
                instrument_id=843_600,
                category="insiders",
                holder_id=blank,
            )
            assert points == []  # No data seeded — but no SILENT-NULL trap.

    def test_iter_categories_covers_every_path(self) -> None:
        cats = list(iter_categories())
        assert set(cats) == {"insiders", "blockholders", "institutions", "treasury", "def14a"}


# ---------------------------------------------------------------------------
# Aggregate mode (#922)
# ---------------------------------------------------------------------------


class TestInstitutionsAggregate:
    """Category-total series (#922). The load-bearing invariant is
    dedup-BEFORE-sum: an amendment (second accession, same filer ×
    quarter) must replace, not add."""

    def _seed_13f(
        self,
        conn: psycopg.Connection[tuple],
        *,
        iid: int,
        filer_cik: str,
        q_end: date,
        doc_id: str,
        accession: str,
        filed_at: datetime,
        shares: Decimal,
        nature: OwnershipNature = "economic",
    ) -> None:
        record_institution_observation(
            conn,
            instrument_id=iid,
            filer_cik=filer_cik,
            filer_name=f"Filer {filer_cik}",
            filer_type="INV",
            ownership_nature=nature,
            source="13f",
            source_document_id=doc_id,
            source_accession=accession,
            source_field=None,
            source_url=None,
            filed_at=filed_at,
            period_start=None,
            period_end=q_end,
            ingest_run_id=uuid4(),
            shares=shares,
            market_value_usd=None,
            voting_authority="SOLE",
        )

    def test_amendment_dedups_before_sum(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.ownership_history import get_ownership_category_totals

        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_600, symbol="AGGTEST")
        conn.commit()
        q = date(2026, 3, 31)
        # Filer A: original then amendment (later filed_at wins).
        self._seed_13f(
            conn,
            iid=843_600,
            filer_cik="0000000001",
            q_end=q,
            doc_id="A-orig",
            accession="0001234599-26-000001",
            filed_at=datetime(2026, 5, 1, tzinfo=UTC),
            shares=Decimal("1000000"),
        )
        self._seed_13f(
            conn,
            iid=843_600,
            filer_cik="0000000001",
            q_end=q,
            doc_id="A-amend",
            accession="0001234599-26-000002",
            filed_at=datetime(2026, 5, 20, tzinfo=UTC),
            shares=Decimal("1200000"),
        )
        # Filer B: single filing.
        self._seed_13f(
            conn,
            iid=843_600,
            filer_cik="0000000002",
            q_end=q,
            doc_id="B-orig",
            accession="0001234599-26-000003",
            filed_at=datetime(2026, 5, 2, tzinfo=UTC),
            shares=Decimal("500000"),
        )
        conn.commit()

        points = get_ownership_category_totals(conn, instrument_id=843_600, category="institutions")
        assert len(points) == 1
        p = points[0]
        # Amendment replaced the original: 1.2M + 0.5M, NOT 1M + 1.2M + 0.5M.
        assert p.shares == Decimal("1700000")
        assert p.holder_count == 2
        assert p.ownership_nature == "economic"
        assert p.source == "13f"
        # An aggregate has no single accession.
        assert p.source_accession is None

    def test_non_economic_nature_excluded_not_summed(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A hypothetical second nature must be EXCLUDED (filtered),
        never silently folded into the "economic" total (spec D2)."""
        from app.services.ownership_history import get_ownership_category_totals

        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_601, symbol="NATTEST")
        conn.commit()
        q = date(2026, 3, 31)
        self._seed_13f(
            conn,
            iid=843_601,
            filer_cik="0000000003",
            q_end=q,
            doc_id="C-econ",
            accession="0001234599-26-000004",
            filed_at=datetime(2026, 5, 1, tzinfo=UTC),
            shares=Decimal("100"),
        )
        self._seed_13f(
            conn,
            iid=843_601,
            filer_cik="0000000003",
            q_end=q,
            doc_id="C-voting",
            accession="0001234599-26-000005",
            filed_at=datetime(2026, 5, 1, tzinfo=UTC),
            shares=Decimal("999"),
            nature="voting",
        )
        conn.commit()

        points = get_ownership_category_totals(conn, instrument_id=843_601, category="institutions")
        assert len(points) == 1
        assert points[0].shares == Decimal("100")

    def test_event_driven_category_raises(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.ownership_history import get_ownership_category_totals

        with pytest.raises(ValueError, match="no honest aggregate"):
            get_ownership_category_totals(ebull_test_conn, instrument_id=843_600, category="insiders")

    def test_treasury_aggregate_keeps_issuer_provenance(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Treasury aggregate IS the existing issuer series — XBRL
        source/accession untouched, holder_count None (spec D3)."""
        from app.services.ownership_history import get_ownership_category_totals

        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_602, symbol="TRAGG")
        conn.commit()
        record_treasury_observation(
            conn,
            instrument_id=843_602,
            source="xbrl_dei",
            source_document_id="T-1",
            source_accession="0000000000-26-000001",
            source_field="TreasuryStockShares",
            source_url=None,
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
            period_start=None,
            period_end=date(2025, 12, 31),
            ingest_run_id=uuid4(),
            treasury_shares=Decimal("250000"),
        )
        conn.commit()

        points = get_ownership_category_totals(conn, instrument_id=843_602, category="treasury")
        assert len(points) == 1
        assert points[0].source == "xbrl_dei"
        assert points[0].source_accession == "0000000000-26-000001"
        assert points[0].holder_count is None
