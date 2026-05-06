"""Tests for the ESOP observations write-through (#843).

Pins the contract of:
  * ``app.services.ownership_observations.record_esop_observation``
  * ``app.services.ownership_observations.refresh_esop_current``
  * ``app.services.def14a_ingest._record_esop_observations_for_filing``

Integration tests exercise the real ``ebull_test`` DB so the
partition + UPSERT semantics actually fire.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.providers.implementations.sec_def14a import Def14ABeneficialHolder
from app.services.def14a_ingest import _record_esop_observations_for_filing
from app.services.ownership_observations import (
    record_esop_observation,
    refresh_esop_current,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


class TestRecordEsopObservation:
    def test_round_trips_through_observations_and_current(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_001, symbol="ESOP1")
        record_esop_observation(
            conn,
            instrument_id=843_001,
            plan_name="Acme Inc. 401(k) Plan",
            plan_trustee_name="Vanguard Fiduciary Trust",
            plan_trustee_cik=None,
            source_document_id="acme-def14a-2026-001",
            source_accession="acme-def14a-2026-001",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 3, 1, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 3, 1),
            ingest_run_id=uuid4(),
            shares=Decimal("2000000"),
            percent_of_class=Decimal("6.50"),
        )
        refresh_esop_current(conn, instrument_id=843_001)
        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(
                """
                SELECT plan_name, plan_trustee_name, shares, percent_of_class
                FROM ownership_esop_current WHERE instrument_id = %s
                """,
                (843_001,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (
            "Acme Inc. 401(k) Plan",
            "Vanguard Fiduciary Trust",
            Decimal("2000000.0000"),
            Decimal("6.5000"),
        )

    def test_amendment_with_later_filed_at_wins(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Refresh picks the latest filing per plan_name. DEF 14A
        amendments (DEFA14A) carry the same period_end as the
        original DEF 14A but are filed later — `filed_at DESC` in
        the refresh ORDER BY ensures the amendment wins."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_002, symbol="ESOP2")
        common = {
            "instrument_id": 843_002,
            "plan_name": "Acme Inc. 401(k) Plan",
            "plan_trustee_name": "Original Trustee",
            "plan_trustee_cik": None,
            "source_field": None,
            "source_url": None,
            "period_start": None,
            "period_end": date(2026, 3, 1),
            "ingest_run_id": uuid4(),
            "shares": Decimal("1000000"),
            "percent_of_class": Decimal("3.00"),
        }
        # Original filing.
        record_esop_observation(
            conn,
            source_document_id="acme-def14a-2026-001",
            source_accession="acme-def14a-2026-001",
            filed_at=datetime(2026, 3, 1, tzinfo=UTC),
            **common,
        )
        # Amendment filed later, same period_end, different shares + trustee.
        amended = dict(common)
        amended["plan_trustee_name"] = "Amended Trustee"
        amended["shares"] = Decimal("2500000")
        record_esop_observation(
            conn,
            source_document_id="acme-defa14a-2026-001",
            source_accession="acme-defa14a-2026-001",
            filed_at=datetime(2026, 4, 15, tzinfo=UTC),
            **amended,
        )
        refresh_esop_current(conn, instrument_id=843_002)
        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(
                "SELECT plan_trustee_name, shares FROM ownership_esop_current WHERE instrument_id = %s",
                (843_002,),
            )
            row = cur.fetchone()
        assert row == ("Amended Trustee", Decimal("2500000.0000"))

    def test_zero_or_negative_shares_rejected(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_003, symbol="ESOP3")
        with pytest.raises(ValueError, match="positive"):
            record_esop_observation(
                conn,
                instrument_id=843_003,
                plan_name="Plan",
                plan_trustee_name=None,
                plan_trustee_cik=None,
                source_document_id="x",
                source_accession="x",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 3, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 3, 1),
                ingest_run_id=uuid4(),
                shares=Decimal("0"),
                percent_of_class=None,
            )

    def test_empty_plan_name_rejected(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_004, symbol="ESOP4")
        with pytest.raises(ValueError, match="plan_name"):
            record_esop_observation(
                conn,
                instrument_id=843_004,
                plan_name="   ",
                plan_trustee_name=None,
                plan_trustee_cik=None,
                source_document_id="x",
                source_accession="x",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 3, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 3, 1),
                ingest_run_id=uuid4(),
                shares=Decimal("1000"),
                percent_of_class=None,
            )


class TestRecordEsopObservationsForFiling:
    """The DEF 14A ingester's internal write-through helper. Filters
    parser-emitted holders to ESOP-tagged rows + extracts plan_name
    from trustee suffix."""

    def test_only_esop_role_rows_are_written(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_010, symbol="MIXED")
        holders = [
            Def14ABeneficialHolder(
                holder_name="Vanguard Group, Inc.",
                holder_role="principal",
                shares=Decimal("3000000"),
                percent_of_class=Decimal("9.5"),
            ),
            Def14ABeneficialHolder(
                holder_name="Acme Inc. 401 Plan, c/o Vanguard Fiduciary Trust as Trustee",
                holder_role="esop",
                shares=Decimal("2000000"),
                percent_of_class=Decimal("6.5"),
            ),
            Def14ABeneficialHolder(
                holder_name="Tim Cook",
                holder_role="officer",
                shares=Decimal("100000"),
                percent_of_class=Decimal("0.5"),
            ),
        ]
        written = _record_esop_observations_for_filing(
            conn,
            instrument_id=843_010,
            accession_number="mixed-def14a-2026",
            as_of_date=date(2026, 3, 1),
            holders=holders,
        )
        assert written == 1
        refresh_esop_current(conn, instrument_id=843_010)
        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(
                "SELECT plan_name, plan_trustee_name, shares FROM ownership_esop_current WHERE instrument_id = %s",
                (843_010,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        plan_name, trustee, shares = rows[0]
        assert plan_name == "Acme Inc. 401 Plan"
        assert trustee == "Vanguard Fiduciary Trust"
        assert shares == Decimal("2000000.0000")

    def test_no_esop_rows_returns_zero(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Common case for large-cap issuers: bene table has zero
        ESOP-tagged rows. The helper returns 0 so the caller can skip
        the refresh + advisory lock."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_011, symbol="NOESOP")
        holders = [
            Def14ABeneficialHolder(
                holder_name="Vanguard Group, Inc.",
                holder_role="principal",
                shares=Decimal("3000000"),
                percent_of_class=Decimal("9.5"),
            ),
            Def14ABeneficialHolder(
                holder_name="Tim Cook",
                holder_role="officer",
                shares=Decimal("100000"),
                percent_of_class=Decimal("0.5"),
            ),
        ]
        written = _record_esop_observations_for_filing(
            conn,
            instrument_id=843_011,
            accession_number="noesop-def14a-2026",
            as_of_date=date(2026, 3, 1),
            holders=holders,
        )
        assert written == 0


class TestEsopRowsExcludedFromDef14aWriteThrough:
    """Critical no-double-count regression (Codex pre-push review
    #843): an ESOP-tagged holder must land ONLY in the dedicated
    `ownership_esop_*` slice, NEVER in `ownership_def14a_current`.
    The general def14a write-through skips `holder_role='esop'` so
    the rollup's insider/blockholder def14a paths don't double-count
    the same shares the funds-slice ESOP overlay (#961) tags."""

    def test_esop_holder_does_not_land_in_def14a_current(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.def14a_ingest import _record_def14a_observations_for_filing
        from app.services.ownership_observations import refresh_def14a_current

        conn = ebull_test_conn
        _seed_instrument(conn, iid=843_020, symbol="DUAL")
        holders = [
            Def14ABeneficialHolder(
                holder_name="Vanguard Group, Inc.",
                holder_role="principal",
                shares=Decimal("3000000"),
                percent_of_class=Decimal("9.5"),
            ),
            Def14ABeneficialHolder(
                holder_name="Acme Inc. 401 Plan, c/o Vanguard Fiduciary Trust as Trustee",
                holder_role="esop",
                shares=Decimal("2000000"),
                percent_of_class=Decimal("6.5"),
            ),
        ]
        # Both write paths fire (mirrors the ingester).
        _record_def14a_observations_for_filing(
            conn,
            instrument_id=843_020,
            accession_number="dual-def14a-2026",
            as_of_date=date(2026, 3, 1),
            holders=holders,
        )
        refresh_def14a_current(conn, instrument_id=843_020)
        _record_esop_observations_for_filing(
            conn,
            instrument_id=843_020,
            accession_number="dual-def14a-2026",
            as_of_date=date(2026, 3, 1),
            holders=holders,
        )
        refresh_esop_current(conn, instrument_id=843_020)

        # def14a slice has Vanguard but NOT the plan.
        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(
                "SELECT holder_name FROM ownership_def14a_current WHERE instrument_id = %s",
                (843_020,),
            )
            def14a_names = {r[0] for r in cur.fetchall()}
        assert "Vanguard Group, Inc." in def14a_names
        assert not any("401 Plan" in n for n in def14a_names), (
            f"ESOP plan must NOT land in ownership_def14a_current; got {def14a_names!r}"
        )

        # ESOP slice has the plan only.
        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(
                "SELECT plan_name FROM ownership_esop_current WHERE instrument_id = %s",
                (843_020,),
            )
            esop_plans = {r[0] for r in cur.fetchall()}
        assert esop_plans == {"Acme Inc. 401 Plan"}
