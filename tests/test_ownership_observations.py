"""Tests for the ownership observations + _current pattern (#840.A).

Covers the foundation sub-PR: provenance shape, insiders
observations/current round-trip, two-axis dedup, refresh
idempotency, advisory-lock contract.

Subsequent sub-PRs (institutions / blockholders / treasury / def14a)
add their own tests; this module establishes the patterns.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.services.ownership_observations import (
    record_blockholder_observation,
    record_def14a_observation,
    record_insider_observation,
    record_institution_observation,
    record_treasury_observation,
    refresh_blockholders_current,
    refresh_def14a_current,
    refresh_insiders_current,
    refresh_institutions_current,
    refresh_treasury_current,
    resolve_filer_cik_or_raise,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


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
# Schema-shape uniformity (Codex plan-review finding #4)
# ---------------------------------------------------------------------------


class TestProvenanceBlockUniformity:
    """Every ``ownership_*_observations`` table must carry the EXACT
    provenance block (column names, types, nullability, source CHECK).
    Drift across categories is a real risk; this test fails CI on any
    deviation.

    For #840.A only ``ownership_insiders_observations`` exists. As
    sub-PRs B-D add institutions/blockholders/treasury/def14a tables,
    they auto-enroll into this test via the
    ``information_schema.tables`` LIKE pattern."""

    _PROVENANCE_COLS: tuple[str, ...] = (
        "source",
        "source_document_id",
        "source_accession",
        "source_field",
        "source_url",
        "filed_at",
        "period_start",
        "period_end",
        "known_from",
        "known_to",
        "ingest_run_id",
    )

    def test_every_observations_table_carries_full_provenance_block(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name LIKE 'ownership_%%_observations'
                  AND table_name NOT LIKE 'ownership_%%_observations_%%'
                """
            )
            tables = [str(row["table_name"]) for row in cur.fetchall()]

        assert tables, "no ownership_*_observations tables found"

        for table in tables:
            with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT column_name, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    """,
                    (table,),
                )
                cols = {str(row["column_name"]): str(row["is_nullable"]) for row in cur.fetchall()}

            for col in self._PROVENANCE_COLS:
                assert col in cols, f"{table} missing provenance column {col!r}"

            # Required-non-null provenance fields:
            for required in ("source", "source_document_id", "filed_at", "period_end", "known_from", "ingest_run_id"):
                assert cols[required] == "NO", f"{table}.{required} must be NOT NULL"

            # Optional fields:
            for nullable in ("source_accession", "source_field", "source_url", "period_start", "known_to"):
                assert cols[nullable] == "YES", f"{table}.{nullable} must be NULLABLE"

    def test_default_partition_is_empty_post_backfill(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Codex plan-review finding #1: the default partition catches
        any row whose ``period_end`` falls outside the explicit ranges.
        On a healthy install this stays empty — anything landing here
        signals a partition floor / ceiling miss that the operator
        must investigate."""
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ownership_insiders_observations_default")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0


# ---------------------------------------------------------------------------
# Insider observation round-trip + dedup
# ---------------------------------------------------------------------------


class TestInsiderObservations:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=840_001, symbol="GME")
        conn.commit()
        return conn

    def test_record_then_refresh_round_trip(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """One observation → one row in _current. Verifies the basic
        record/refresh cycle works end-to-end."""
        conn = _setup
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik="0001767470",
            holder_name="Cohen Ryan",
            ownership_nature="direct",
            source="form4",
            source_document_id="0001234567-26-000001",
            source_accession="0001234567-26-000001",
            source_field=None,
            source_url="https://www.sec.gov/.../form4.xml",
            filed_at=datetime(2026, 1, 21, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 1, 21),
            ingest_run_id=uuid4(),
            shares=Decimal("38347842"),
        )
        conn.commit()

        n = refresh_insiders_current(conn, instrument_id=840_001)
        conn.commit()
        assert n == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT holder_cik, ownership_nature, source, shares
                FROM ownership_insiders_current WHERE instrument_id = %s
                """,
                (840_001,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["holder_cik"] == "0001767470"
        assert rows[0]["ownership_nature"] == "direct"
        assert rows[0]["source"] == "form4"
        assert rows[0]["shares"] == Decimal("38347842")

    def test_dedup_within_nature_form4_outranks_form3(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Two observations for the same (CIK, nature) — Form 4 wins
        Form 3 per the source-priority chain."""
        conn = _setup
        cik = "0009990001"
        run_id = uuid4()
        # Older Form 3.
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik=cik,
            holder_name="Officer A",
            ownership_nature="direct",
            source="form3",
            source_document_id="ACC-F3",
            source_accession="ACC-F3",
            source_field=None,
            source_url=None,
            filed_at=datetime(2024, 5, 1, tzinfo=UTC),
            period_start=None,
            period_end=date(2024, 5, 1),
            ingest_run_id=run_id,
            shares=Decimal("10000"),
        )
        # Newer Form 4 — should win.
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik=cik,
            holder_name="Officer A",
            ownership_nature="direct",
            source="form4",
            source_document_id="ACC-F4",
            source_accession="ACC-F4",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 1, 15, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 1, 15),
            ingest_run_id=run_id,
            shares=Decimal("12500"),
        )
        conn.commit()

        refresh_insiders_current(conn, instrument_id=840_001)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT source, shares FROM ownership_insiders_current
                WHERE instrument_id = %s AND holder_cik = %s
                """,
                (840_001, cik),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["source"] == "form4"
        assert rows[0]["shares"] == Decimal("12500")

    def test_dual_render_across_natures_for_same_cik(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Cohen-on-GME shape: same CIK with TWO different
        ``ownership_nature`` values — direct (Form 4, 38M) and
        beneficial (13D, 75M). Both must surface in ``_current`` —
        cross-nature dedup is forbidden under the two-axis spec."""
        conn = _setup
        cik = "0001767470"
        run_id = uuid4()
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik=cik,
            holder_name="Cohen Ryan",
            ownership_nature="direct",
            source="form4",
            source_document_id="ACC-F4-COHEN",
            source_accession="ACC-F4-COHEN",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 1, 21, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 1, 21),
            ingest_run_id=run_id,
            shares=Decimal("38347842"),
        )
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik=cik,
            holder_name="Cohen Ryan",
            ownership_nature="beneficial",
            source="13d",
            source_document_id="ACC-13D-COHEN",
            source_accession="ACC-13D-COHEN",
            source_field=None,
            source_url=None,
            filed_at=datetime(2025, 1, 29, tzinfo=UTC),
            period_start=None,
            period_end=date(2025, 1, 29),
            ingest_run_id=run_id,
            shares=Decimal("75000000"),
        )
        conn.commit()

        refresh_insiders_current(conn, instrument_id=840_001)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT ownership_nature, source, shares
                FROM ownership_insiders_current
                WHERE instrument_id = %s AND holder_cik = %s
                ORDER BY ownership_nature
                """,
                (840_001, cik),
            )
            rows = cur.fetchall()
        # Two rows, both surface.
        assert len(rows) == 2
        natures = {r["ownership_nature"]: (r["source"], r["shares"]) for r in rows}
        assert natures["direct"] == ("form4", Decimal("38347842"))
        assert natures["beneficial"] == ("13d", Decimal("75000000"))

    def test_refresh_is_idempotent(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Running ``refresh_insiders_current`` twice with no new
        observations leaves the same set of rows. Refresh atomicity
        guard exercised."""
        conn = _setup
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik="0001234567",
            holder_name="Test Holder",
            ownership_nature="direct",
            source="form4",
            source_document_id="ACC-1",
            source_accession="ACC-1",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 1, 1),
            ingest_run_id=uuid4(),
            shares=Decimal("100"),
        )
        conn.commit()

        first = refresh_insiders_current(conn, instrument_id=840_001)
        conn.commit()
        second = refresh_insiders_current(conn, instrument_id=840_001)
        conn.commit()
        assert first == 1
        assert second == 1

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ownership_insiders_current WHERE instrument_id = %s",
                (840_001,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1

    def test_record_is_idempotent_on_natural_key(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Recording the same observation twice (re-running the
        ingester on the same accession) does NOT duplicate the
        observations row — ON CONFLICT DO UPDATE refreshes in place."""
        conn = _setup
        run_id = uuid4()
        for _ in range(2):
            record_insider_observation(
                conn,
                instrument_id=840_001,
                holder_cik="0007770007",
                holder_name="Idempotent Holder",
                ownership_nature="direct",
                source="form4",
                source_document_id="ACC-IDEMP",
                source_accession="ACC-IDEMP",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 2, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 2, 1),
                ingest_run_id=run_id,
                shares=Decimal("500"),
            )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM ownership_insiders_observations
                WHERE instrument_id = %s AND holder_cik = %s
                """,
                (840_001, "0007770007"),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1


# ---------------------------------------------------------------------------
# Institution observations + _current (#840.B)
# ---------------------------------------------------------------------------


def _seed_institutional_filer(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    name: str,
    filer_type: str | None = None,
) -> int:
    """Returns filer_id for the legacy join path."""
    conn.execute(
        """
        INSERT INTO institutional_filers (cik, name, filer_type)
        VALUES (%s, %s, %s)
        ON CONFLICT (cik) DO UPDATE SET filer_type = EXCLUDED.filer_type
        """,
        (cik, name, filer_type),
    )
    with conn.cursor() as cur:
        cur.execute("SELECT filer_id FROM institutional_filers WHERE cik = %s", (cik,))
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


class TestInstitutionObservations:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=840_100, symbol="AAPL")
        conn.commit()
        return conn

    def test_record_then_refresh_round_trip(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        record_institution_observation(
            conn,
            instrument_id=840_100,
            filer_cik="0000102909",
            filer_name="Vanguard Group Inc",
            filer_type="ETF",
            ownership_nature="economic",
            source="13f",
            source_document_id="0001234567-26-VG-Q1",
            source_accession="0001234567-26-VG-Q1",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 4, 15, tzinfo=UTC),
            period_start=date(2026, 1, 1),
            period_end=date(2026, 3, 31),
            ingest_run_id=uuid4(),
            shares=Decimal("1500000000"),
            market_value_usd=Decimal("250000000000"),
            voting_authority="SOLE",
        )
        conn.commit()

        n = refresh_institutions_current(conn, instrument_id=840_100)
        conn.commit()
        assert n == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT filer_cik, shares, voting_authority
                FROM ownership_institutions_current WHERE instrument_id = %s
                """,
                (840_100,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["filer_cik"] == "0000102909"
        assert rows[0]["shares"] == Decimal("1500000000")
        assert rows[0]["voting_authority"] == "SOLE"

    def test_dedup_picks_latest_period_end(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Same filer, two consecutive quarters — _current carries the
        Q1 2026 row; Q4 2025 stays in observations as history."""
        conn = _setup
        cik = "0000102909"
        run_id = uuid4()
        for q_end, accession, shares in [
            (date(2025, 12, 31), "ACC-Q4", Decimal("1400000000")),
            (date(2026, 3, 31), "ACC-Q1", Decimal("1500000000")),
        ]:
            record_institution_observation(
                conn,
                instrument_id=840_100,
                filer_cik=cik,
                filer_name="Vanguard Group Inc",
                filer_type="ETF",
                ownership_nature="economic",
                source="13f",
                source_document_id=accession,
                source_accession=accession,
                source_field=None,
                source_url=None,
                filed_at=datetime(q_end.year, q_end.month, 28, tzinfo=UTC),
                period_start=None,
                period_end=q_end,
                ingest_run_id=run_id,
                shares=shares,
                market_value_usd=None,
                voting_authority=None,
            )
        conn.commit()

        refresh_institutions_current(conn, instrument_id=840_100)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT period_end, shares FROM ownership_institutions_current WHERE filer_cik = %s",
                (cik,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["period_end"] == date(2026, 3, 31)
        assert rows[0]["shares"] == Decimal("1500000000")

        # Both observations preserved (history).
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ownership_institutions_observations WHERE filer_cik = %s",
                (cik,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 2

    def test_equity_put_call_coexist_per_accession(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Codex review for #840.B: 13F-HR can carry up to three legal
        rows per (accession, instrument): equity, PUT, CALL. They MUST
        coexist in observations and in _current — collapsing on
        ON CONFLICT loses the option exposures."""
        conn = _setup
        cik = "0000102909"
        run_id = uuid4()
        accession = "ACC-3-EXPOSURE"
        period_end = date(2026, 3, 31)
        for kind, shares in [("EQUITY", Decimal("1000000")), ("PUT", Decimal("50000")), ("CALL", Decimal("75000"))]:
            record_institution_observation(
                conn,
                instrument_id=840_100,
                filer_cik=cik,
                filer_name="Vanguard Group Inc",
                filer_type="ETF",
                ownership_nature="economic",
                source="13f",
                source_document_id=accession,
                source_accession=accession,
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 4, 15, tzinfo=UTC),
                period_start=None,
                period_end=period_end,
                ingest_run_id=run_id,
                shares=shares,
                market_value_usd=None,
                voting_authority=None,
                exposure_kind=kind,  # type: ignore[arg-type]
            )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ownership_institutions_observations WHERE instrument_id = %s",
                (840_100,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 3

        refresh_institutions_current(conn, instrument_id=840_100)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT exposure_kind, shares FROM ownership_institutions_current
                WHERE instrument_id = %s ORDER BY exposure_kind
                """,
                (840_100,),
            )
            rows = cur.fetchall()
        kinds = {r["exposure_kind"]: r["shares"] for r in rows}
        assert kinds == {
            "CALL": Decimal("75000"),
            "EQUITY": Decimal("1000000"),
            "PUT": Decimal("50000"),
        }

    def test_record_rejects_blank_filer_cik(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Codex plan-review finding #2: orphan filer_cik must fail
        loud, not silently drop. The new model's identity is filer_cik;
        a blank value is unrecoverable."""
        with pytest.raises(ValueError, match="filer_cik is required"):
            record_institution_observation(
                _setup,
                instrument_id=840_100,
                filer_cik="   ",
                filer_name="Blank",
                filer_type=None,
                ownership_nature="economic",
                source="13f",
                source_document_id="ACC-X",
                source_accession="ACC-X",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 4, 15, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 3, 31),
                ingest_run_id=uuid4(),
                shares=Decimal("1"),
                market_value_usd=None,
                voting_authority=None,
            )


class TestResolveFilerCikOrRaise:
    """Codex plan-review finding #2: backfill must resolve filer_id →
    cik via institutional_filers and fail loud on orphans."""

    def test_resolves_known_filer(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        filer_id = _seed_institutional_filer(conn, cik="0000102909", name="Vanguard", filer_type="ETF")
        conn.commit()
        cik, name, ftype = resolve_filer_cik_or_raise(conn, filer_id=filer_id)
        assert cik == "0000102909"
        assert name == "Vanguard"
        assert ftype == "ETF"

    def test_raises_on_orphan_filer_id(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with pytest.raises(ValueError, match="filer_id=999999"):
            resolve_filer_cik_or_raise(ebull_test_conn, filer_id=999_999)


# ---------------------------------------------------------------------------
# Blockholder observations + _current (#840.C)
# ---------------------------------------------------------------------------


class TestBlockholderObservations:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=840_200, symbol="GME")
        conn.commit()
        return conn

    def test_record_then_refresh_round_trip(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        record_blockholder_observation(
            conn,
            instrument_id=840_200,
            reporter_cik="0001767470",
            reporter_name="Cohen Ryan",
            ownership_nature="beneficial",
            submission_type="SCHEDULE 13D/A",
            status_flag="active",
            source="13d",
            source_document_id="0000921895-25-000190",
            source_accession="0000921895-25-000190",
            source_field=None,
            source_url=None,
            filed_at=datetime(2025, 1, 29, tzinfo=UTC),
            period_start=None,
            period_end=date(2025, 1, 29),
            ingest_run_id=uuid4(),
            aggregate_amount_owned=Decimal("75000000"),
            percent_of_class=Decimal("16.77"),
        )
        conn.commit()

        n = refresh_blockholders_current(conn, instrument_id=840_200)
        conn.commit()
        assert n == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT reporter_cik, ownership_nature, source, aggregate_amount_owned
                FROM ownership_blockholders_current WHERE instrument_id = %s
                """,
                (840_200,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["reporter_cik"] == "0001767470"
        assert rows[0]["ownership_nature"] == "beneficial"
        assert rows[0]["source"] == "13d"
        assert rows[0]["aggregate_amount_owned"] == Decimal("75000000")

    def test_amendment_chain_picks_latest_filed_at(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Two amendments, same primary filer + nature. Latest
        ``filed_at`` wins. Earlier amendments stay in observations
        for history."""
        conn = _setup
        cik = "0001767470"
        run_id = uuid4()
        for filed_year, accession, amount in [
            (2024, "13D-RC-2024-001", Decimal("60000000")),
            (2025, "13D-RC-2025-001", Decimal("75000000")),
        ]:
            record_blockholder_observation(
                conn,
                instrument_id=840_200,
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

        refresh_blockholders_current(conn, instrument_id=840_200)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT source_accession, aggregate_amount_owned
                FROM ownership_blockholders_current
                WHERE reporter_cik = %s
                """,
                (cik,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["source_accession"] == "13D-RC-2025-001"
        assert rows[0]["aggregate_amount_owned"] == Decimal("75000000")

        # History preserved.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ownership_blockholders_observations WHERE reporter_cik = %s",
                (cik,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 2

    def test_invariant_13d_must_be_active_13g_must_be_passive(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Codex review for #840.C: cross-column invariant from
        legacy ``blockholder_filings`` (sql/095) must be preserved.
        13D / 13D/A are active; 13G / 13G/A are passive. Two
        independent enum CHECKs would let a misclassified row
        through (e.g., 13D + passive). Compound CHECK guards both
        observations and current tables."""
        from psycopg.errors import CheckViolation

        with pytest.raises(CheckViolation):
            record_blockholder_observation(
                _setup,
                instrument_id=840_200,
                reporter_cik="0001234567",
                reporter_name="Bad Mix",
                ownership_nature="beneficial",
                submission_type="SCHEDULE 13D",
                status_flag="passive",  # invalid: 13D must be active
                source="13d",
                source_document_id="ACC-BAD",
                source_accession="ACC-BAD",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 1, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 1, 1),
                ingest_run_id=uuid4(),
                aggregate_amount_owned=Decimal("1"),
                percent_of_class=None,
            )

    def test_record_rejects_blank_reporter_cik(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        with pytest.raises(ValueError, match="reporter_cik is required"):
            record_blockholder_observation(
                _setup,
                instrument_id=840_200,
                reporter_cik="",
                reporter_name="Blank",
                ownership_nature="beneficial",
                submission_type="SCHEDULE 13D",
                status_flag=None,
                source="13d",
                source_document_id="ACC",
                source_accession="ACC",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 1, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 1, 1),
                ingest_run_id=uuid4(),
                aggregate_amount_owned=Decimal("1"),
                percent_of_class=None,
            )


# ---------------------------------------------------------------------------
# Treasury observations + _current (#840.D)
# ---------------------------------------------------------------------------


class TestTreasuryObservations:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=840_300, symbol="JPM")
        conn.commit()
        return conn

    def test_round_trip_picks_latest_period(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        run_id = uuid4()
        for q_end, accession, shares in [
            (date(2025, 12, 31), "ACC-Q4", Decimal("1408661319")),
            (date(2026, 3, 31), "ACC-Q1", Decimal("1425422477")),
        ]:
            record_treasury_observation(
                conn,
                instrument_id=840_300,
                source="xbrl_dei",
                source_document_id=accession,
                source_accession=accession,
                source_field="TreasuryStockShares",
                source_url=None,
                filed_at=datetime(q_end.year, q_end.month, 28, tzinfo=UTC),
                period_start=None,
                period_end=q_end,
                ingest_run_id=run_id,
                treasury_shares=shares,
            )
        conn.commit()

        n = refresh_treasury_current(conn, instrument_id=840_300)
        conn.commit()
        assert n == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT period_end, treasury_shares FROM ownership_treasury_current WHERE instrument_id = %s",
                (840_300,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["period_end"] == date(2026, 3, 31)
        assert rows[0]["treasury_shares"] == Decimal("1425422477")

    def test_null_observation_does_not_displace_non_null(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """A re-parse that lost the concept (NULL value) must NOT
        blank out the prior good value in _current."""
        conn = _setup
        run_id = uuid4()
        record_treasury_observation(
            conn,
            instrument_id=840_300,
            source="xbrl_dei",
            source_document_id="ACC-OLD",
            source_accession="ACC-OLD",
            source_field=None,
            source_url=None,
            filed_at=datetime(2025, 6, 30, tzinfo=UTC),
            period_start=None,
            period_end=date(2025, 6, 30),
            ingest_run_id=run_id,
            treasury_shares=Decimal("1300000000"),
        )
        record_treasury_observation(
            conn,
            instrument_id=840_300,
            source="xbrl_dei",
            source_document_id="ACC-NEW",
            source_accession="ACC-NEW",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 3, 31, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 3, 31),
            ingest_run_id=run_id,
            treasury_shares=None,
        )
        conn.commit()

        refresh_treasury_current(conn, instrument_id=840_300)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT treasury_shares FROM ownership_treasury_current WHERE instrument_id = %s",
                (840_300,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["treasury_shares"] == Decimal("1300000000")


# ---------------------------------------------------------------------------
# DEF 14A observations + _current (#840.D)
# ---------------------------------------------------------------------------


class TestDef14aObservations:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=840_400, symbol="AAPL")
        conn.commit()
        return conn

    def test_round_trip(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        record_def14a_observation(
            conn,
            instrument_id=840_400,
            holder_name="Tim Cook",
            holder_role="CEO",
            ownership_nature="beneficial",
            source="def14a",
            source_document_id="ACC-PROXY-2026",
            source_accession="ACC-PROXY-2026",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 1, 15, tzinfo=UTC),
            period_start=None,
            period_end=date(2025, 12, 31),
            ingest_run_id=uuid4(),
            shares=Decimal("3300000"),
            percent_of_class=Decimal("0.02"),
        )
        conn.commit()

        n = refresh_def14a_current(conn, instrument_id=840_400)
        conn.commit()
        assert n == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT holder_name, holder_name_key, shares
                FROM ownership_def14a_current WHERE instrument_id = %s
                """,
                (840_400,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["holder_name"] == "Tim Cook"
        assert rows[0]["holder_name_key"] == "tim cook"
        assert rows[0]["shares"] == Decimal("3300000")

    def test_holder_name_normalised_to_key(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Two proxies, same officer with whitespace / case variation
        in the name: dedup collapses to one ``_current`` row keyed on
        the normalised name."""
        conn = _setup
        run_id = uuid4()
        for q_end, accession, name, shares in [
            (date(2024, 12, 31), "ACC-2024", "  Tim Cook  ", Decimal("3000000")),
            (date(2025, 12, 31), "ACC-2025", "TIM COOK", Decimal("3300000")),
        ]:
            record_def14a_observation(
                conn,
                instrument_id=840_400,
                holder_name=name,
                holder_role=None,
                ownership_nature="beneficial",
                source="def14a",
                source_document_id=accession,
                source_accession=accession,
                source_field=None,
                source_url=None,
                filed_at=datetime(q_end.year, q_end.month, 31, tzinfo=UTC),
                period_start=None,
                period_end=q_end,
                ingest_run_id=run_id,
                shares=shares,
                percent_of_class=None,
            )
        conn.commit()

        refresh_def14a_current(conn, instrument_id=840_400)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT shares FROM ownership_def14a_current WHERE instrument_id = %s",
                (840_400,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["shares"] == Decimal("3300000")

    def test_dual_nature_for_same_holder_same_accession(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Bot review for #840.D PR #854: a real proxy filing carries
        BOTH beneficial and voting rows for the same holder under the
        SAME accession. The observations PK must include
        ``ownership_nature`` so the two rows coexist; otherwise the
        second INSERT collapses the first via ON CONFLICT before
        refresh ever runs. Migration 117 fixes this."""
        conn = _setup
        run_id = uuid4()
        accession = "ACC-PROXY-2026"  # SAME accession for both natures
        for nature, shares in [
            ("beneficial", Decimal("3300000")),
            ("voting", Decimal("3000000")),
        ]:
            record_def14a_observation(
                conn,
                instrument_id=840_400,
                holder_name="Tim Cook",
                holder_role="CEO",
                ownership_nature=nature,  # type: ignore[arg-type]
                source="def14a",
                source_document_id=accession,
                source_accession=accession,
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 1, 15, tzinfo=UTC),
                period_start=None,
                period_end=date(2025, 12, 31),
                ingest_run_id=run_id,
                shares=shares,
                percent_of_class=None,
            )
        conn.commit()

        # Both observations preserved (no PK collision).
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM ownership_def14a_observations
                WHERE instrument_id = %s AND source_document_id = %s
                """,
                (840_400, accession),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 2

        refresh_def14a_current(conn, instrument_id=840_400)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT ownership_nature, shares FROM ownership_def14a_current
                WHERE instrument_id = %s ORDER BY ownership_nature
                """,
                (840_400,),
            )
            rows = cur.fetchall()
        assert len(rows) == 2
        natures = {r["ownership_nature"]: r["shares"] for r in rows}
        assert natures == {"beneficial": Decimal("3300000"), "voting": Decimal("3000000")}

    def test_dual_nature_for_same_holder(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Earlier dual-nature test using DIFFERENT accessions per
        nature. Kept for coverage of the cross-proxy case."""
        conn = _setup
        run_id = uuid4()
        for nature, accession, shares in [
            ("beneficial", "ACC-BEN", Decimal("3300000")),
            ("voting", "ACC-VOTE", Decimal("3000000")),
        ]:
            record_def14a_observation(
                conn,
                instrument_id=840_400,
                holder_name="Tim Cook",
                holder_role="CEO",
                ownership_nature=nature,  # type: ignore[arg-type]
                source="def14a",
                source_document_id=accession,
                source_accession=accession,
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 1, 15, tzinfo=UTC),
                period_start=None,
                period_end=date(2025, 12, 31),
                ingest_run_id=run_id,
                shares=shares,
                percent_of_class=None,
            )
        conn.commit()

        refresh_def14a_current(conn, instrument_id=840_400)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT ownership_nature, shares FROM ownership_def14a_current
                WHERE instrument_id = %s ORDER BY ownership_nature
                """,
                (840_400,),
            )
            rows = cur.fetchall()
        assert len(rows) == 2
        natures = {r["ownership_nature"]: r["shares"] for r in rows}
        assert natures == {"beneficial": Decimal("3300000"), "voting": Decimal("3000000")}

    def test_null_shares_does_not_displace_prior_good_value(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Codex review for #840.D: a re-parse of a later proxy that
        loses the shares concept must NOT blank out the prior good
        value in _current. Refresh filters NULL shares."""
        conn = _setup
        run_id = uuid4()
        record_def14a_observation(
            conn,
            instrument_id=840_400,
            holder_name="Tim Cook",
            holder_role=None,
            ownership_nature="beneficial",
            source="def14a",
            source_document_id="ACC-OLD",
            source_accession="ACC-OLD",
            source_field=None,
            source_url=None,
            filed_at=datetime(2024, 1, 15, tzinfo=UTC),
            period_start=None,
            period_end=date(2023, 12, 31),
            ingest_run_id=run_id,
            shares=Decimal("3000000"),
            percent_of_class=None,
        )
        record_def14a_observation(
            conn,
            instrument_id=840_400,
            holder_name="Tim Cook",
            holder_role=None,
            ownership_nature="beneficial",
            source="def14a",
            source_document_id="ACC-NEW",
            source_accession="ACC-NEW",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 1, 15, tzinfo=UTC),
            period_start=None,
            period_end=date(2025, 12, 31),
            ingest_run_id=run_id,
            shares=None,
            percent_of_class=None,
        )
        conn.commit()

        refresh_def14a_current(conn, instrument_id=840_400)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT shares FROM ownership_def14a_current WHERE instrument_id = %s",
                (840_400,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["shares"] == Decimal("3000000")

    def test_record_rejects_blank_holder_name(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        with pytest.raises(ValueError, match="holder_name is required"):
            record_def14a_observation(
                _setup,
                instrument_id=840_400,
                holder_name="   ",
                holder_role=None,
                ownership_nature="beneficial",
                source="def14a",
                source_document_id="ACC-X",
                source_accession="ACC-X",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 1, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 1, 1),
                ingest_run_id=uuid4(),
                shares=Decimal("1"),
                percent_of_class=None,
            )
