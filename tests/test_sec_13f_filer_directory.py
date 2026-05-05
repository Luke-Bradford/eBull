"""Tests for the SEC 13F-HR filer directory sync (#912 / #841 PR1).

Pins the contract of :mod:`app.services.sec_13f_filer_directory`:

  * Walks the last N closed quarters' ``form.idx``, harvests every
    distinct 13F-HR filer CIK, UPSERTs into ``institutional_filers``.
  * Idempotent — re-run on the same quarter set produces zero new
    inserts but refreshes ``name`` + ``last_filing_at``.
  * ``filer_type`` resolves via curated ETF list + N-CEN classifier
    (defaulting to ``'INV'``); never NULL on the rows this job
    writes.
  * Per-quarter fetch failures are isolated — a transient SEC
    outage on one quarter doesn't abort the whole sweep.
  * Empty-name rows are skipped + counted (loudly, via warning).

Unit tests use a fake fetcher so no real SEC traffic. Integration
tests against ``ebull_test`` exercise the UPSERT + classification
chain end-to-end.
"""

from __future__ import annotations

from datetime import date

import psycopg
import psycopg.rows
import pytest

from app.services.sec_13f_filer_directory import (
    _bulk_classify_filer_type,
    _last_completed_quarter,
    _last_n_quarters,
    sync_filer_directory,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_HEADER = "Description: x\n\nForm Type   Company Name   CIK   Date Filed   File Name\n" + "-" * 100 + "\n"


def _row(form_type: str, company: str, cik: int, filed: str, file: str) -> str:
    return f"{form_type}  {company}  {cik}  {filed}  {file}\n"


# ---------------------------------------------------------------------------
# Pure-helper unit tests
# ---------------------------------------------------------------------------


class TestLastCompletedQuarter:
    def test_january_picks_prior_year_q4(self) -> None:
        assert _last_completed_quarter(date(2026, 1, 5)) == (2025, 4)

    def test_march_still_in_q1_picks_prior_year_q4(self) -> None:
        assert _last_completed_quarter(date(2026, 3, 31)) == (2025, 4)

    def test_april_first_picks_q1(self) -> None:
        assert _last_completed_quarter(date(2026, 4, 1)) == (2026, 1)

    def test_july_picks_q2(self) -> None:
        assert _last_completed_quarter(date(2026, 7, 15)) == (2026, 2)


class TestLastNQuarters:
    def test_returns_n_quarters_newest_first(self) -> None:
        # 2026-05-05 → most recent closed quarter is 2026 Q1.
        assert _last_n_quarters(date(2026, 5, 5), 4) == [
            (2026, 1),
            (2025, 4),
            (2025, 3),
            (2025, 2),
        ]

    def test_zero_returns_empty(self) -> None:
        assert _last_n_quarters(date(2026, 5, 5), 0) == []

    def test_walks_across_year_boundary(self) -> None:
        assert _last_n_quarters(date(2026, 1, 15), 5) == [
            (2025, 4),
            (2025, 3),
            (2025, 2),
            (2025, 1),
            (2024, 4),
        ]


# ---------------------------------------------------------------------------
# Integration: bulk classifier + sync against ebull_test
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.integration


class TestBulkClassifyFilerType:
    def test_empty_list_short_circuits(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
        assert _bulk_classify_filer_type(ebull_test_conn, []) == {}

    def test_default_is_inv_for_unknown_ciks(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        result = _bulk_classify_filer_type(ebull_test_conn, ["0009999991", "0009999992"])
        assert result == {"0009999991": "INV", "0009999992": "INV"}

    def test_curated_etf_overrides_ncen_and_default(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Curated ETF row + an N-CEN row claiming INS — ETF must win.
        ebull_test_conn.execute(
            "INSERT INTO etf_filer_cik_seeds (cik, label, active) VALUES ('0009999990', 'Test ETF Issuer', TRUE)",
        )
        ebull_test_conn.execute(
            """
            INSERT INTO ncen_filer_classifications
                (cik, investment_company_type, derived_filer_type,
                 accession_number, filed_at)
            VALUES ('0009999990', 'N-3', 'INS', '0000000000-99-999999',
                    NOW())
            """,
        )
        ebull_test_conn.commit()

        result = _bulk_classify_filer_type(ebull_test_conn, ["0009999990"])
        assert result == {"0009999990": "ETF"}

    def test_ncen_overrides_default_when_no_curated_etf(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        ebull_test_conn.execute(
            """
            INSERT INTO ncen_filer_classifications
                (cik, investment_company_type, derived_filer_type,
                 accession_number, filed_at)
            VALUES ('0009999988', 'N-3', 'INS', '0000000000-99-999998',
                    NOW())
            """,
        )
        ebull_test_conn.commit()

        result = _bulk_classify_filer_type(ebull_test_conn, ["0009999988"])
        assert result == {"0009999988": "INS"}


class TestSyncFilerDirectory:
    def test_inserts_new_filers_and_classifies_via_curated_list(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Curate 0000000300 as ETF; leave 0000000100 unclassified.
        ebull_test_conn.execute(
            "INSERT INTO etf_filer_cik_seeds (cik, label, active) VALUES ('0000000300', 'Test ETF', TRUE)",
        )
        ebull_test_conn.commit()

        payload = (
            _HEADER
            + _row("13F-HR", "FILER ALPHA", 100, "2026-02-14", "edgar/data/100/a.txt")
            + _row("13F-HR/A", "FILER ALPHA", 100, "2026-03-15", "edgar/data/100/aa.txt")
            + _row("13F-HR", "FILER GAMMA ETF TR", 300, "2026-02-14", "edgar/data/300/g.txt")
            + _row("4", "Some Insider", 999, "2026-02-14", "edgar/data/999/4.txt")
        )

        def _fake(year: int, q: int) -> str:
            return payload

        result = sync_filer_directory(
            ebull_test_conn,
            quarters=1,
            today=date(2026, 5, 5),
            fetch=_fake,
        )

        assert result.quarters_attempted == 1
        assert result.quarters_failed == 0
        assert result.filers_seen == 2  # Form 4 row excluded
        assert result.filers_inserted == 2
        assert result.filers_refreshed == 0

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT cik, name, filer_type, last_filing_at "
                "FROM institutional_filers WHERE cik IN ('0000000100', '0000000300') "
                "ORDER BY cik"
            )
            rows = cur.fetchall()
        assert [r["cik"] for r in rows] == ["0000000100", "0000000300"]
        # 0000000100 default → INV. 0000000300 curated ETF → ETF.
        assert rows[0]["filer_type"] == "INV"
        assert rows[1]["filer_type"] == "ETF"
        # latest filing date wins for the renamed amendment.
        assert rows[0]["name"] == "FILER ALPHA"
        assert rows[0]["last_filing_at"].date() == date(2026, 3, 15)

    def test_idempotent_refresh_does_not_double_insert(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        payload = _HEADER + _row("13F-HR", "FILER ALPHA", 100, "2026-02-14", "edgar/data/100/a.txt")

        def _fake(year: int, q: int) -> str:
            return payload

        first = sync_filer_directory(ebull_test_conn, quarters=1, today=date(2026, 5, 5), fetch=_fake)
        second = sync_filer_directory(ebull_test_conn, quarters=1, today=date(2026, 5, 5), fetch=_fake)

        assert first.filers_inserted == 1
        assert first.filers_refreshed == 0
        assert second.filers_inserted == 0
        assert second.filers_refreshed == 1

    def test_filer_rename_propagates_on_second_run(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        first_payload = _HEADER + _row("13F-HR", "OLD NAME LLC", 100, "2026-01-15", "edgar/data/100/a.txt")
        second_payload = _HEADER + _row("13F-HR/A", "NEW NAME LLC", 100, "2026-04-15", "edgar/data/100/b.txt")

        sync_filer_directory(ebull_test_conn, quarters=1, today=date(2026, 5, 5), fetch=lambda *_: first_payload)
        sync_filer_directory(ebull_test_conn, quarters=1, today=date(2026, 5, 5), fetch=lambda *_: second_payload)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT name, last_filing_at FROM institutional_filers WHERE cik = '0000000100'")
            row = cur.fetchone()
        assert row is not None
        assert row["name"] == "NEW NAME LLC"
        # GREATEST() — second run's later date_filed must win.
        assert row["last_filing_at"].date() == date(2026, 4, 15)

    def test_per_quarter_fetch_failure_isolated(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        ok_payload = _HEADER + _row("13F-HR", "FILER A", 100, "2026-01-15", "edgar/data/100/a.txt")

        def _fake(year: int, q: int) -> str:
            if (year, q) == (2025, 4):
                raise RuntimeError("simulated SEC outage")
            return ok_payload

        result = sync_filer_directory(ebull_test_conn, quarters=2, today=date(2026, 5, 5), fetch=_fake)

        assert result.quarters_attempted == 2
        assert result.quarters_failed == 1
        assert result.filers_seen == 1
        assert result.filers_inserted == 1

    def test_empty_company_name_is_skipped_and_counted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Defensive guard: if the aggregator surfaces a whitespace-
        only ``company_name`` (parser usually filters these but
        SEC layout drift could one day produce one), the row is
        skipped + counted, and the rest of the sweep proceeds.
        Bypasses the parser by stubbing ``_aggregate_filer_directory``
        via fetch — the parser regex itself rejects empty names, so
        this is a unit-level guard test rather than a parser test.
        """
        from app.services import sec_13f_filer_directory as svc

        def _fake_aggregate(
            quarters: list[tuple[int, int]],
            *,
            fetch: object,
        ) -> tuple[dict[str, str], dict[str, date], int]:
            return (
                {"0000000100": "VALID FILER", "0000000200": "   "},
                {"0000000100": date(2026, 2, 14), "0000000200": date(2026, 2, 14)},
                0,
            )

        original = svc._aggregate_filer_directory
        svc._aggregate_filer_directory = _fake_aggregate  # type: ignore[assignment]
        try:
            result = sync_filer_directory(ebull_test_conn, quarters=1, today=date(2026, 5, 5), fetch=lambda *_: "")
        finally:
            svc._aggregate_filer_directory = original  # type: ignore[assignment]

        assert result.filers_seen == 2
        assert result.filers_inserted == 1
        assert result.skipped_empty_name == 1

    def test_same_date_collation_is_deterministic_lexmax(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """When two 13F-HR rows for the same CIK share ``date_filed``
        (e.g. 13F-HR + 13F-HR/A landing same day), the
        lexicographically-greatest name wins so the result is
        deterministic regardless of parser iteration order. Codex
        pre-push review #912 P2-1."""
        payload = (
            _HEADER
            + _row("13F-HR", "AAA NAME LLC", 100, "2026-02-14", "edgar/data/100/a.txt")
            + _row("13F-HR/A", "ZZZ NAME LLC", 100, "2026-02-14", "edgar/data/100/b.txt")
        )

        sync_filer_directory(ebull_test_conn, quarters=1, today=date(2026, 5, 5), fetch=lambda *_: payload)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT name FROM institutional_filers WHERE cik = '0000000100'")
            row = cur.fetchone()
        assert row is not None
        assert row["name"] == "ZZZ NAME LLC"

    def test_existing_newer_last_filing_at_preserves_existing_name(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """If ``institutional_filers`` already has a newer
        ``last_filing_at`` (e.g. populated by the holdings ingester
        from ``primary_doc.xml``), the form.idx walk must not
        regress the name to an older closed-quarter filing.
        Codex pre-push review #912 P2-2."""
        from datetime import datetime as _dt

        # Pre-populate with a NEWER timestamp than anything our
        # 4-quarter walk will produce.
        ebull_test_conn.execute(
            "INSERT INTO institutional_filers (cik, name, filer_type, last_filing_at) "
            "VALUES ('0000000500', 'NEWER CANONICAL NAME', 'INV', %s)",
            (_dt(2027, 1, 15, 0, 0, tzinfo=__import__("datetime").timezone.utc),),
        )
        ebull_test_conn.commit()

        # form.idx walk surfaces an OLDER filing for the same CIK.
        payload = _HEADER + _row("13F-HR", "OLDER FORM_IDX NAME", 500, "2026-02-14", "edgar/data/500/a.txt")
        sync_filer_directory(ebull_test_conn, quarters=1, today=date(2026, 5, 5), fetch=lambda *_: payload)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT name, last_filing_at FROM institutional_filers WHERE cik = '0000000500'")
            row = cur.fetchone()
        assert row is not None
        # Older form.idx must NOT regress the name — newer-source name preserved.
        assert row["name"] == "NEWER CANONICAL NAME"
        # GREATEST() keeps the newer timestamp.
        assert row["last_filing_at"].year == 2027

    def test_filer_type_preserved_on_update(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A pre-existing filer with an N-CEN-derived filer_type must
        keep that classification when this job refreshes the row.
        N-CEN classifier (#782) owns later refinement; this job only
        sets the floor on first INSERT."""
        # Pre-populate institutional_filers with an N-CEN-derived 'INS' row.
        ebull_test_conn.execute(
            "INSERT INTO institutional_filers (cik, name, filer_type) "
            "VALUES ('0000000400', 'PRE-EXISTING FILER', 'INS')",
        )
        ebull_test_conn.commit()

        payload = _HEADER + _row("13F-HR", "REFRESHED NAME", 400, "2026-02-14", "edgar/data/400/a.txt")
        sync_filer_directory(ebull_test_conn, quarters=1, today=date(2026, 5, 5), fetch=lambda *_: payload)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT name, filer_type FROM institutional_filers WHERE cik = '0000000400'")
            row = cur.fetchone()
        assert row is not None
        assert row["name"] == "REFRESHED NAME"
        # filer_type must be preserved — INSERT-only column on UPDATE.
        assert row["filer_type"] == "INS"
