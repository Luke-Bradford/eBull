"""Tests for the SEC N-PORT RIC trust-CIK directory sync (#963).

Sibling of ``tests/test_sec_13f_filer_directory.py`` (#912). Pins the
contract of :mod:`app.services.sec_nport_filer_directory`:

  * Walks the last N closed quarters' ``form.idx``, harvests every
    distinct NPORT-P / NPORT-P/A filer CIK, UPSERTs into
    ``sec_nport_filer_directory``.
  * Idempotent — re-run on the same quarter set produces zero new
    inserts but refreshes ``fund_trust_name`` + ``last_seen_filed_at``.
  * Per-quarter fetch failures are isolated — a transient SEC outage
    on one quarter doesn't abort the whole sweep.
  * Empty-name rows are skipped + counted (loudly, via warning).
  * Form-type filter accepts NPORT-P / NPORT-P/A / N-PORT / N-PORT/A
    (modern + legacy spellings) and rejects everything else.
  * Same-day name tiebreak is deterministic (lex-greatest wins).

Plus a regression test pinning that ``sec_n_port_ingest`` job reads
from the new directory rather than the old ``institutional_filers``
universe.
"""

from __future__ import annotations

from datetime import date

import psycopg
import psycopg.rows
import pytest

from app.services.sec_nport_filer_directory import (
    _aggregate_nport_filer_directory,
    _last_completed_quarter,
    _last_n_quarters,
    sync_nport_filer_directory,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_HEADER = "Description: x\n\nForm Type   Company Name   CIK   Date Filed   File Name\n" + "-" * 100 + "\n"


def _row(form_type: str, company: str, cik: int, filed: str, file: str) -> str:
    # Same width-tolerant row format that ``parse_form_index`` accepts —
    # mirrors ``tests/test_sec_13f_filer_directory.py`` so the parser
    # contract is exercised the same way.
    return f"{form_type}  {company}  {cik}  {filed}  {file}\n"


# ---------------------------------------------------------------------------
# Pure-helper unit tests
# ---------------------------------------------------------------------------


class TestLastCompletedQuarter:
    """Same shape as the #912 walker — duplicated test to pin the
    contract independently in case the two helpers ever diverge."""

    def test_january_picks_prior_year_q4(self) -> None:
        assert _last_completed_quarter(date(2026, 1, 5)) == (2025, 4)

    def test_april_first_picks_q1(self) -> None:
        assert _last_completed_quarter(date(2026, 4, 1)) == (2026, 1)

    def test_july_picks_q2(self) -> None:
        assert _last_completed_quarter(date(2026, 7, 15)) == (2026, 2)


class TestLastNQuarters:
    def test_returns_n_quarters_newest_first(self) -> None:
        assert _last_n_quarters(date(2026, 5, 5), 4) == [
            (2026, 1),
            (2025, 4),
            (2025, 3),
            (2025, 2),
        ]

    def test_walks_across_year_boundary(self) -> None:
        assert _last_n_quarters(date(2026, 1, 15), 5) == [
            (2025, 4),
            (2025, 3),
            (2025, 2),
            (2025, 1),
            (2024, 4),
        ]


class TestAggregateNportFilerDirectory:
    """Exercises the form-type filter + per-quarter failure isolation
    + same-day name tiebreak via a fake fetcher (no real SEC)."""

    def test_only_nport_form_types_captured(self) -> None:
        idx = _HEADER + "".join(
            [
                _row("NPORT-P", "VANGUARD INDEX FUNDS", 36405, "2026-01-15", "edgar/data/36405/x.txt"),
                _row("NPORT-P/A", "ISHARES TRUST", 1100663, "2026-02-20", "edgar/data/1100663/y.txt"),
                _row("13F-HR", "VANGUARD GROUP INC", 102909, "2026-01-15", "edgar/data/102909/z.txt"),
                _row("10-K", "APPLE INC", 320193, "2026-03-01", "edgar/data/320193/k.txt"),
                _row("N-PORT", "LEGACY TRUST", 999999, "2017-06-01", "edgar/data/999999/legacy.txt"),
            ]
        )

        def _fetch(_y: int, _q: int) -> str:
            return idx

        names, filed, failed = _aggregate_nport_filer_directory(
            [(2026, 1)],
            fetch=_fetch,
        )
        # 13F-HR and 10-K rows excluded; NPORT-P + NPORT-P/A + legacy
        # N-PORT all kept. parse_form_index zero-pads CIKs to 10
        # digits — assert against that canonical shape.
        assert set(names.keys()) == {"0000036405", "0001100663", "0000999999"}
        assert names["0000036405"] == "VANGUARD INDEX FUNDS"
        assert filed["0001100663"] == date(2026, 2, 20)
        assert failed == 0

    def test_per_quarter_failure_isolated(self) -> None:
        good = _HEADER + _row("NPORT-P", "VANGUARD INDEX FUNDS", 36405, "2026-01-15", "edgar/data/36405/x.txt")

        def _fetch(year: int, _q: int) -> str:
            if year == 2025:
                raise RuntimeError("simulated SEC outage")
            return good

        names, _, failed = _aggregate_nport_filer_directory(
            [(2026, 1), (2025, 4)],
            fetch=_fetch,
        )
        # 2025 quarter raised, but 2026 quarter still aggregated.
        assert names == {"0000036405": "VANGUARD INDEX FUNDS"}
        assert failed == 1

    def test_same_day_tiebreak_picks_lex_greatest_name(self) -> None:
        # Two NPORT-P rows for the same CIK on the same date_filed.
        # The deterministic tiebreak picks the lex-greatest name.
        idx = _HEADER + "".join(
            [
                _row("NPORT-P", "AAA TRUST", 36405, "2026-01-15", "edgar/data/36405/a.txt"),
                _row("NPORT-P", "ZZZ TRUST", 36405, "2026-01-15", "edgar/data/36405/z.txt"),
            ]
        )

        def _fetch(_y: int, _q: int) -> str:
            return idx

        names, _, _ = _aggregate_nport_filer_directory([(2026, 1)], fetch=_fetch)
        assert names["0000036405"] == "ZZZ TRUST"

    def test_newest_filing_date_wins_for_name(self) -> None:
        # Across two quarters: the newer date_filed wins for the name +
        # filed_at, regardless of iteration order.
        q1 = _HEADER + _row("NPORT-P", "OLD NAME", 36405, "2025-04-15", "edgar/data/36405/old.txt")
        q2 = _HEADER + _row("NPORT-P", "NEW NAME", 36405, "2026-01-20", "edgar/data/36405/new.txt")

        def _fetch(year: int, q: int) -> str:
            return q2 if (year, q) == (2026, 1) else q1

        names, filed, _ = _aggregate_nport_filer_directory(
            [(2026, 1), (2025, 2)],
            fetch=_fetch,
        )
        assert names["0000036405"] == "NEW NAME"
        assert filed["0000036405"] == date(2026, 1, 20)


# ---------------------------------------------------------------------------
# Integration: sync_nport_filer_directory against ebull_test
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.integration


class TestSyncNportFilerDirectory:
    def test_inserts_new_filers_into_empty_directory(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        idx = _HEADER + "".join(
            [
                _row("NPORT-P", "VANGUARD INDEX FUNDS", 36405, "2026-01-15", "edgar/data/36405/x.txt"),
                _row("NPORT-P", "ISHARES TRUST", 1100663, "2026-02-20", "edgar/data/1100663/y.txt"),
                _row("NPORT-P/A", "INVESCO QQQ TRUST SERIES 1", 884394, "2026-03-10", "edgar/data/884394/z.txt"),
            ]
        )

        result = sync_nport_filer_directory(
            conn,
            quarters=1,
            today=date(2026, 5, 5),
            fetch=lambda _y, _q: idx,
        )
        assert result.filers_inserted == 3
        assert result.filers_refreshed == 0
        assert result.filers_seen == 3
        assert result.skipped_empty_name == 0
        assert result.quarters_failed == 0

        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(
                """
                SELECT cik, fund_trust_name, last_seen_filed_at
                FROM sec_nport_filer_directory
                WHERE cik IN ('0000036405', '0001100663', '0000884394')
                ORDER BY cik
                """
            )
            rows = cur.fetchall()
        assert len(rows) == 3
        names = {r[0]: r[1] for r in rows}
        assert names["0000036405"] == "VANGUARD INDEX FUNDS"
        assert names["0001100663"] == "ISHARES TRUST"
        assert names["0000884394"] == "INVESCO QQQ TRUST SERIES 1"

    def test_idempotent_rerun_produces_zero_new_inserts(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        idx = _HEADER + _row("NPORT-P", "VANGUARD INDEX FUNDS", 36405, "2026-01-15", "edgar/data/36405/x.txt")

        first = sync_nport_filer_directory(conn, quarters=1, today=date(2026, 5, 5), fetch=lambda _y, _q: idx)
        assert first.filers_inserted == 1
        assert first.filers_refreshed == 0

        second = sync_nport_filer_directory(conn, quarters=1, today=date(2026, 5, 5), fetch=lambda _y, _q: idx)
        assert second.filers_inserted == 0
        assert second.filers_refreshed == 1

    def test_name_refresh_when_newer_filing_arrives(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        # Initial load: old name + old filed date.
        old_idx = _HEADER + _row("NPORT-P", "OLD TRUST NAME", 36405, "2025-04-15", "edgar/data/36405/old.txt")
        sync_nport_filer_directory(conn, quarters=1, today=date(2025, 8, 1), fetch=lambda _y, _q: old_idx)

        # Second load: same CIK, NEWER filing date + new name. Refresh.
        new_idx = _HEADER + _row("NPORT-P", "NEW TRUST NAME", 36405, "2026-01-20", "edgar/data/36405/new.txt")
        sync_nport_filer_directory(conn, quarters=1, today=date(2026, 5, 5), fetch=lambda _y, _q: new_idx)

        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute("SELECT fund_trust_name FROM sec_nport_filer_directory WHERE cik = '0000036405'")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "NEW TRUST NAME"

    def test_name_does_not_regress_when_older_filing_arrives_later(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """If a later directory walk happens to encounter an OLDER
        form.idx row than what's already stored (e.g. operator runs
        a backfill walk against historical quarters), the canonical
        name must NOT regress to the older value. Mirrors the
        regression guard #912 added after Codex pre-push review."""
        conn = ebull_test_conn
        # Newest filing first.
        new_idx = _HEADER + _row("NPORT-P", "NEW TRUST NAME", 36405, "2026-01-20", "edgar/data/36405/new.txt")
        sync_nport_filer_directory(conn, quarters=1, today=date(2026, 5, 5), fetch=lambda _y, _q: new_idx)

        # Older filing arrives second. Name must not regress.
        old_idx = _HEADER + _row("NPORT-P", "OLD TRUST NAME", 36405, "2025-04-15", "edgar/data/36405/old.txt")
        sync_nport_filer_directory(conn, quarters=1, today=date(2025, 8, 1), fetch=lambda _y, _q: old_idx)

        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute("SELECT fund_trust_name FROM sec_nport_filer_directory WHERE cik = '0000036405'")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "NEW TRUST NAME"

    def test_n_port_ingest_job_selector_reads_from_new_directory(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """The reason #963 exists: ``sec_n_port_ingest`` job
        (``app/workers/scheduler.py``) used to walk
        ``institutional_filers WHERE filer_type IN (...)`` which is
        the 13F-MANAGER CIK universe — wrong entity for N-PORT (which
        files under RIC TRUST CIKs). Pin that the job's selector now
        reads from ``sec_nport_filer_directory`` so the cron path
        produces non-zero rows in production without the
        ``.claude/nport-panel-backfill.py`` workaround #919 needed.

        We exercise the selector SQL by inspecting the source — the
        full job invocation hits SEC EDGAR which we don't want in a
        unit test. The constants assert is enough to catch a future
        regression that swaps the selector back."""
        # Get the source of sec_n_port_ingest so we can assert the
        # selector references the new table, not the old one.
        import inspect

        from app.workers import scheduler as scheduler_mod

        source = inspect.getsource(scheduler_mod.sec_n_port_ingest)
        assert "FROM sec_nport_filer_directory" in source, (
            "sec_n_port_ingest must read from sec_nport_filer_directory (#963), "
            "not the legacy institutional_filers universe."
        )
        assert "FROM institutional_filers" not in source, (
            "sec_n_port_ingest must NOT read from institutional_filers any more — "
            "that's the 13F-MANAGER directory, wrong entity for N-PORT."
        )
