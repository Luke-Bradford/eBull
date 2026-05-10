"""Tests for daily-index reconciliation (#868)."""

from __future__ import annotations

from datetime import date

import psycopg
import pytest

from app.jobs.sec_atom_fast_lane import ResolvedSubject
from app.jobs.sec_daily_index_reconcile import run_daily_index_reconcile
from app.services.sec_manifest import (
    get_manifest_row,
    record_manifest_entry,
    transition_status,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


_DAILY_INDEX_SAMPLE = b"""\
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    April 30, 2026

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|2026-04-30|edgar/data/320193/0000320193-26-000042.txt
9999999|Out Of Universe|4|2026-04-30|edgar/data/9999999/0009999999-26-000001.txt
320193|Apple Inc.|S-1|2026-04-30|edgar/data/320193/0000320193-26-000777.txt
"""


def _fake_get(status: int, body: bytes):
    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return status, body

    return _impl


def _seed_aapl(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (1701, 'AAPL', 'Apple', '4', 'USD', TRUE)
        """
    )
    conn.commit()


def _aapl_resolver(conn, cik):
    if cik == "0000320193":
        return ResolvedSubject(subject_type="issuer", subject_id="1701", instrument_id=1701)
    return None


class TestReconcile:
    def test_filters_in_universe_skips_unmapped_form(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        stats = run_daily_index_reconcile(
            ebull_test_conn,
            http_get=_fake_get(200, _DAILY_INDEX_SAMPLE),
            when=date(2026, 4, 30),
            subject_resolver=_aapl_resolver,
        )
        ebull_test_conn.commit()

        # 3 index rows (AAPL 8-K, out-of-universe 4, AAPL S-1)
        assert stats.index_rows == 3
        # AAPL 8-K matches; out-of-universe filtered; S-1 unmapped
        assert stats.upserted == 1
        assert stats.skipped_unknown_subject == 1
        assert stats.skipped_unmapped_form == 1

        row = get_manifest_row(ebull_test_conn, "0000320193-26-000042")
        assert row is not None
        assert row.source == "sec_8k"

    def test_does_not_downgrade_already_parsed_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # An Atom-discovered + worker-parsed row should NOT regress to
        # pending when the daily-index re-discovers it.
        _seed_aapl(ebull_test_conn)
        record_manifest_entry(
            ebull_test_conn,
            "0000320193-26-000042",
            cik="0000320193",
            form="8-K",
            source="sec_8k",
            subject_type="issuer",
            subject_id="1701",
            instrument_id=1701,
            filed_at=date(2026, 4, 30),  # type: ignore[arg-type]
        )
        transition_status(ebull_test_conn, "0000320193-26-000042", ingest_status="parsed")
        ebull_test_conn.commit()

        run_daily_index_reconcile(
            ebull_test_conn,
            http_get=_fake_get(200, _DAILY_INDEX_SAMPLE),
            when=date(2026, 4, 30),
            subject_resolver=_aapl_resolver,
        )
        ebull_test_conn.commit()

        row = get_manifest_row(ebull_test_conn, "0000320193-26-000042")
        assert row is not None
        assert row.ingest_status == "parsed"  # preserved

    def test_amendment_detection_handles_non_suffix_form(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #935 §4 regression pin. Pre-fix the daily-index path used
        # ``form.endswith("/A")``, which misses non-suffix amendment
        # forms like ``DEFA14A`` and ``DEFR14A`` (PROXY amendments
        # whose form names contain ``A`` but don't end in ``/A``).
        # The canonical helper ``is_amendment_form`` recognises both
        # families; the daily-index parser routes through it.
        #
        # Asserts: a DEFA14A row from the daily-index lands in
        # sec_filing_manifest with ``is_amendment=TRUE`` AND a
        # plain ``DEF 14A`` non-amendment lands with FALSE.
        _seed_aapl(ebull_test_conn)
        body = (
            b"Description:           Master Index of EDGAR Dissemination Feed\n"
            b"\n"
            b"CIK|Company Name|Form Type|Date Filed|Filename\n"
            b"---\n"
            b"320193|Apple Inc.|DEFA14A|2026-04-30|edgar/data/320193/0000320193-26-000050.txt\n"
            b"320193|Apple Inc.|DEF 14A|2026-04-30|edgar/data/320193/0000320193-26-000051.txt\n"
            b"320193|Apple Inc.|8-K/A|2026-04-30|edgar/data/320193/0000320193-26-000052.txt\n"
        )
        run_daily_index_reconcile(
            ebull_test_conn,
            http_get=_fake_get(200, body),
            when=date(2026, 4, 30),
            subject_resolver=_aapl_resolver,
        )
        ebull_test_conn.commit()

        defa = get_manifest_row(ebull_test_conn, "0000320193-26-000050")
        defp = get_manifest_row(ebull_test_conn, "0000320193-26-000051")
        eightka = get_manifest_row(ebull_test_conn, "0000320193-26-000052")

        assert defa is not None and defa.is_amendment is True
        assert defp is not None and defp.is_amendment is False
        assert eightka is not None and eightka.is_amendment is True
